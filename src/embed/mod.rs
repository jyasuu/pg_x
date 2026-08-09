//! Embedding API client and embeddable-text interpolation for the `consume`
//! pipeline.
//!
//! The wire format handling lives here as pure functions (request builders and
//! response parsers), so both Ollama and OpenAI-compatible formats are
//! unit-tested without a live server. The HTTP transport ([`EmbedClient`]) is
//! gated behind the `embed` feature.

use anyhow::{anyhow, Result};
#[cfg(feature = "embed")]
use anyhow::{bail, Context};
use async_trait::async_trait;
use serde_json::{json, Value};

/// The embedding API wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbedApi {
    /// Ollama `POST {base}/api/embed`, response `embeddings[0]`.
    Ollama,
    /// OpenAI-compatible `POST {base}/v1/embeddings`, response `data[0].embedding`.
    Openai,
}

impl std::str::FromStr for EmbedApi {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ollama" => Ok(Self::Ollama),
            "openai" => Ok(Self::Openai),
            other => Err(format!(
                "unknown embedding API '{other}'; expected ollama|openai"
            )),
        }
    }
}

impl std::fmt::Display for EmbedApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ollama => write!(f, "ollama"),
            Self::Openai => write!(f, "openai"),
        }
    }
}

/// An embedding provider. A sink-stage failure surfaces as `Err` and obeys the
/// consume session's existing error policy.
#[async_trait]
pub trait Embed: Send + Sync {
    /// Embed `text`, returning the float vector.
    async fn embed(&self, text: &str) -> Result<Vec<f32>>;
}

// ── Pure wire-format builders and parsers ────────────────────────────────────

/// Ollama `/api/embed` request body.
pub fn ollama_request(model: &str, text: &str) -> Value {
    json!({ "model": model, "input": text })
}

/// OpenAI-compatible `/v1/embeddings` request body.
pub fn openai_request(model: &str, text: &str) -> Value {
    json!({ "model": model, "input": text })
}

/// Parse an Ollama `/api/embed` response body into the first embedding.
pub fn parse_ollama_response(body: &Value) -> Result<Vec<f32>> {
    let embeddings = body
        .get("embeddings")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("ollama response missing 'embeddings' array"))?;
    let first = embeddings
        .first()
        .ok_or_else(|| anyhow!("ollama response 'embeddings' is empty"))?;
    parse_float_array(first)
}

/// Parse an OpenAI-compatible `/v1/embeddings` response body into the first
/// embedding.
pub fn parse_openai_response(body: &Value) -> Result<Vec<f32>> {
    let data = body
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("openai response missing 'data' array"))?;
    let first = data
        .first()
        .ok_or_else(|| anyhow!("openai response 'data' is empty"))?;
    let embedding = first
        .get("embedding")
        .ok_or_else(|| anyhow!("openai response entry missing 'embedding'"))?;
    parse_float_array(embedding)
}

fn parse_float_array(value: &Value) -> Result<Vec<f32>> {
    let arr = value
        .as_array()
        .ok_or_else(|| anyhow!("expected a float array"))?;
    arr.iter()
        .map(|x| {
            x.as_f64()
                .map(|f| f as f32)
                .ok_or_else(|| anyhow!("expected a numeric embedding value"))
        })
        .collect()
}

/// The HTTP transport shared by the embedding clients.
#[cfg(feature = "embed")]
pub struct EmbedClient {
    url: String,
    api: EmbedApi,
    model: String,
    client: reqwest::Client,
}

#[cfg(feature = "embed")]
impl EmbedClient {
    /// Build a client for `api` at `url`, embedding with `model`.
    pub fn new(url: &str, api: EmbedApi, model: &str) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()?;
        Ok(Self {
            url: url.trim_end_matches('/').to_string(),
            api,
            model: model.to_string(),
            client,
        })
    }
}

#[cfg(feature = "embed")]
#[async_trait]
impl Embed for EmbedClient {
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let (path, body, parse): (&str, Value, fn(&Value) -> Result<Vec<f32>>) = match self.api {
            EmbedApi::Ollama => (
                "/api/embed",
                ollama_request(&self.model, text),
                parse_ollama_response,
            ),
            EmbedApi::Openai => (
                "/v1/embeddings",
                openai_request(&self.model, text),
                parse_openai_response,
            ),
        };
        let url = format!("{}{}", self.url, path);
        let response = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("embedding request failed to {url}"))?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            bail!("embedding API {url} returned HTTP {status}: {text}");
        }
        let body: Value = response.json().await?;
        parse(&body)
    }
}

// ── Embeddable-text template interpolation ───────────────────────────────────

/// Interpolate `{field}` and dotted `{a.b}` placeholders from `doc` into
/// `template`. Missing paths render empty; non-placeholder text is preserved.
pub fn interpolate(template: &str, doc: &Value) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    loop {
        match rest.find('{') {
            None => {
                out.push_str(rest);
                break;
            }
            Some(start) => {
                out.push_str(&rest[..start]);
                let after = &rest[start + 1..];
                match after.find('}') {
                    None => {
                        out.push_str(&rest[start..]);
                        break;
                    }
                    Some(end) => {
                        let path = &after[..end];
                        out.push_str(&lookup(path, doc).unwrap_or_default());
                        rest = &after[end + 1..];
                    }
                }
            }
        }
    }
    out
}

/// The default template for embedding `field`: `{field}`.
pub fn default_template(field: &str) -> String {
    format!("{{{field}}}")
}

/// Resolve a (possibly dotted) path from `doc`. Strings render verbatim,
/// other scalars via their JSON representation; missing paths are `None`.
fn lookup(path: &str, doc: &Value) -> Option<String> {
    let mut current = doc;
    for part in path.split('.') {
        current = current.get(part)?;
    }
    match current {
        Value::String(s) => Some(s.clone()),
        Value::Null => None,
        _ => Some(current.to_string()),
    }
}

#[cfg(test)]
mod wire_format_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ollama_request_shape() {
        assert_eq!(
            ollama_request("bge-m3", "hello"),
            json!({ "model": "bge-m3", "input": "hello" })
        );
    }

    #[test]
    fn openai_request_shape() {
        assert_eq!(
            openai_request("text-embedding-3-small", "hello"),
            json!({ "model": "text-embedding-3-small", "input": "hello" })
        );
    }

    #[test]
    fn parse_ollama_happy_path() {
        let body = json!({ "embeddings": [[0.1, 0.2, 0.3]] });
        assert_eq!(parse_ollama_response(&body).unwrap(), vec![0.1, 0.2, 0.3]);
    }

    #[test]
    fn parse_openai_happy_path() {
        let body = json!({ "data": [{ "embedding": [0.1, 0.2] }] });
        assert_eq!(parse_openai_response(&body).unwrap(), vec![0.1, 0.2]);
    }

    #[test]
    fn parse_ollama_missing_embeddings() {
        assert!(parse_ollama_response(&json!({})).is_err());
    }

    #[test]
    fn parse_ollama_empty_embeddings() {
        assert!(parse_ollama_response(&json!({ "embeddings": [] })).is_err());
    }

    #[test]
    fn parse_openai_missing_embedding_field() {
        assert!(parse_openai_response(&json!({ "data": [{ "index": 0 }] })).is_err());
    }

    #[test]
    fn parse_rejects_non_numeric_values() {
        assert!(parse_float_array(&json!(["x", 1])).is_err());
    }

    #[test]
    fn embed_api_from_str() {
        assert_eq!("ollama".parse::<EmbedApi>().unwrap(), EmbedApi::Ollama);
        assert_eq!("openai".parse::<EmbedApi>().unwrap(), EmbedApi::Openai);
        assert!("azure".parse::<EmbedApi>().is_err());
    }
}

#[cfg(test)]
mod template_tests {
    use super::*;
    use serde_json::json;

    fn doc() -> Value {
        json!({
            "content": "premium cotton",
            "source": "catalog",
            "meta": { "status": "active", "rank": 2 }
        })
    }

    #[test]
    fn plain_field() {
        assert_eq!(interpolate("{content}", &doc()), "premium cotton");
    }

    #[test]
    fn dotted_path() {
        assert_eq!(interpolate("{meta.status}", &doc()), "active");
        assert_eq!(interpolate("{meta.rank}", &doc()), "2");
    }

    #[test]
    fn missing_path_renders_empty() {
        assert_eq!(interpolate("{missing}", &doc()), "");
        assert_eq!(interpolate("{meta.missing}", &doc()), "");
    }

    #[test]
    fn mixed_text_preserved() {
        assert_eq!(
            interpolate("{content}\n-- {source} --", &doc()),
            "premium cotton\n-- catalog --"
        );
    }

    #[test]
    fn no_placeholders_passthrough() {
        assert_eq!(interpolate("literal text", &doc()), "literal text");
    }

    #[test]
    fn unterminated_placeholder_left_as_is() {
        assert_eq!(interpolate("a {content", &doc()), "a {content");
    }

    #[test]
    fn default_template_is_field() {
        assert_eq!(default_template("content"), "{content}");
    }
}
