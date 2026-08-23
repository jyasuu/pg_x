#[cfg(feature = "embed")]
use anyhow::bail;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use clap::{Args, ValueEnum};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::commands::consume_session::{Compose, ConsumeSession};
use crate::consumer::r#trait::{ConsumeSink, Consumer};
use crate::embed::{default_template, EmbedApi};
#[cfg(feature = "embed")]
use crate::embed::{interpolate, Embed, EmbedClient};
use crate::graphql::query::{NamedQuery, QueryLoader};
use crate::graphql::{executor, pool::QueryConn, schema::SchemaRegistry};
use crate::utils::config::{
    Connection, ConsumeConfig, ConsumeSinkKind, ConsumeSourceKind, EmbedConfig, MutationConfig,
    ResolverConfig,
};
use crate::utils::session_loop::{self, ReconnectConfig, SessionExit};
use crate::utils::signal::shutdown_signal;

// ── CLI args ─────────────────────────────────────────────────────────────────

#[derive(Args)]
pub struct ConsumeArgs {
    /// Source type: rabbitmq, kafka, or nats
    #[arg(long, value_enum, default_value_t = ConsumeSourceType::Rabbitmq)]
    pub source: ConsumeSourceType,

    /// Sink type(s): stdout, elasticsearch, webhook, kv, postgres-vector, or
    /// graphql-mutate.
    /// Repeatable; with more than one, the composed document fans out to all.
    #[arg(long, value_enum)]
    pub sink: Vec<ConsumeSinkType>,

    // ── Source: RabbitMQ ──
    #[arg(long, env = "AMQP_URL")]
    pub amqp_url: Option<String>,
    #[arg(long)]
    pub queue: Option<String>,
    #[arg(long)]
    pub exchange: Option<String>,
    #[arg(long)]
    pub routing_key: Option<String>,
    /// Max unacknowledged messages per consumer (0 = no limit). Limits how many
    /// messages RabbitMQ delivers before awaiting an ack, preventing an
    /// unbounded unacked backlog when processing is slower than publishing.
    #[arg(long)]
    pub prefetch_count: Option<u16>,

    // ── Source: Kafka ──
    #[arg(long, env = "KAFKA_BROKERS")]
    pub brokers: Option<String>,
    #[arg(long)]
    pub topic: Option<String>,
    #[arg(long)]
    pub group_id: Option<String>,

    // ── Source: NATS (JetStream) ──
    /// NATS server URL.
    #[arg(long, env = "NATS_URL")]
    pub nats_url: Option<String>,
    /// JetStream stream name.
    #[arg(long)]
    pub nats_stream: Option<String>,
    /// Subject filter within the stream, if the stream covers more than one
    /// subject (wildcards allowed).
    #[arg(long)]
    pub nats_subject: Option<String>,
    /// Durable consumer name; survives restarts and reuses the consumer's
    /// cursor instead of starting from the stream default.
    #[arg(long)]
    pub nats_consumer: Option<String>,
    /// Create the stream (and durable consumer) on startup if missing —
    /// dev/test convenience; off by default so prod does not silently create
    /// infrastructure.
    #[arg(long, default_value_t = false)]
    pub nats_create_stream: bool,

    // ── Query ──
    /// Query mode: contract (name from message event_type) or simple (fixed --query)
    #[arg(long, value_enum)]
    pub query_mode: Option<ConsumeQueryMode>,
    /// Query name (required in simple mode)
    #[arg(long)]
    pub query: Option<String>,
    /// Max resolver recursion depth
    #[arg(long)]
    pub max_depth: Option<u32>,
    /// Schema directory (defaults to ~/.pgx/schema)
    #[arg(long)]
    pub schema_dir: Option<String>,

    // ── Error handling ──
    /// Error mode: lenient (log + continue) or strict (nack + abort)
    #[arg(long, value_enum)]
    pub on_error: Option<ConsumeErrorMode>,

    // ── Idempotence ──
    /// Make redelivered messages harmless: dedupe recently seen message ids and
    /// derive stable sink keys from the message id.
    #[arg(long, default_value_t = false)]
    pub idempotent: bool,
    /// How long (seconds) to remember seen message ids for dedupe (default 900).
    #[arg(long)]
    pub dedup_ttl: Option<u64>,

    // ── Reconnection ──
    /// Max reconnect attempts (0 = infinite)
    #[arg(long, default_value_t = 0)]
    pub max_reconnect_attempts: u32,
    /// Reconnect backoff base in milliseconds
    #[arg(long, default_value_t = 1000)]
    pub reconnect_base_ms: u64,
    /// Reconnect backoff max in milliseconds
    #[arg(long, default_value_t = 30000)]
    pub reconnect_max_ms: u64,

    // ── Sink: Elasticsearch ──
    #[arg(long, env = "ES_URL")]
    pub es_url: Option<String>,
    #[arg(long)]
    pub index: Option<String>,
    #[arg(long)]
    pub id_field: Option<String>,

    // ── Sink: Webhook ──
    #[arg(long, env = "WEBHOOK_URL")]
    pub webhook_url: Option<String>,

    // ── Sink: KV (Redis / Memcached) ──
    /// KV store URL (redis://... or memcached://...)
    #[arg(long, env = "KV_URL")]
    pub kv_url: Option<String>,
    /// Field in the document to use as the cache key
    #[arg(long)]
    pub key_field: Option<String>,
    /// Prefix to prepend to the cache key
    #[arg(long)]
    pub key_prefix: Option<String>,
    /// TTL in seconds (0 = no expiry)
    #[arg(long)]
    pub ttl: Option<u64>,

    // ── Embedding stage ──
    /// Embedding API base URL (e.g. http://localhost:11434). Enables the
    /// embedding stage: each composed document is embedded and the vector is
    /// attached before delivery.
    #[arg(long, env = "EMBED_URL")]
    pub embed_url: Option<String>,
    /// Embedding wire format: ollama or openai (default ollama)
    #[arg(long)]
    pub embed_api: Option<String>,
    /// Embedding model name (required when the stage is enabled)
    #[arg(long)]
    pub embed_model: Option<String>,
    /// Field embedded when no --embed-template is set (default "content")
    #[arg(long)]
    pub embed_field: Option<String>,
    /// Template interpolating {{field}} / {{a.b}} from the composed document
    #[arg(long)]
    pub embed_template: Option<String>,
    /// Document field receiving the vector (default "embedding")
    #[arg(long)]
    pub embed_output_field: Option<String>,
    /// Expected vector dimension; mismatches fail the sink stage
    #[arg(long)]
    pub embed_dim: Option<usize>,
    /// Table name for the postgres-vector sink (default "chunk_embeddings")
    #[arg(long)]
    pub vector_table: Option<String>,

    // ── Sink: GraphQL mutate ──
    /// Named mutation from [mutations.*] in ~/.pgx/config.toml; required by the
    /// graphql-mutate sink.
    #[arg(long)]
    pub mutation: Option<String>,
    /// Target Postgres URL for the graphql-mutate sink; used when the mutation
    /// has no target_url of its own.
    #[arg(long, env = "MUTATE_TARGET_URL")]
    pub mutate_target_url: Option<String>,
}

#[derive(Clone, ValueEnum)]
pub enum ConsumeSourceType {
    Rabbitmq,
    Kafka,
    Nats,
}

#[derive(Clone, ValueEnum)]
pub enum ConsumeSinkType {
    Stdout,
    Elasticsearch,
    Webhook,
    /// Key-value store (Redis / Memcached). Requires the 'kv' feature.
    Kv,
    /// Upsert the document's embedding into a Postgres pgvector table.
    PostgresVector,
    /// Execute a configured SQL mutation against a second Postgres database
    /// (DB B), binding composed-document fields to positional parameters.
    GraphqlMutate,
}

#[derive(Debug, Clone, PartialEq, ValueEnum)]
pub enum ConsumeQueryMode {
    Simple,
    Contract,
}

#[derive(Debug, Clone, PartialEq, ValueEnum)]
pub enum ConsumeErrorMode {
    Lenient,
    Strict,
}

/// The stage of the per-message pipeline where a failure occurred.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ErrorStage {
    /// The payload did not parse as a contract message (contract mode only).
    Parse,
    /// No named query matched the message's event type.
    Lookup,
    /// GraphQL execution failed while composing the document.
    Compose,
    /// Delivery to the sink failed.
    Sink,
}

/// The control-flow decision produced by the error policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ErrorAction {
    /// Nack without requeue and continue; the message is dropped.
    Discard,
    /// Nack with requeue and continue; the broker redelivers the message.
    Requeue,
    /// Nack with requeue and stop the session with the caller's error.
    Abort,
}

impl ConsumeErrorMode {
    /// One error policy for the consume session. Stage × mode → decision.
    ///
    /// | stage   | lenient | strict |
    /// |---------|---------|--------|
    /// | Parse   | Discard | Abort  |
    /// | Lookup  | Discard | Abort  |
    /// | Compose | Discard | Abort  |
    /// | Sink    | Requeue | Abort  |
    ///
    /// Per-message failures (parse, lookup, compose) are dropped in lenient
    /// mode: the message itself is unusable, so redelivering it would fail
    /// identically. A sink failure is transient, so lenient mode requeues
    /// rather than silently discarding a durable event. Strict mode aborts
    /// the session on any failure so nothing is dropped unseen.
    pub(crate) fn handle(&self, stage: ErrorStage) -> ErrorAction {
        match (self, stage) {
            (ConsumeErrorMode::Lenient, ErrorStage::Sink) => ErrorAction::Requeue,
            (ConsumeErrorMode::Lenient, _) => ErrorAction::Discard,
            (ConsumeErrorMode::Strict, _) => ErrorAction::Abort,
        }
    }
}

// ── Sink implementations ──────────────────────────────────────────────────────

struct StdoutConsumeSink;

#[async_trait]
impl ConsumeSink for StdoutConsumeSink {
    fn name(&self) -> &str {
        "stdout"
    }

    async fn send(&self, doc: &Value, _msg_id: Option<&str>) -> Result<()> {
        println!("{}", serde_json::to_string_pretty(doc)?);
        Ok(())
    }
}

#[cfg(feature = "elasticsearch")]
struct ElasticsearchConsumeSink {
    index: String,
    id_field: Option<String>,
    es: crate::downstream::delivery::elasticsearch::Elasticsearch,
}

#[cfg(feature = "elasticsearch")]
#[async_trait]
impl ConsumeSink for ElasticsearchConsumeSink {
    fn name(&self) -> &str {
        "elasticsearch"
    }

    async fn send(&self, doc: &Value, msg_id: Option<&str>) -> Result<()> {
        let doc_id = crate::downstream::delivery::elasticsearch::Elasticsearch::doc_id(
            self.id_field.as_deref(),
            doc,
            msg_id,
        );

        self.es.push(&self.index, doc_id.as_deref(), doc).await
    }
}

#[cfg(feature = "webhook")]
struct WebhookConsumeSink {
    url: String,
    webhook: crate::downstream::delivery::webhook::Webhook,
}

#[cfg(feature = "webhook")]
#[async_trait]
impl ConsumeSink for WebhookConsumeSink {
    fn name(&self) -> &str {
        "webhook"
    }

    async fn send(&self, doc: &Value, msg_id: Option<&str>) -> Result<()> {
        // No retry here: the consume session nacks and redelivers via the
        // broker when delivery fails. Idempotency-Key lets the receiver dedupe.
        self.webhook
            .post(&self.url, &HashMap::new(), doc, msg_id)
            .await
    }
}

/// Decorator that embeds each composed document and attaches the vector under
/// `output_field` before forwarding to the wrapped sink. An embed failure is a
/// sink-stage failure and obeys the consume session's existing error policy.
#[cfg(feature = "embed")]
struct EmbeddingSink {
    inner: Arc<dyn ConsumeSink>,
    embedder: Arc<dyn Embed>,
    template: String,
    output_field: String,
    dim: Option<usize>,
}

#[cfg(feature = "embed")]
#[async_trait]
impl ConsumeSink for EmbeddingSink {
    fn name(&self) -> &str {
        "embedding"
    }

    async fn send(&self, doc: &Value, msg_id: Option<&str>) -> Result<()> {
        let text = interpolate(&self.template, doc)
            .with_context(|| format!("embedding template '{}' is invalid", self.template))?;
        if text.trim().is_empty() {
            bail!(
                "embedding template '{0}' rendered empty text ({1} chars) — check the field it references",
                self.template,
                text.len()
            );
        }
        let vec = self.embedder.embed(&text).await?;
        if let Some(dim) = self.dim {
            if vec.len() != dim {
                bail!(
                    "embedding dimension mismatch: got {}, expected {}",
                    vec.len(),
                    dim
                );
            }
        }
        let mut enriched = doc.clone();
        enriched[self.output_field.as_str()] = Value::from(vec);
        self.inner.send(&enriched, msg_id).await
    }
}

/// Fan-out delivery: forwards each document to every sink in order. The first
/// failure short-circuits the rest.
struct FanoutConsumeSink {
    sinks: Vec<Arc<dyn ConsumeSink>>,
}

#[async_trait]
impl ConsumeSink for FanoutConsumeSink {
    fn name(&self) -> &str {
        "fanout"
    }

    async fn send(&self, doc: &Value, msg_id: Option<&str>) -> Result<()> {
        for sink in &self.sinks {
            sink.send(doc, msg_id).await?;
        }
        Ok(())
    }
}

/// Upserts the document's embedding into a Postgres pgvector table
/// (`id` + `embedding`), using the consume session's query pool.
struct PostgresVectorConsumeSink {
    pool: Arc<QueryConn>,
    table: String,
    id_field: Option<String>,
    embedding_field: String,
}

impl PostgresVectorConsumeSink {
    /// Derive the table `id` for a document: the explicit `--id-field` string
    /// wins, then the message id (idempotent mode), then `None`.
    fn doc_id(id_field: Option<&str>, doc: &Value, msg_id: Option<&str>) -> Option<String> {
        let explicit = id_field.and_then(|idf| match doc {
            Value::Object(m) => m.get(idf).and_then(|v| v.as_str().map(|s| s.to_string())),
            _ => None,
        });
        explicit.or_else(|| msg_id.map(|s| s.to_string()))
    }

    /// Render a float vector as a pgvector literal: `[0.1,0.2,…]`. The caller
    /// binds it as a text parameter cast to `vector` at the SQL level
    /// (`$2::text::vector`), so the value must be the bare array without
    /// quotes or a cast suffix.
    fn vector_literal(v: &[f32]) -> String {
        let nums: Vec<String> = v.iter().map(|f| f.to_string()).collect();
        format!("[{}]", nums.join(","))
    }

    /// Quote an identifier for safe interpolation into SQL, mirroring
    /// Postgres' `quote_ident`: identifiers that are safe unquoted are
    /// returned as-is; everything else is double-quoted with embedded quotes
    /// doubled.
    fn quote_ident(name: &str) -> String {
        let safe = !name.is_empty()
            && !name.as_bytes()[0].is_ascii_digit()
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
        if safe {
            name.to_string()
        } else {
            format!("\"{}\"", name.replace('"', "\"\""))
        }
    }

    /// The upsert statement. The vector is bound as a `text` parameter and
    /// cast via `$2::text::vector`: binding it as a `vector` parameter
    /// directly fails client-side, because tokio-postgres rejects a `String`
    /// value for a non-text parameter type.
    fn upsert_sql(table: &str) -> String {
        format!(
            "INSERT INTO {} (id, embedding) VALUES ($1, $2::text::vector) \
             ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding",
            Self::quote_ident(table)
        )
    }
}

#[async_trait]
impl ConsumeSink for PostgresVectorConsumeSink {
    fn name(&self) -> &str {
        "postgres-vector"
    }

    async fn send(&self, doc: &Value, msg_id: Option<&str>) -> Result<()> {
        let id = Self::doc_id(self.id_field.as_deref(), doc, msg_id).ok_or_else(|| {
            anyhow!(
                "postgres-vector sink needs a stable id — provide --id-field or run with --idempotent"
            )
        })?;
        let embedding = doc
            .get(self.embedding_field.as_str())
            .and_then(Value::as_array)
            .ok_or_else(|| {
                anyhow!(
                    "document has no '{}' array; is the embedding stage enabled?",
                    self.embedding_field
                )
            })?;
        let floats: Vec<f32> = embedding
            .iter()
            .map(|v| {
                v.as_f64()
                    .map(|f| f as f32)
                    .ok_or_else(|| anyhow!("embedding contains a non-numeric value"))
            })
            .collect::<Result<_>>()?;
        let literal = Self::vector_literal(&floats);
        let sql = Self::upsert_sql(&self.table);
        let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&id, &literal];
        self.pool
            .execute_cached(&sql, &params)
            .await
            .with_context(|| format!("pgvector upsert failed on table {}", self.table))?;
        Ok(())
    }
}

/// A composed-document field rendered as a positional SQL parameter. Scalars
/// bind as their natural Postgres type; arrays and objects bind as their JSON
/// serialization so user SQL can cast (`$3::jsonb`).
#[derive(Debug)]
enum SqlParam {
    Null,
    Bool(bool),
    Int(i64),
    Real(f64),
    Text(String),
}

impl SqlParam {
    fn from_value(v: &Value) -> Self {
        match v {
            Value::Null => Self::Null,
            Value::Bool(b) => Self::Bool(*b),
            Value::Number(n) => match n.as_i64() {
                Some(i) => Self::Int(i),
                None => Self::Real(n.as_f64().unwrap_or_default()),
            },
            Value::String(s) => Self::Text(s.clone()),
            other => Self::Text(other.to_string()),
        }
    }

    fn as_dyn(&self) -> &(dyn tokio_postgres::types::ToSql + Sync) {
        match self {
            Self::Null => &Option::<String>::None,
            Self::Bool(b) => b,
            Self::Int(i) => i,
            Self::Real(f) => f,
            Self::Text(s) => s,
        }
    }

    /// Postgres type used when the server cannot infer a parameter's type on
    /// its own (see `prepared`).
    fn pg_type(&self) -> tokio_postgres::types::Type {
        use tokio_postgres::types::Type;
        match self {
            // NULL has no type of its own; TEXT is a safe default since the
            // server coerces it in typed positions.
            Self::Null | Self::Text(_) => Type::TEXT,
            Self::Bool(_) => Type::BOOL,
            Self::Int(_) => Type::INT8,
            Self::Real(_) => Type::FLOAT8,
        }
    }
}

/// Executes a configured mutation's SQL against a second Postgres database
/// ("DB B"), binding composed-document fields to the statement's positional
/// `$1…$n` parameters. One message → one composed doc → one execution;
/// cross-message batching is a known v1 limitation.
///
/// A single-statement mutation runs in autocommit. A multi-statement mutation
/// (`statements = […]`) wraps each message's writes in one explicit
/// transaction: all statements commit or none do, so a failure mid-way leaves
/// DB B untouched and redelivery starts clean.
struct GraphqlMutateConsumeSink {
    /// One dedicated connection per target URL; consume processes messages
    /// serially, so a pool would sit idle.
    client: Arc<tokio::sync::Mutex<tokio_postgres::Client>>,
    /// Prepared-statement cache keyed by SQL text; statements prepared on the
    /// client are valid inside that client's transactions too.
    cache: tokio::sync::Mutex<HashMap<String, tokio_postgres::Statement>>,
    mutation: String,
    params: Vec<String>,
    statements: Vec<String>,
}

impl GraphqlMutateConsumeSink {
    /// Resolve the named mutation: `name_in_kind` (config sink) falls back to
    /// `name_from_cli` (`--mutation`). Fails when unnamed, unknown to config,
    /// or invalid (must set exactly one of `sql` / `statements`).
    fn resolve<'a>(
        name: Option<&'a str>,
        mutations: &'a HashMap<String, MutationConfig>,
    ) -> Result<(&'a str, &'a MutationConfig)> {
        let name = name.ok_or_else(|| {
            anyhow!(
                "graphql-mutate sink requires --mutation <name> \
                 naming a [mutations.<name>] entry in ~/.pgx/config.toml"
            )
        })?;
        let cfg = mutations.get(name).ok_or_else(|| {
            anyhow!(
                "unknown mutation '{name}' — define it under [mutations.{name}] \
                 in ~/.pgx/config.toml"
            )
        })?;
        cfg.validate()
            .with_context(|| format!("mutation '{name}' config is invalid"))?;
        Ok((name, cfg))
    }

    /// Target database URL for the mutation: its own target_url wins, else the
    /// `--mutate-target-url` override.
    fn target_url<'a>(mcfg: &'a MutationConfig, url_override: Option<&'a str>) -> Result<&'a str> {
        mcfg.target_url.as_deref().or(url_override).ok_or_else(|| {
            anyhow!(
                "mutation has no target database — set target_url under [mutations.*] \
                 or pass --mutate-target-url"
            )
        })
    }

    /// Bind the configured field names against the composed document in
    /// positional order ($1, $2, …). A missing top-level field fails the
    /// message rather than silently binding NULL.
    fn bind_params(doc: &Value, fields: &[String], mutation: &str) -> Result<Vec<SqlParam>> {
        let obj = doc.as_object().ok_or_else(|| {
            anyhow!(
                "mutation '{mutation}' expected an object document, got {}",
                doc
            )
        })?;
        fields
            .iter()
            .map(|field| {
                obj.get(field).map(SqlParam::from_value).ok_or_else(|| {
                    anyhow!("mutation '{mutation}' missing field '{field}' in composed document")
                })
            })
            .collect()
    }

    /// The highest positional parameter (`$n`) a statement references, outside
    /// single-quoted literals. Each statement binds only up to that count, so
    /// one statement can use a prefix of the mutation's params (e.g. a DELETE
    /// keyed on `$1` alone). Known blind spot: `$n` inside dollar-quoted
    /// bodies inflates the count.
    fn max_param(sql: &str) -> usize {
        let bytes = sql.as_bytes();
        let mut max = 0;
        let mut i = 0;
        let mut in_string = false;
        while i < bytes.len() {
            match bytes[i] {
                b'\'' => {
                    if in_string && bytes.get(i + 1) == Some(&b'\'') {
                        i += 1; // escaped '' inside a literal
                    } else {
                        in_string = !in_string;
                    }
                }
                b'$' if !in_string => {
                    let mut j = i + 1;
                    let mut n = 0usize;
                    while j < bytes.len() && bytes[j].is_ascii_digit() {
                        n = n * 10 + (bytes[j] - b'0') as usize;
                        j += 1;
                    }
                    if j > i + 1 {
                        max = max.max(n);
                    }
                    i = j.saturating_sub(1);
                }
                _ => {}
            }
            i += 1;
        }
        max
    }

    /// Look up (or prepare and cache) the statement for `sql` on this
    /// connection. If the server cannot infer a parameter's type (`42P18`,
    /// common when a `$n` only appears under casts or set-returning
    /// functions), retry with explicit types taken from the bound values.
    async fn prepared(
        &self,
        client: &tokio_postgres::Client,
        sql: &str,
        bound: &[SqlParam],
    ) -> Result<tokio_postgres::Statement> {
        let mut cache = self.cache.lock().await;
        match cache.get(sql) {
            Some(stmt) => Ok(stmt.clone()),
            None => {
                let stmt = match client.prepare(sql).await {
                    Ok(stmt) => stmt,
                    // The server could not infer every parameter type; retry
                    // with explicit types taken from the bound values.
                    Err(_) => {
                        let types: Vec<tokio_postgres::types::Type> = (0..Self::max_param(sql))
                            .map(|i| {
                                bound
                                    .get(i)
                                    .map(SqlParam::pg_type)
                                    .unwrap_or(tokio_postgres::types::Type::TEXT)
                            })
                            .collect();
                        client.prepare_typed(sql, &types).await.with_context(|| {
                            format!("preparing statement failed: {}", sql.trim())
                        })?
                    }
                };
                cache.insert(sql.to_string(), stmt.clone());
                Ok(stmt)
            }
        }
    }

    /// The parameter references (`$1…`) a statement binds: a prefix of the
    /// fully bound list, cut at the highest `$n` the statement mentions.
    fn bind_slice<'a>(
        sql: &str,
        bound: &'a [SqlParam],
    ) -> Vec<&'a (dyn tokio_postgres::types::ToSql + Sync)> {
        let n = Self::max_param(sql).min(bound.len());
        bound[..n].iter().map(|p| p.as_dyn()).collect()
    }
}

#[async_trait]
impl ConsumeSink for GraphqlMutateConsumeSink {
    fn name(&self) -> &str {
        "graphql-mutate"
    }

    async fn send(&self, doc: &Value, _msg_id: Option<&str>) -> Result<()> {
        // Idempotency comes from the session-level dedupe cache in
        // --idempotent mode; statements should still be written idempotently
        // (upserts) since that cache is per-process.
        let bound = Self::bind_params(doc, &self.params, &self.mutation)?;
        // Serialize access: opening a transaction needs &mut on the client,
        // and concurrent sends would interleave statements anyway.
        let mut client = self.client.lock().await;

        if self.statements.len() == 1 {
            let sql = &self.statements[0];
            let stmt = self.prepared(&client, sql, &bound).await?;
            let refs = Self::bind_slice(sql, &bound);
            client
                .execute(&stmt, &refs)
                .await
                .with_context(|| format!("mutation '{}' failed", self.mutation))?;
            return Ok(());
        }

        // Multi-statement: prepare everything up front (statements prepared on
        // the client stay valid inside its transactions), then run all inside
        // one transaction. A transaction dropped on error rolls back, so DB B
        // is untouched unless every statement succeeded.
        let mut prepared: Vec<tokio_postgres::Statement> =
            Vec::with_capacity(self.statements.len());
        for sql in &self.statements {
            prepared.push(self.prepared(&client, sql, &bound).await?);
        }
        let tx = client
            .transaction()
            .await
            .context("mutation transaction failed to start")?;
        for (sql, stmt) in self.statements.iter().zip(&prepared) {
            let refs = Self::bind_slice(sql, &bound);
            tx.execute(stmt, &refs).await.with_context(|| {
                format!(
                    "mutation '{}' failed on statement: {}",
                    self.mutation,
                    sql.trim()
                )
            })?;
        }
        tx.commit()
            .await
            .with_context(|| format!("mutation '{}' commit failed", self.mutation))?;
        Ok(())
    }
}

// ── KV sink (Redis / Memcached) ───────────────────────────────────────────────

#[cfg(feature = "kv")]
use crate::consumer::kv::KvConsumeSink;

// ── Builders ─────────────────────────────────────────────────────────────────

/// Embedding-stage settings after merging CLI flags with connection-level
/// config and defaults. `active` is true whenever an embedding URL was
/// resolved, which wraps the delivery sink in [`EmbeddingSink`].
/// The fields are only read by the `embed`-gated sink wrapper, so they appear
/// dead in a `--no-default-features` build.
#[allow(dead_code)]
struct EffectiveEmbedConfig {
    url: String,
    api: EmbedApi,
    model: String,
    template: String,
    output_field: String,
    dim: Option<usize>,
}

impl EffectiveEmbedConfig {
    /// Merge CLI flags (win) with `consume.embed` config and defaults. `None`
    /// when the stage is not configured at all. Fails fast on an embed
    /// configuration that is present but incomplete (no URL, or no model).
    fn resolve(cli: &ConsumeArgs, cfg: Option<&ConsumeConfig>) -> Result<Option<Self>> {
        let embed_cfg: Option<&EmbedConfig> = cfg.and_then(|c| c.embed.as_ref());
        let configured = cli.embed_url.is_some()
            || cli.embed_api.is_some()
            || cli.embed_model.is_some()
            || cli.embed_field.is_some()
            || cli.embed_template.is_some()
            || cli.embed_output_field.is_some()
            || cli.embed_dim.is_some()
            || embed_cfg.is_some();

        if !configured {
            return Ok(None);
        }

        let url = cli
            .embed_url
            .clone()
            .or_else(|| embed_cfg.and_then(|e| e.url.clone()))
            .ok_or_else(|| {
                anyhow!(
                    "Embedding stage is configured but has no URL — provide --embed-url or consume.embed.url"
                )
            })?;
        let model = cli
            .embed_model
            .clone()
            .or_else(|| embed_cfg.and_then(|e| e.model.clone()))
            .ok_or_else(|| {
                anyhow!(
                    "--embed-url requires a model — provide --embed-model or consume.embed.model"
                )
            })?;
        let api = cli
            .embed_api
            .as_deref()
            .or_else(|| embed_cfg.and_then(|e| e.api.as_deref()))
            .unwrap_or("ollama")
            .parse::<EmbedApi>()
            .map_err(anyhow::Error::msg)?;
        let field = cli
            .embed_field
            .clone()
            .or_else(|| embed_cfg.and_then(|e| e.field.clone()))
            .unwrap_or_else(|| "content".to_string());
        let template = cli
            .embed_template
            .clone()
            .or_else(|| embed_cfg.and_then(|e| e.template.clone()))
            .unwrap_or_else(|| default_template(&field));
        let output_field = cli
            .embed_output_field
            .clone()
            .or_else(|| embed_cfg.and_then(|e| e.output_field.clone()))
            .unwrap_or_else(|| "embedding".to_string());
        let dim = cli.embed_dim.or_else(|| embed_cfg.and_then(|e| e.dim));

        Ok(Some(Self {
            url,
            api,
            model,
            template,
            output_field,
            dim,
        }))
    }
}

/// Resolve the delivery sink list: explicit CLI `--sink` flags win; else the
/// connection's `sink` plus `additional_sinks`; else stdout.
fn resolve_sink_kinds(args: &ConsumeArgs, cfg: Option<&ConsumeConfig>) -> Vec<ConsumeSinkKind> {
    if !args.sink.is_empty() {
        args.sink
            .iter()
            .map(|t| cli_sink_to_kind(args, t.clone()))
            .collect()
    } else if let Some(cfg) = cfg {
        std::iter::once(cfg.sink.clone())
            .chain(cfg.additional_sinks.iter().cloned())
            .collect()
    } else {
        vec![ConsumeSinkKind::Stdout]
    }
}

/// Whether a multi-sink fan-out runs without a stable sink key. With no
/// `--id-field` naming a document field and `--idempotent` off, sinks like
/// elasticsearch auto-generate their record id, so a redelivery after a
/// later-sink failure appends a duplicate instead of overwriting.
fn fanout_lacks_stable_keys(
    kinds: &[ConsumeSinkKind],
    idempotent: bool,
    id_field: Option<&str>,
) -> bool {
    kinds.len() > 1 && !idempotent && id_field.is_none()
}

/// Map a CLI sink type to a config kind, pulling its options from the args.
fn cli_sink_to_kind(args: &ConsumeArgs, t: ConsumeSinkType) -> ConsumeSinkKind {
    match t {
        ConsumeSinkType::Stdout => ConsumeSinkKind::Stdout,
        ConsumeSinkType::Elasticsearch => ConsumeSinkKind::Elasticsearch {
            url: args
                .es_url
                .clone()
                .unwrap_or_else(|| "http://localhost:9200".to_string()),
            index: args.index.clone().unwrap_or_else(|| "pgx".to_string()),
            id_field: args.id_field.clone(),
        },
        ConsumeSinkType::Webhook => ConsumeSinkKind::Webhook {
            url: args.webhook_url.clone().unwrap_or_default(),
            headers: None,
        },
        ConsumeSinkType::Kv => ConsumeSinkKind::Kv {
            url: args
                .kv_url
                .clone()
                .unwrap_or_else(|| "redis://localhost:6379".to_string()),
            key_field: args.key_field.clone(),
            key_prefix: args.key_prefix.clone(),
            ttl: args.ttl,
        },
        ConsumeSinkType::PostgresVector => ConsumeSinkKind::PostgresVector {
            table: args.vector_table.clone(),
        },
        ConsumeSinkType::GraphqlMutate => ConsumeSinkKind::GraphqlMutate {
            mutation: args.mutation.clone(),
            target_url: args.mutate_target_url.clone(),
        },
    }
}

/// Build the delivery sinks for the resolved kind list (single sink, or a
/// fan-out), then wrap them in the embedding decorator when the stage is
/// active. `mutate_clients` holds one pre-connected client per distinct
/// graphql-mutate target URL.
#[allow(unused_variables)]
async fn build_sinks(
    args: &ConsumeArgs,
    cfg: Option<&ConsumeConfig>,
    pool: Arc<QueryConn>,
    embed: Option<&EffectiveEmbedConfig>,
    mutations: &HashMap<String, MutationConfig>,
    mutate_clients: &HashMap<String, Arc<tokio::sync::Mutex<tokio_postgres::Client>>>,
) -> Result<Arc<dyn ConsumeSink>> {
    let kinds = resolve_sink_kinds(args, cfg);
    let mut sinks = Vec::with_capacity(kinds.len());
    for kind in &kinds {
        sinks.push(build_one_sink(args, kind, cfg, &pool, embed, mutations, mutate_clients).await?);
    }

    let base: Arc<dyn ConsumeSink> = if sinks.len() == 1 {
        sinks.pop().expect("one sink")
    } else {
        Arc::new(FanoutConsumeSink { sinks })
    };

    match embed {
        Some(e) => {
            #[cfg(feature = "embed")]
            {
                Ok(Arc::new(EmbeddingSink {
                    inner: base,
                    embedder: Arc::new(EmbedClient::new(&e.url, e.api, &e.model)?),
                    template: e.template.clone(),
                    output_field: e.output_field.clone(),
                    dim: e.dim,
                }))
            }
            #[cfg(not(feature = "embed"))]
            {
                anyhow::bail!("embedding stage requires the 'embed' feature")
            }
        }
        None => Ok(base),
    }
}

#[allow(unused_variables)]
async fn build_one_sink(
    args: &ConsumeArgs,
    kind: &ConsumeSinkKind,
    cfg: Option<&ConsumeConfig>,
    pool: &Arc<QueryConn>,
    embed: Option<&EffectiveEmbedConfig>,
    mutations: &HashMap<String, MutationConfig>,
    mutate_clients: &HashMap<String, Arc<tokio::sync::Mutex<tokio_postgres::Client>>>,
) -> Result<Arc<dyn ConsumeSink>> {
    match kind {
        ConsumeSinkKind::Stdout => Ok(Arc::new(StdoutConsumeSink)),

        ConsumeSinkKind::Elasticsearch {
            url,
            index,
            id_field,
        } => {
            #[cfg(feature = "elasticsearch")]
            {
                let es = crate::downstream::delivery::elasticsearch::Elasticsearch::new(url)?;
                Ok(Arc::new(ElasticsearchConsumeSink {
                    index: index.clone(),
                    id_field: id_field.clone(),
                    es,
                }))
            }
            #[cfg(not(feature = "elasticsearch"))]
            {
                anyhow::bail!("Elasticsearch sink requires the 'elasticsearch' feature")
            }
        }

        ConsumeSinkKind::Webhook { url, .. } => {
            #[cfg(feature = "webhook")]
            {
                if url.is_empty() {
                    anyhow::bail!(
                        "Webhook URL is required — provide --webhook-url or set WEBHOOK_URL env"
                    );
                }
                let webhook = crate::downstream::delivery::webhook::Webhook::with_retries(0);
                Ok(Arc::new(WebhookConsumeSink {
                    url: url.clone(),
                    webhook,
                }))
            }
            #[cfg(not(feature = "webhook"))]
            {
                anyhow::bail!("Webhook sink requires the 'webhook' feature")
            }
        }

        ConsumeSinkKind::Kv {
            url,
            key_field,
            key_prefix,
            ttl,
        } => {
            #[cfg(feature = "kv")]
            {
                let sink = KvConsumeSink::connect(
                    url,
                    key_prefix.as_deref().unwrap_or("pgx:"),
                    key_field.clone(),
                    ttl.unwrap_or(0),
                )
                .await?;
                Ok(Arc::new(sink))
            }
            #[cfg(not(feature = "kv"))]
            {
                anyhow::bail!("KV sink requires the 'kv' feature")
            }
        }

        ConsumeSinkKind::PostgresVector { table } => {
            let table = table
                .clone()
                .or_else(|| args.vector_table.clone())
                .or_else(|| cfg.and_then(|c| c.vector_table.clone()))
                .unwrap_or_else(|| "chunk_embeddings".to_string());
            let embedding_field = embed
                .map(|e| e.output_field.clone())
                .unwrap_or_else(|| "embedding".to_string());
            Ok(Arc::new(PostgresVectorConsumeSink {
                pool: Arc::clone(pool),
                table,
                id_field: args.id_field.clone(),
                embedding_field,
            }))
        }

        ConsumeSinkKind::GraphqlMutate {
            mutation,
            target_url,
        } => {
            let (name, mcfg) = GraphqlMutateConsumeSink::resolve(
                mutation.as_deref().or(args.mutation.as_deref()),
                mutations,
            )?;
            let url = GraphqlMutateConsumeSink::target_url(mcfg, target_url.as_deref())?;
            let client = mutate_clients.get(url).ok_or_else(|| {
                anyhow!("mutation '{name}': no client connected for target {url}")
            })?;
            Ok(Arc::new(GraphqlMutateConsumeSink {
                client: Arc::clone(client),
                cache: tokio::sync::Mutex::new(HashMap::new()),
                mutation: name.to_string(),
                params: mcfg.params.clone(),
                statements: mcfg.statement_list().to_vec(),
            }))
        }
    }
}

#[allow(unused_variables)]
async fn build_consumer(args: &ConsumeArgs) -> Result<Arc<dyn Consumer>> {
    match args.source {
        #[cfg(feature = "rabbitmq")]
        ConsumeSourceType::Rabbitmq => {
            let amqp_url = args
                .amqp_url
                .as_deref()
                .unwrap_or("amqp://guest:guest@localhost:5672/%2F");
            let queue = args.queue.as_deref().unwrap_or("pgx-events");
            let exchange = args.exchange.as_deref();
            let routing_key = args.routing_key.as_deref();
            let c = crate::consumer::rabbitmq::rabbitmq::RabbitMqConsumer::connect(
                amqp_url,
                queue,
                exchange,
                routing_key,
                args.prefetch_count.unwrap_or(0),
            )
            .await?;
            Ok(Arc::new(c))
        }

        #[cfg(not(feature = "rabbitmq"))]
        ConsumeSourceType::Rabbitmq => {
            anyhow::bail!("RabbitMQ consumer requires the 'rabbitmq' feature")
        }

        #[cfg(feature = "kafka")]
        ConsumeSourceType::Kafka => {
            let brokers = args.brokers.as_deref().unwrap_or("localhost:9092");
            let topic = args.topic.as_deref().unwrap_or("pgx-events");
            let group_id = args.group_id.as_deref().unwrap_or("pgx-consume");
            let c = crate::consumer::kafka::kafka::KafkaConsumer::connect(brokers, topic, group_id)
                .await?;
            Ok(Arc::new(c))
        }

        #[cfg(not(feature = "kafka"))]
        ConsumeSourceType::Kafka => {
            anyhow::bail!("Kafka consumer requires the 'kafka' feature")
        }

        #[cfg(feature = "nats")]
        ConsumeSourceType::Nats => {
            let url = args.nats_url.as_deref().unwrap_or("nats://localhost:4222");
            let stream = args.nats_stream.as_deref().unwrap_or("pgx-events");
            let subject = args.nats_subject.as_deref();
            let durable = args.nats_consumer.as_deref().unwrap_or("pgx-consume");
            let c = crate::consumer::nats::nats::NatsConsumer::connect(
                url,
                stream,
                subject,
                durable,
                args.nats_create_stream,
            )
            .await?;
            Ok(Arc::new(c))
        }

        #[cfg(not(feature = "nats"))]
        ConsumeSourceType::Nats => {
            anyhow::bail!("NATS consumer requires the 'nats' feature")
        }
    }
}

// ── Composition ──────────────────────────────────────────────────────────────

/// Production wiring of the [`Compose`] seam: executes a named query against the
/// GraphQL query pool with the configured resolvers and recursion depth.
struct GraphqlCompose {
    pool: Arc<QueryConn>,
    resolvers: Arc<HashMap<String, ResolverConfig>>,
    max_depth: u32,
}

#[async_trait]
impl Compose for GraphqlCompose {
    async fn compose(
        &self,
        query: &NamedQuery,
        variables: &HashMap<String, Value>,
    ) -> Result<Value> {
        executor::execute(
            query,
            variables,
            &self.resolvers,
            self.pool.as_ref(),
            self.max_depth,
        )
        .await
    }
}

// ── Resolve schema dir ──────────────────────────────────────────────────────

fn resolve_schema_dir(override_dir: Option<&str>) -> Result<PathBuf> {
    let home = dirs::home_dir().context("Cannot determine home directory")?;
    if let Some(dir) = override_dir {
        let expanded = dir.replace('~', &home.to_string_lossy());
        return Ok(PathBuf::from(expanded));
    }
    Ok(home.join(".pgx").join("schema"))
}

// ── Effective consume config ─────────────────────────────────────────────────

/// Consume settings after merging CLI flags with connection-level config, then
/// built-in defaults. Produced once by explicit precedence rules (explicit CLI
/// wins over config, which wins over defaults); no code compares against clap
/// defaults, so a default can move without silently breaking the merge.
struct EffectiveConsumeConfig {
    query: Option<String>,
    schema_dir: Option<String>,
    max_depth: u32,
    query_mode: ConsumeQueryMode,
    on_error: ConsumeErrorMode,
    idempotent: bool,
    dedup_ttl: Option<u64>,
}

impl EffectiveConsumeConfig {
    fn merge(cli: &ConsumeArgs, cfg: Option<&ConsumeConfig>) -> Self {
        let max_depth = cli.max_depth.or(cfg.and_then(|c| c.max_depth)).unwrap_or(8);
        let query_mode = cli
            .query_mode
            .clone()
            .or_else(|| {
                cfg.and_then(|c| c.query_mode.as_deref())
                    .and_then(|m| m.parse::<ConsumeQueryMode>().ok())
            })
            .unwrap_or(ConsumeQueryMode::Contract);
        let on_error = cli
            .on_error
            .clone()
            .or_else(|| {
                cfg.and_then(|c| c.on_error.as_deref())
                    .and_then(|m| m.parse::<ConsumeErrorMode>().ok())
            })
            .unwrap_or(ConsumeErrorMode::Lenient);
        Self {
            query: cli
                .query
                .clone()
                .or_else(|| cfg.and_then(|c| c.query.clone())),
            schema_dir: cli
                .schema_dir
                .clone()
                .or_else(|| cfg.and_then(|c| c.schema_dir.clone())),
            max_depth,
            query_mode,
            on_error,
            idempotent: cli.idempotent || cfg.is_some_and(|c| c.idempotent.unwrap_or(false)),
            dedup_ttl: cli.dedup_ttl.or(cfg.and_then(|c| c.dedup_ttl)),
        }
    }
}

// ── Run ─────────────────────────────────────────────────────────────────────

pub async fn run(
    url: String,
    mut args: ConsumeArgs,
    conn: Option<&Connection>,
    use_tls: bool,
    resolvers: &HashMap<String, ResolverConfig>,
    mutations: &HashMap<String, MutationConfig>,
) -> Result<()> {
    // ── Merge connection-level defaults into an effective config ────────────
    let cfg = conn.and_then(|c| c.consume.as_ref());
    if let Some(cfg) = cfg {
        // Source defaults
        args.source = match cfg.source {
            ConsumeSourceKind::Rabbitmq { .. } => ConsumeSourceType::Rabbitmq,
            ConsumeSourceKind::Kafka { .. } => ConsumeSourceType::Kafka,
            ConsumeSourceKind::Nats { .. } => ConsumeSourceType::Nats,
        };
        merge_source_config(&mut args, &cfg.source);
    }
    let eff = EffectiveConsumeConfig::merge(&args, cfg);

    // ── Embedding stage: resolve once, before the session loop ──────────────
    let eff_embed = EffectiveEmbedConfig::resolve(&args, cfg)?;
    let sink_kinds = resolve_sink_kinds(&args, cfg);
    if eff_embed.is_none()
        && sink_kinds
            .iter()
            .any(|k| matches!(k, ConsumeSinkKind::PostgresVector { .. }))
    {
        anyhow::bail!(
            "postgres-vector sink requires the embedding stage — provide --embed-url (or consume.embed.url)"
        );
    }

    // ── Validate graphql-mutate sinks and connect their target clients ───────
    // One dedicated connection per distinct target URL, shared across
    // mutations pointing at the same DB B.
    let mut mutate_clients: HashMap<String, Arc<tokio::sync::Mutex<tokio_postgres::Client>>> =
        HashMap::new();
    for kind in sink_kinds
        .iter()
        .filter(|k| matches!(k, ConsumeSinkKind::GraphqlMutate { .. }))
    {
        let ConsumeSinkKind::GraphqlMutate {
            mutation,
            target_url,
        } = kind
        else {
            unreachable!("filtered above")
        };
        let (name, mcfg) = GraphqlMutateConsumeSink::resolve(
            mutation.as_deref().or(args.mutation.as_deref()),
            mutations,
        )?;
        let url = GraphqlMutateConsumeSink::target_url(mcfg, target_url.as_deref())?;
        info!("Connecting mutation '{name}' to {url}");
        let client = crate::utils::db::connect(url, use_tls)
            .await
            .with_context(|| format!("connecting mutation '{name}' to target database"))?;
        mutate_clients.insert(url.to_string(), Arc::new(tokio::sync::Mutex::new(client)));
    }

    // A fan-out with no stable sink key appends on redelivery instead of
    // overwriting (elasticsearch auto-generates its _id). Surface it up front.
    if fanout_lacks_stable_keys(&sink_kinds, eff.idempotent, args.id_field.as_deref()) {
        warn!(
            "fan-out across {} sinks with no stable sink key (no --idempotent, no --id-field): a redelivery after a later-sink failure can duplicate records. Add --idempotent or --id-field so redelivery upserts instead of appends.",
            sink_kinds.len()
        );
    }

    // Validate simple mode requires --query
    if matches!(eff.query_mode, ConsumeQueryMode::Simple) && eff.query.is_none() {
        anyhow::bail!("Simple query mode requires --query <name> or consume.query in config");
    }

    // ── Load schema and queries (once, outside reconnection loop) ────────────
    let schema_dir = resolve_schema_dir(eff.schema_dir.as_deref())?;
    let schema = SchemaRegistry::load_from_dir(&schema_dir)?;
    let queries = Arc::new(QueryLoader::load(&schema)?);
    info!(
        "Loaded {} type definitions, {} queries",
        schema.types.len(),
        queries.queries.len()
    );

    // ── Build GraphQL query pool (once) ──────────────────────────────────────
    let pool = Arc::new(QueryConn::connect(&url, use_tls).await?);
    info!("Connected GraphQL query pool to PostgreSQL");

    // ── Resolve default query name (contract mode fallback) ──────────────────
    let default_query = eff.query.clone().unwrap_or_else(|| "default".to_string());

    // ── Build sinks (once), wrapped in the embedding decorator if configured ─
    let sink: Arc<dyn ConsumeSink> = build_sinks(
        &args,
        cfg,
        pool.clone(),
        eff_embed.as_ref(),
        mutations,
        &mutate_clients,
    )
    .await?;
    info!("Using {} sink", sink.name());

    // ── Wire the composition seam to the GraphQL executor ────────────────────
    let compose: Arc<dyn Compose> = Arc::new(GraphqlCompose {
        pool: pool.clone(),
        resolvers: Arc::new(resolvers.clone()),
        max_depth: eff.max_depth,
    });

    // ── Build the session (owns the dedupe lifecycle) ────────────────────────
    let session = Arc::new(ConsumeSession::new(
        eff.query_mode.clone(),
        eff.on_error.clone(),
        eff.idempotent,
        eff.dedup_ttl,
        default_query,
        queries,
        sink,
        compose,
    ));

    info!(
        "Starting consume loop (mode={:?}, error={:?})",
        eff.query_mode, eff.on_error
    );

    let reconnect = ReconnectConfig {
        max_attempts: args.max_reconnect_attempts,
        base_ms: args.reconnect_base_ms,
        max_ms: args.reconnect_max_ms,
    };

    let args = Arc::new(args);

    // ── Session-loop: reconnect with backoff until shutdown or a fatal error ─
    session_loop::run(
        move |shutdown| {
            let session = session.clone();
            let args = args.clone();
            async move {
                // ── Build consumer ───────────────────────────────────────────
                let consumer: Arc<dyn Consumer> = match build_consumer(args.as_ref()).await {
                    Ok(c) => c,
                    Err(e) => {
                        error!(error = %e, "Failed to connect consumer");
                        return SessionExit::Reconnect;
                    }
                };
                info!("Connected to {} consumer", consumer.name());

                let mut shutdown = shutdown;
                session.run(consumer.as_ref(), &mut shutdown).await
            }
        },
        shutdown_signal(),
        &reconnect,
    )
    .await
}

fn merge_source_config(args: &mut ConsumeArgs, source: &ConsumeSourceKind) {
    match source {
        ConsumeSourceKind::Rabbitmq {
            amqp_url,
            queue,
            exchange,
            routing_key,
            prefetch_count,
        } => {
            if args.amqp_url.is_none() && amqp_url.is_some() {
                args.amqp_url = amqp_url.clone();
            }
            if args.queue.is_none() && queue.is_some() {
                args.queue = queue.clone();
            }
            if args.exchange.is_none() && exchange.is_some() {
                args.exchange = exchange.clone();
            }
            if args.routing_key.is_none() && routing_key.is_some() {
                args.routing_key = routing_key.clone();
            }
            if args.prefetch_count.is_none() && prefetch_count.is_some() {
                args.prefetch_count = *prefetch_count;
            }
        }
        ConsumeSourceKind::Kafka {
            brokers,
            topic,
            group_id,
        } => {
            if args.brokers.is_none() && brokers.is_some() {
                args.brokers = brokers.clone();
            }
            if args.topic.is_none() && topic.is_some() {
                args.topic = topic.clone();
            }
            if args.group_id.is_none() && group_id.is_some() {
                args.group_id = group_id.clone();
            }
        }
        ConsumeSourceKind::Nats {
            url,
            stream,
            subject,
            consumer,
            create_stream,
        } => {
            if args.nats_url.is_none() && url.is_some() {
                args.nats_url = url.clone();
            }
            if args.nats_stream.is_none() && stream.is_some() {
                args.nats_stream = stream.clone();
            }
            if args.nats_subject.is_none() && subject.is_some() {
                args.nats_subject = subject.clone();
            }
            if args.nats_consumer.is_none() && consumer.is_some() {
                args.nats_consumer = consumer.clone();
            }
            if !args.nats_create_stream && create_stream.unwrap_or(false) {
                args.nats_create_stream = true;
            }
        }
    }
}

// ── Parse helpers ───────────────────────────────────────────────────────────

impl std::str::FromStr for ConsumeQueryMode {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "simple" => Ok(Self::Simple),
            "contract" => Ok(Self::Contract),
            other => Err(format!(
                "unknown query mode '{other}'; expected simple|contract"
            )),
        }
    }
}

impl std::str::FromStr for ConsumeErrorMode {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "lenient" => Ok(Self::Lenient),
            "strict" => Ok(Self::Strict),
            other => Err(format!(
                "unknown error mode '{other}'; expected lenient|strict"
            )),
        }
    }
}

#[cfg(test)]
#[cfg(feature = "elasticsearch")]
mod tests {
    use crate::downstream::delivery::elasticsearch::Elasticsearch;
    use serde_json::json;

    #[test]
    fn es_id_explicit_field_wins() {
        let doc = json!({"mat_no": "M001", "name": "steel"});
        assert_eq!(
            Elasticsearch::doc_id(Some("mat_no"), &doc, Some("msg-7")),
            Some("M001".into())
        );
    }

    #[test]
    fn es_id_falls_back_to_msg_id() {
        let doc = json!({"name": "steel"});
        assert_eq!(
            Elasticsearch::doc_id(None, &doc, Some("msg-7")),
            Some("msg-7".into())
        );
        assert_eq!(
            Elasticsearch::doc_id(Some("missing"), &doc, Some("msg-7")),
            Some("msg-7".into())
        );
    }

    #[test]
    fn es_id_none_without_idempotence() {
        let doc = json!({"name": "steel"});
        assert_eq!(Elasticsearch::doc_id(None, &doc, None), None);
    }

    #[test]
    fn es_id_ignores_non_string_field() {
        let doc = json!({"mat_no": 42});
        assert_eq!(
            Elasticsearch::doc_id(Some("mat_no"), &doc, Some("msg-7")),
            Some("msg-7".into())
        );
    }
}

#[cfg(test)]
mod error_policy_tests {
    use super::{ConsumeErrorMode, ErrorAction, ErrorStage};

    #[test]
    fn lenient_discards_per_message_failures() {
        for stage in [ErrorStage::Parse, ErrorStage::Lookup, ErrorStage::Compose] {
            assert_eq!(
                ConsumeErrorMode::Lenient.handle(stage),
                ErrorAction::Discard
            );
        }
    }

    #[test]
    fn lenient_requeues_transient_sink_failures() {
        assert_eq!(
            ConsumeErrorMode::Lenient.handle(ErrorStage::Sink),
            ErrorAction::Requeue
        );
    }

    #[test]
    fn strict_aborts_on_every_failure() {
        for stage in [
            ErrorStage::Parse,
            ErrorStage::Lookup,
            ErrorStage::Compose,
            ErrorStage::Sink,
        ] {
            assert_eq!(ConsumeErrorMode::Strict.handle(stage), ErrorAction::Abort);
        }
    }
}

#[cfg(test)]
mod effective_config_tests {
    use super::*;
    use crate::utils::config::{ConsumeConfig, ConsumeSinkKind, ConsumeSourceKind};

    pub(super) fn cli() -> ConsumeArgs {
        ConsumeArgs {
            source: ConsumeSourceType::Rabbitmq,
            sink: vec![],
            amqp_url: None,
            queue: None,
            exchange: None,
            routing_key: None,
            prefetch_count: None,
            brokers: None,
            topic: None,
            group_id: None,
            nats_url: None,
            nats_stream: None,
            nats_subject: None,
            nats_consumer: None,
            nats_create_stream: false,
            query_mode: None,
            query: None,
            max_depth: None,
            schema_dir: None,
            on_error: None,
            idempotent: false,
            dedup_ttl: None,
            max_reconnect_attempts: 0,
            reconnect_base_ms: 1000,
            reconnect_max_ms: 30000,
            es_url: None,
            index: None,
            id_field: None,
            webhook_url: None,
            kv_url: None,
            key_field: None,
            key_prefix: None,
            ttl: None,
            embed_url: None,
            embed_api: None,
            embed_model: None,
            embed_field: None,
            embed_template: None,
            embed_output_field: None,
            embed_dim: None,
            vector_table: None,
            mutation: None,
            mutate_target_url: None,
        }
    }

    pub(super) fn cfg() -> ConsumeConfig {
        ConsumeConfig {
            source: ConsumeSourceKind::Rabbitmq {
                amqp_url: None,
                queue: None,
                exchange: None,
                routing_key: None,
                prefetch_count: None,
            },
            sink: ConsumeSinkKind::Stdout,
            query_mode: Some("contract".to_string()),
            query: Some("cfg-query".to_string()),
            max_depth: Some(16),
            schema_dir: Some("cfg-schema".to_string()),
            on_error: Some("lenient".to_string()),
            idempotent: Some(true),
            dedup_ttl: Some(60),
            embed: None,
            additional_sinks: vec![],
            vector_table: None,
        }
    }

    #[test]
    fn explicit_cli_wins_over_config() {
        let mut c = cli();
        c.query_mode = Some(ConsumeQueryMode::Simple);
        c.on_error = Some(ConsumeErrorMode::Strict);
        c.max_depth = Some(3);
        c.query = Some("cli-query".to_string());
        c.schema_dir = Some("cli-schema".to_string());
        c.idempotent = true;
        c.dedup_ttl = Some(5);

        let eff = EffectiveConsumeConfig::merge(&c, Some(&cfg()));
        assert_eq!(eff.query_mode, ConsumeQueryMode::Simple);
        assert_eq!(eff.on_error, ConsumeErrorMode::Strict);
        assert_eq!(eff.max_depth, 3);
        assert_eq!(eff.query.as_deref(), Some("cli-query"));
        assert_eq!(eff.schema_dir.as_deref(), Some("cli-schema"));
        assert!(eff.idempotent);
        assert_eq!(eff.dedup_ttl, Some(5));
    }

    #[test]
    fn config_fills_when_cli_absent() {
        let eff = EffectiveConsumeConfig::merge(&cli(), Some(&cfg()));
        assert_eq!(eff.query_mode, ConsumeQueryMode::Contract);
        assert_eq!(eff.on_error, ConsumeErrorMode::Lenient);
        assert_eq!(eff.max_depth, 16);
        assert_eq!(eff.query.as_deref(), Some("cfg-query"));
        assert_eq!(eff.schema_dir.as_deref(), Some("cfg-schema"));
        assert!(eff.idempotent);
        assert_eq!(eff.dedup_ttl, Some(60));
    }

    #[test]
    fn defaults_apply_when_nothing_provided() {
        let eff = EffectiveConsumeConfig::merge(&cli(), None);
        assert_eq!(eff.query_mode, ConsumeQueryMode::Contract);
        assert_eq!(eff.on_error, ConsumeErrorMode::Lenient);
        assert_eq!(eff.max_depth, 8);
        assert_eq!(eff.query, None);
        assert_eq!(eff.schema_dir, None);
        assert!(!eff.idempotent);
        assert_eq!(eff.dedup_ttl, None);
    }

    #[test]
    fn explicit_cli_default_is_respected_over_config() {
        // The regression this merge fixes: an explicit `--max-depth 8` used to
        // be indistinguishable from the clap default, so config silently won.
        let mut c = cli();
        c.max_depth = Some(8);
        let eff = EffectiveConsumeConfig::merge(&c, Some(&cfg()));
        assert_eq!(eff.max_depth, 8);
    }
}

#[cfg(test)]
mod sink_resolution_tests {
    use super::effective_config_tests::{cfg, cli};
    use super::*;

    #[test]
    fn defaults_to_stdout_without_cli_or_config() {
        assert_eq!(
            resolve_sink_kinds(&cli(), None),
            vec![ConsumeSinkKind::Stdout]
        );
    }

    #[test]
    fn cli_sinks_win_over_config() {
        let mut c = cli();
        c.sink = vec![ConsumeSinkType::Elasticsearch];
        assert_eq!(
            resolve_sink_kinds(&c, Some(&cfg())),
            vec![ConsumeSinkKind::Elasticsearch {
                url: "http://localhost:9200".to_string(),
                index: "pgx".to_string(),
                id_field: None,
            }]
        );
    }

    #[test]
    fn config_sink_plus_additional_sinks_when_no_cli() {
        let mut c = cfg();
        c.additional_sinks = vec![ConsumeSinkKind::PostgresVector { table: None }];
        assert_eq!(
            resolve_sink_kinds(&cli(), Some(&c)),
            vec![
                ConsumeSinkKind::Stdout,
                ConsumeSinkKind::PostgresVector { table: None }
            ]
        );
    }

    #[test]
    fn cli_postgres_vector_carries_vector_table() {
        let mut c = cli();
        c.sink = vec![ConsumeSinkType::PostgresVector];
        c.vector_table = Some("embeddings".to_string());
        assert_eq!(
            resolve_sink_kinds(&c, None),
            vec![ConsumeSinkKind::PostgresVector {
                table: Some("embeddings".to_string())
            }]
        );
    }

    #[test]
    fn cli_graphql_mutate_carries_mutation_and_target_url() {
        let mut c = cli();
        c.sink = vec![ConsumeSinkType::GraphqlMutate];
        c.mutation = Some("upsert_material".to_string());
        c.mutate_target_url = Some("postgres://dbb".to_string());
        assert_eq!(
            resolve_sink_kinds(&c, None),
            vec![ConsumeSinkKind::GraphqlMutate {
                mutation: Some("upsert_material".to_string()),
                target_url: Some("postgres://dbb".to_string()),
            }]
        );
    }

    fn fanout_kinds() -> Vec<ConsumeSinkKind> {
        vec![
            ConsumeSinkKind::Stdout,
            ConsumeSinkKind::Elasticsearch {
                url: "http://localhost:9200".to_string(),
                index: "pgx".to_string(),
                id_field: None,
            },
        ]
    }

    #[test]
    fn fanout_without_stable_key_warns() {
        assert!(fanout_lacks_stable_keys(&fanout_kinds(), false, None));
    }

    #[test]
    fn single_sink_never_warns() {
        let kinds = vec![ConsumeSinkKind::Stdout];
        assert!(!fanout_lacks_stable_keys(&kinds, false, None));
    }

    #[test]
    fn idempotent_or_id_field_suppress_fanout_warning() {
        assert!(!fanout_lacks_stable_keys(&fanout_kinds(), true, None));
        assert!(!fanout_lacks_stable_keys(
            &fanout_kinds(),
            false,
            Some("mat_no")
        ));
    }
}

#[cfg(test)]
mod embed_config_tests {
    use super::effective_config_tests::{cfg, cli};
    use super::*;
    use crate::embed::EmbedApi;
    use crate::utils::config::EmbedConfig;

    #[test]
    fn not_configured_is_none() {
        assert!(EffectiveEmbedConfig::resolve(&cli(), None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn url_and_model_from_cli() {
        let mut c = cli();
        c.embed_url = Some("http://localhost:11434".to_string());
        c.embed_model = Some("bge-m3".to_string());
        let e = EffectiveEmbedConfig::resolve(&c, None)
            .unwrap()
            .expect("active");
        assert_eq!(e.url, "http://localhost:11434");
        assert_eq!(e.model, "bge-m3");
        assert_eq!(e.api, EmbedApi::Ollama);
        assert_eq!(e.template, "{{content}}");
        assert_eq!(e.output_field, "embedding");
        assert_eq!(e.dim, None);
    }

    #[test]
    fn config_fills_when_cli_absent() {
        let mut cfg = cfg();
        cfg.embed = Some(EmbedConfig {
            url: Some("http://ollama:11434".to_string()),
            api: Some("openai".to_string()),
            model: Some("text-embedding-3-small".to_string()),
            field: Some("title".to_string()),
            output_field: Some("vec".to_string()),
            dim: Some(1536),
            ..EmbedConfig::default()
        });
        let e = EffectiveEmbedConfig::resolve(&cli(), Some(&cfg))
            .unwrap()
            .expect("active");
        assert_eq!(e.url, "http://ollama:11434");
        assert_eq!(e.api, EmbedApi::Openai);
        assert_eq!(e.template, "{{title}}");
        assert_eq!(e.output_field, "vec");
        assert_eq!(e.dim, Some(1536));
    }

    #[test]
    fn cli_wins_over_config() {
        let mut c = cli();
        c.embed_url = Some("http://cli:11434".to_string());
        c.embed_model = Some("cli-model".to_string());
        c.embed_api = Some("openai".to_string());
        c.embed_dim = Some(512);
        let mut cfg = cfg();
        cfg.embed = Some(EmbedConfig {
            url: Some("http://cfg:11434".to_string()),
            api: Some("ollama".to_string()),
            model: Some("cfg-model".to_string()),
            dim: Some(1024),
            ..EmbedConfig::default()
        });
        let e = EffectiveEmbedConfig::resolve(&c, Some(&cfg))
            .unwrap()
            .expect("active");
        assert_eq!(e.url, "http://cli:11434");
        assert_eq!(e.model, "cli-model");
        assert_eq!(e.api, EmbedApi::Openai);
        assert_eq!(e.dim, Some(512));
    }

    #[test]
    fn url_without_model_fails_fast() {
        let mut c = cli();
        c.embed_url = Some("http://localhost:11434".to_string());
        assert!(EffectiveEmbedConfig::resolve(&c, None).is_err());
    }

    #[test]
    fn embed_section_without_url_fails_fast() {
        let mut cfg = cfg();
        cfg.embed = Some(EmbedConfig {
            model: Some("bge-m3".to_string()),
            ..EmbedConfig::default()
        });
        assert!(EffectiveEmbedConfig::resolve(&cli(), Some(&cfg)).is_err());
    }

    #[test]
    fn invalid_api_fails_fast() {
        let mut c = cli();
        c.embed_url = Some("http://localhost:11434".to_string());
        c.embed_model = Some("bge-m3".to_string());
        c.embed_api = Some("azure".to_string());
        assert!(EffectiveEmbedConfig::resolve(&c, None).is_err());
    }
}

#[cfg(all(test, feature = "embed"))]
mod embedding_sink_tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    struct FakeEmbed {
        out: Vec<f32>,
        recorded: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl Embed for FakeEmbed {
        async fn embed(&self, text: &str) -> Result<Vec<f32>> {
            self.recorded.lock().unwrap().push(text.to_string());
            Ok(self.out.clone())
        }
    }

    struct RecordingSink {
        docs: Mutex<Vec<Value>>,
    }

    #[async_trait]
    impl ConsumeSink for RecordingSink {
        fn name(&self) -> &str {
            "recording"
        }

        async fn send(&self, doc: &Value, _msg_id: Option<&str>) -> Result<()> {
            self.docs.lock().unwrap().push(doc.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn attaches_vector_under_output_field() {
        let embedder = Arc::new(FakeEmbed {
            out: vec![0.1, 0.2],
            recorded: Mutex::new(Vec::new()),
        });
        let inner = Arc::new(RecordingSink {
            docs: Mutex::new(Vec::new()),
        });
        let decorator = EmbeddingSink {
            inner: inner.clone(),
            embedder: embedder.clone(),
            template: "{{content}}".to_string(),
            output_field: "embedding".to_string(),
            dim: None,
        };

        let doc = json!({ "content": "premium cotton" });
        decorator.send(&doc, Some("m1")).await.unwrap();

        let received = &inner.docs.lock().unwrap()[0];
        // serde_json renders f32 through f64, so compare with the same
        // representation rather than decimal f64 literals.
        assert_eq!(received["embedding"], json!([0.1f32, 0.2f32]));
        assert_eq!(received["content"], "premium cotton");
        // The original document is not mutated by the decorator.
        assert!(doc.get("embedding").is_none());
    }

    #[tokio::test]
    async fn template_selects_the_embedded_text() {
        let embedder = Arc::new(FakeEmbed {
            out: vec![1.0],
            recorded: Mutex::new(Vec::new()),
        });
        let inner = Arc::new(RecordingSink {
            docs: Mutex::new(Vec::new()),
        });
        let decorator = EmbeddingSink {
            inner: inner.clone(),
            embedder: embedder.clone(),
            template: "{{content}}\n-- {{source}} --".to_string(),
            output_field: "embedding".to_string(),
            dim: None,
        };

        let doc = json!({ "content": "hello", "source": "catalog" });
        decorator.send(&doc, Some("m1")).await.unwrap();

        let recorded = embedder.recorded.lock().unwrap();
        assert_eq!(*recorded, vec!["hello\n-- catalog --"]);
    }

    #[tokio::test]
    async fn dimension_mismatch_fails_the_sink_stage() {
        let embedder = Arc::new(FakeEmbed {
            out: vec![0.1, 0.2],
            recorded: Mutex::new(Vec::new()),
        });
        let inner = Arc::new(RecordingSink {
            docs: Mutex::new(Vec::new()),
        });
        let decorator = EmbeddingSink {
            inner: inner.clone(),
            embedder,
            template: "{{content}}".to_string(),
            output_field: "embedding".to_string(),
            dim: Some(3),
        };

        let doc = json!({ "content": "hello" });
        assert!(decorator.send(&doc, Some("m1")).await.is_err());
        // The inner sink must not be reached on a dim mismatch.
        assert!(inner.docs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dimension_match_passes() {
        let embedder = Arc::new(FakeEmbed {
            out: vec![0.1, 0.2, 0.3],
            recorded: Mutex::new(Vec::new()),
        });
        let inner = Arc::new(RecordingSink {
            docs: Mutex::new(Vec::new()),
        });
        let decorator = EmbeddingSink {
            inner: inner.clone(),
            embedder,
            template: "{{content}}".to_string(),
            output_field: "embedding".to_string(),
            dim: Some(3),
        };

        decorator
            .send(&json!({ "content": "hello" }), Some("m1"))
            .await
            .unwrap();
        assert_eq!(inner.docs.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn empty_rendered_text_fails_fast() {
        let embedder = Arc::new(FakeEmbed {
            out: vec![0.1],
            recorded: Mutex::new(Vec::new()),
        });
        let inner = Arc::new(RecordingSink {
            docs: Mutex::new(Vec::new()),
        });
        let decorator = EmbeddingSink {
            inner: inner.clone(),
            embedder,
            template: "{{content}}".to_string(),
            output_field: "embedding".to_string(),
            dim: None,
        };

        let doc = json!({ "name": "no content field" });
        let err = decorator.send(&doc, Some("m1")).await.unwrap_err();
        assert!(err.to_string().contains("empty text"), "{err}");
        assert!(err.to_string().contains("{{content}}"), "{err}");
        assert!(inner.docs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn whitespace_only_text_fails_fast() {
        let embedder = Arc::new(FakeEmbed {
            out: vec![0.1],
            recorded: Mutex::new(Vec::new()),
        });
        let inner = Arc::new(RecordingSink {
            docs: Mutex::new(Vec::new()),
        });
        let decorator = EmbeddingSink {
            inner: inner.clone(),
            embedder,
            template: "{{content}}".to_string(),
            output_field: "embedding".to_string(),
            dim: None,
        };

        let doc = json!({ "content": "  \n\t " });
        assert!(decorator.send(&doc, Some("m1")).await.is_err());
        assert!(inner.docs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn fanout_delivers_to_every_sink() {
        let a = Arc::new(RecordingSink {
            docs: Mutex::new(Vec::new()),
        });
        let b = Arc::new(RecordingSink {
            docs: Mutex::new(Vec::new()),
        });
        let fanout = FanoutConsumeSink {
            sinks: vec![a.clone(), b.clone()],
        };
        fanout.send(&json!({ "x": 1 }), None).await.unwrap();
        assert_eq!(a.docs.lock().unwrap().len(), 1);
        assert_eq!(b.docs.lock().unwrap().len(), 1);
    }
}

#[cfg(test)]
mod postgres_vector_sink_tests {
    use super::PostgresVectorConsumeSink;
    use serde_json::json;

    #[test]
    fn doc_id_explicit_field_wins() {
        let doc = json!({ "id": "doc-1", "content": "x" });
        assert_eq!(
            PostgresVectorConsumeSink::doc_id(Some("id"), &doc, Some("m1")),
            Some("doc-1".to_string())
        );
    }

    #[test]
    fn doc_id_falls_back_to_msg_id() {
        let doc = json!({ "content": "x" });
        assert_eq!(
            PostgresVectorConsumeSink::doc_id(Some("id"), &doc, Some("m1")),
            Some("m1".to_string())
        );
        assert_eq!(
            PostgresVectorConsumeSink::doc_id(None, &doc, Some("m1")),
            Some("m1".to_string())
        );
    }

    #[test]
    fn doc_id_none_without_key() {
        let doc = json!({ "content": "x" });
        assert_eq!(PostgresVectorConsumeSink::doc_id(None, &doc, None), None);
    }

    #[test]
    fn vector_literal_rendering() {
        assert_eq!(
            PostgresVectorConsumeSink::vector_literal(&[0.1, 0.2, 1.0]),
            "[0.1,0.2,1]"
        );
    }

    #[test]
    fn upsert_sql_binds_vector_as_text_param() {
        // The vector must be bound as `$2::text::vector`; declaring the param
        // as `vector` makes tokio-postgres reject the String binding with a
        // client-side serialization error.
        let sql = PostgresVectorConsumeSink::upsert_sql("chunk_embeddings");
        assert!(sql.contains("VALUES ($1, $2::text::vector)"));
        assert!(sql.contains("ON CONFLICT (id) DO UPDATE"));
    }

    #[test]
    fn upsert_sql_quotes_unsafe_table_names() {
        let sql = PostgresVectorConsumeSink::upsert_sql("my-tables");
        assert!(
            sql.starts_with("INSERT INTO \"my-tables\" (id, embedding)"),
            "{sql}"
        );

        let plain = PostgresVectorConsumeSink::upsert_sql("embeddings");
        assert!(
            plain.starts_with("INSERT INTO embeddings (id, embedding)"),
            "{plain}"
        );
    }

    #[test]
    fn upsert_sql_neutralizes_identifier_injection() {
        // The whole malicious suffix lands inside a single quoted identifier,
        // so nothing can break out into a second statement.
        let sql =
            PostgresVectorConsumeSink::upsert_sql("chunk_embeddings\"; DROP TABLE documents;--");
        assert!(
            sql.contains("\"chunk_embeddings\"\"; DROP TABLE documents;--\""),
            "{sql}"
        );
    }
}

#[cfg(test)]
mod graphql_mutate_sink_tests {
    use super::{GraphqlMutateConsumeSink, SqlParam};
    use crate::utils::config::MutationConfig;
    use serde_json::{json, Value};
    use std::collections::HashMap;

    fn mutations() -> HashMap<String, MutationConfig> {
        let mut m = HashMap::new();
        m.insert(
            "upsert_material".to_string(),
            MutationConfig {
                target_url: None,
                sql: Some("INSERT INTO t VALUES ($1, $2)".to_string()),
                statements: None,
                params: vec!["mat_no".to_string(), "name".to_string()],
            },
        );
        m.insert(
            "pinned_target".to_string(),
            MutationConfig {
                target_url: Some("postgres://dbb".to_string()),
                sql: Some("SELECT 1".to_string()),
                statements: None,
                params: vec![],
            },
        );
        m.insert(
            "sync_full".to_string(),
            MutationConfig {
                target_url: None,
                sql: None,
                statements: Some(vec![
                    "INSERT INTO t VALUES ($1, $2)".to_string(),
                    "DELETE FROM s WHERE mat_no = $1".to_string(),
                ]),
                params: vec!["mat_no".to_string(), "name".to_string()],
            },
        );
        m
    }

    fn bind(doc: Value, fields: &[&str]) -> anyhow::Result<Vec<SqlParam>> {
        let owned: Vec<String> = fields.iter().map(|f| f.to_string()).collect();
        GraphqlMutateConsumeSink::bind_params(&doc, &owned, "upsert_material")
    }

    #[test]
    fn bind_params_preserves_positional_order() {
        let doc = json!({ "name": "steel", "mat_no": "M001" });
        let params = bind(doc, &["mat_no", "name"]).unwrap();
        assert!(matches!(&params[0], SqlParam::Text(s) if s == "M001"));
        assert!(matches!(&params[1], SqlParam::Text(s) if s == "steel"));
    }

    #[test]
    fn bind_params_missing_field_names_mutation_and_field() {
        let err = bind(json!({ "mat_no": "M001" }), &["mat_no", "description"])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(
                "mutation 'upsert_material' missing field 'description' in composed document"
            ),
            "{err}"
        );
    }

    #[test]
    fn bind_params_rejects_non_object_document() {
        assert!(bind(Value::Array(vec![]), &["a"]).is_err());
    }

    #[test]
    fn bind_params_maps_scalar_types() {
        let params = bind(
            json!({ "s": "x", "i": 7, "f": 0.5, "b": true, "n": null }),
            &["s", "i", "f", "b", "n"],
        )
        .unwrap();
        assert!(matches!(params[0], SqlParam::Text(ref s) if s == "x"));
        assert!(matches!(params[1], SqlParam::Int(7)));
        assert!(matches!(params[2], SqlParam::Real(f) if f == 0.5));
        assert!(matches!(params[3], SqlParam::Bool(true)));
        assert!(matches!(params[4], SqlParam::Null));
    }

    #[test]
    fn bind_params_serializes_nested_values_as_json_text() {
        let params = bind(
            json!({ "sizes": ["S", "M"], "meta": { "color": "red" } }),
            &["sizes", "meta"],
        )
        .unwrap();
        assert!(
            matches!(&params[0], SqlParam::Text(t) if t == r#"["S","M"]"#),
            "{:?}",
            params[0]
        );
        assert!(
            matches!(&params[1], SqlParam::Text(t) if t == r#"{"color":"red"}"#),
            "{:?}",
            params[1]
        );
    }

    #[test]
    fn resolve_requires_a_name() {
        let err = GraphqlMutateConsumeSink::resolve(None, &mutations())
            .unwrap_err()
            .to_string();
        assert!(err.contains("--mutation"), "{err}");
    }

    #[test]
    fn resolve_unknown_name_points_at_config_table() {
        let err = GraphqlMutateConsumeSink::resolve(Some("nope"), &mutations())
            .unwrap_err()
            .to_string();
        assert!(err.contains("[mutations.nope]"), "{err}");
    }

    #[test]
    fn resolve_returns_known_mutation() {
        let m = mutations();
        let (name, mcfg) = GraphqlMutateConsumeSink::resolve(Some("upsert_material"), &m).unwrap();
        assert_eq!(name, "upsert_material");
        assert_eq!(mcfg.params.len(), 2);
    }

    #[test]
    fn target_url_prefers_the_mutations_own() {
        let mcfg = &mutations()["pinned_target"];
        let url = GraphqlMutateConsumeSink::target_url(mcfg, Some("postgres://override")).unwrap();
        assert_eq!(url, "postgres://dbb");
    }

    #[test]
    fn target_url_falls_back_to_override() {
        let mcfg = &mutations()["upsert_material"];
        let url = GraphqlMutateConsumeSink::target_url(mcfg, Some("postgres://flag")).unwrap();
        assert_eq!(url, "postgres://flag");
    }

    #[test]
    fn target_url_without_any_fails_fast() {
        let mcfg = &mutations()["upsert_material"];
        assert!(GraphqlMutateConsumeSink::target_url(mcfg, None).is_err());
    }

    #[test]
    fn resolve_rejects_invalid_mutation_config() {
        let mut m = mutations();
        // sql XOR statements: both set is invalid.
        m.insert(
            "both".to_string(),
            MutationConfig {
                target_url: None,
                sql: Some("SELECT 1".to_string()),
                statements: Some(vec!["SELECT 2".to_string()]),
                params: vec![],
            },
        );
        let err = GraphqlMutateConsumeSink::resolve(Some("both"), &m)
            .unwrap_err()
            .to_string();
        // The outer context wraps validate()'s message; match either.
        assert!(
            err.contains("either 'sql' or 'statements'") || err.contains("config is invalid"),
            "{err}"
        );
    }

    #[test]
    fn max_param_finds_highest_reference() {
        assert_eq!(GraphqlMutateConsumeSink::max_param("SELECT 1"), 0);
        assert_eq!(
            GraphqlMutateConsumeSink::max_param("DELETE WHERE a = $1"),
            1
        );
        assert_eq!(
            GraphqlMutateConsumeSink::max_param("INSERT VALUES ($2, $10, $3)"),
            10
        );
    }

    #[test]
    fn max_param_ignores_string_literals() {
        // The '$9' inside the literal must not inflate the count; the '' escape
        // keeps the literal open through it.
        let sql = "INSERT VALUES ($1, 'cost $9', 'it''s $7') RETURNING $3";
        assert_eq!(GraphqlMutateConsumeSink::max_param(sql), 3);
    }

    #[test]
    fn max_param_zero_pads_are_real_references() {
        // '$01' parses as parameter 1, not as noise.
        assert_eq!(GraphqlMutateConsumeSink::max_param("VALUES ($01)"), 1);
    }
}
