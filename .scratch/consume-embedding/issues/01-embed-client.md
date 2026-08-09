# 01 — Embed client: Ollama + OpenAI-compatible wire formats

Type: task
Status: ready-for-agent

## Problem

The embedding stage needs one HTTP client that speaks both Ollama
(`POST {base}/api/embed`, response `embeddings[0]`) and OpenAI-compatible
(`POST {base}/v1/embeddings`, response `data[0].embedding`) wire formats,
selected by `--embed-api ollama|openai`. Request building and response
parsing must be pure functions so both formats are unit-tested without a
server (spec user stories 5, 16).

## Scope

- New module `src/embed/` exposing:
  - `pub trait Embed: Send + Sync { async fn embed(&self, text: &str) -> Result<Vec<f32>>; }`
  - `EmbedApi` enum (`Ollama`, `Openai`) parsing from `"ollama"` / `"openai"`.
  - Pure builders `ollama_request(model, text) -> Value` and
    `openai_request(model, text) -> Value`.
  - Pure parsers `parse_ollama_response(&Value) -> Result<Vec<f32>>` and
    `parse_openai_response(&Value) -> Result<Vec<f32>>` (errors on missing/
    malformed embedding).
  - `EmbedClient { url, api, model }` implementing `Embed` via reqwest.
- Feature gate the HTTP client on a new `embed` feature
  (`embed = ["dep:reqwest"]`, added to `default`); the pure functions are
  testable without the feature.
- Default API is `ollama` (matches the agentic-rag-mcp seed).

## Verification

- Unit tests on builders (exact JSON shape for both APIs) and parsers
  (happy path, empty embeddings, malformed body).
- `cargo test embed`.
