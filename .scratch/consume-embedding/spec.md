# Spec: Embedding-enriched `consume` pipeline

Status: ready-for-agent

## Problem Statement

The agentic-rag-mcp server exposes retrieval over a Postgres + Elasticsearch
knowledge base. Its corpus lives in Postgres (`documents` + `chunk_embeddings`),
but every ES index document must carry a BGE-M3 `embedding` dense_vector(1024)
field for kNN/hybrid retrieval. Today that embedding is written by a separate
seed/ingestion script that calls an embedding API directly — a dual-write path:
pgx cannot move content changes into ES with their vectors, so the knowledge
base stays consistent only when that out-of-band script is re-run.

pgx already has the whole change → enrichment → ES pipeline except the vector
step: a broker **message** is composed through GraphQL into a document
(`graphql > template`), and the composed document is delivered to a **sink**
(Elasticsearch, webhook, KV). What is missing is the `embedding` stage between
composition and delivery: take the composed document, shape its text, call an
embedding API, attach the resulting vector, and only then write. And there is no
path to write the vector to Postgres pgvector (`chunk_embeddings`) at all.

## Solution

Add an optional embedding stage to the `consume` session, placed between
**composition** and **delivery** by wrapping the sink in a decorator. For each
composed document the stage:

1. derives the text to embed from the document via a template (default: the
   `content` field),
2. calls an embedding API (Ollama `/api/embed` or OpenAI-compatible
   `/v1/embeddings` — configurable), returning a float vector,
3. attaches the vector to the document under a configurable field
   (default `embedding`),
4. fans the enriched document out to the configured sink(s): Elasticsearch
   (dense_vector) and/or Postgres pgvector (`chunk_embeddings`).

Because the stage is a `ConsumeSink` wrapper, the consume session, its dedupe
lifecycle, and its settle protocol are untouched. An embedding-API failure is a
sink-stage failure and obeys the existing error policy (lenient → requeue,
strict → abort). Nothing changes when the embedding stage is not configured.

## User Stories

1. As an operator of a Postgres → Elasticsearch RAG pipeline, I want `consume`
   to attach an embedding to each composed document before writing it to ES, so
   that the index stays consistent with Postgres content without an out-of-band
   embedding script.
2. As an operator, I want the text that gets embedded to be selectable, so that
   the semantic representation matches what retrieval actually queries.
3. As an operator, I want the embeddable text to be a template over the composed
   document (e.g. `{content}` or `{content}\n{source}`), so that I can shape
   multi-field documents for better embeddings.
4. As an operator, I want the embedding API to be configurable via URL and model,
   so that I can point at my own Ollama or OpenAI-compatible endpoint.
5. As an operator, I want both Ollama (`/api/embed`) and OpenAI-compatible
   (`/v1/embeddings`) request formats supported behind one flag, so that I can
   use whatever embedding service the corpus already uses.
6. As an operator, I want the attached vector to land in the ES document as a
   `dense_vector` float array, so that the existing ES kNN/hybrid funnel works
   without schema changes.
7. As an operator, I want the same enriched document to also be written to the
   Postgres `chunk_embeddings` table, so that a Postgres-side ANN backend
   (pgvector) sees the same vectors.
8. As an operator, I want the pgvector write to be upsert-by-id, so that
   redelivered messages do not create duplicate embedding rows.
9. As an operator, I want the vector field name to be configurable, so that I
   can match a pre-existing ES mapping.
10. As an operator, I want an optional expected-dimension check, so that a model
    returning the wrong vector size fails loudly instead of producing a broken
    index.
11. As an operator running a high-throughput pipeline, I want the embedding
    stage to never block the session loop's settle protocol, so that a slow
    embedding API causes requeues, not silent drops or hangs.
12. As an operator who already runs `consume` without embeddings, I want
    behavior to be unchanged when no embedding options are set, so that
    upgrading pgx does not silently add an API dependency.
13. As an operator, I want the whole stage configured from `~/.pgx/config.toml`
    as well as the CLI, so that a pipeline can be declared in the connection
    profile.
14. As an operator of the agentic-rag-mcp stack, I want the backfill path
    (`export -m rabbitmq --event-type <Name>` → `consume`) to pick up the
    embedding stage, so that a full re-index embeds as it goes.
15. As a developer, I want the text-template derivation to be a pure function,
    so that the interpolation logic is unit-tested without an API.
16. As a developer, I want the Ollama and OpenAI request builders and response
    parsers to be pure, so that both wire formats are verified without a live
    server.
17. As a developer, I want the embedding stage to be tested through the existing
    `ConsumeSink` seam with a fake embedder, so that the vector attachment is
    verified without network calls.
18. As an operator, I want an end-to-end scripted test that drives message →
    compose → embed → ES/pgvector and asserts one document with one vector, so
    that the user-visible guarantee is verified in CI.
19. As an operator using idempotent mode, I want an embedding failure to requeue
    in lenient mode and abort in strict mode, consistent with every other
    delivery failure, so that retry semantics stay predictable.
20. As an operator, I want a clear startup error when the embedding stage is
    configured but incomplete (no URL), so that misconfiguration fails fast
    instead of surfacing on the first message.

## Implementation Decisions

- **Seam: a `ConsumeSink` decorator.** The stage is `EmbeddingSink { inner,
  embedder, template, output_field }` implementing `ConsumeSink::send(doc,
  msg_id)`: extract text via template, `embed(text)`, attach the vector to a
  cloned doc under `output_field`, forward to `inner`. Multiple targets are a
  fan-out of sinks (reusing the existing delivery-handle pattern); when no
  embedding options are configured, `build_sink` returns the plain sink and
  nothing changes. The session loop, dedupe lifecycle, and settlement are
  untouched — the stage sits entirely inside `ConsumeSink::send`, so an embed
  failure is a sink-stage failure with the existing error policy.
- **`Embed` trait and clients.** A small `Embed` trait (`async fn embed(&self,
  text: &str) -> Result<Vec<f32>>`) with one HTTP client implementing both wire
  formats, selected by `--embed-api ollama|openai` (default `ollama`, matching
  the agentic-rag-mcp seed):
  - Ollama: `POST {base}/api/embed`, body `{"model": M, "input": text}`,
    response `embeddings[0]`.
  - OpenAI-compatible: `POST {base}/v1/embeddings`, body
    `{"model": M, "input": text}`, response `data[0].embedding`.
  Request building and response parsing are pure functions (no I/O), so both
  formats are unit-tested without a server.
- **Template.** The embeddable text defaults to the composed document's
  `content` field. `--embed-template` (config `embed.template`) interpolates
  `{field}` and dotted `{a.b}` placeholders from the document; missing paths
  render empty. This is a pure function over the document value.
- **Elasticsearch target.** Reuses `ElasticsearchConsumeSink` unchanged: the
  bulk `index` action already carries the float array into the mapped
  `dense_vector` field. `_id` derivation is unchanged (`--id-field` wins, then
  message id under idempotence, then ES-generated).
- **Postgres pgvector target.** A new `PostgresVectorConsumeSink` upserts
  `chunk_embeddings(id, embedding)` (`ON CONFLICT (id) DO UPDATE`), where `id`
  is the `--id-field` value from the document, else the message id (idempotent
  mode), else a per-document error. The vector is rendered as a pgvector
  literal (`'[0.1,0.2,…]'::vector`) from the float array. The sink uses the
  consume session's existing `QueryConn` pool — no new connection. Table name
  configurable (default `chunk_embeddings`).
- **Multi-sink.** The `--sink` flag becomes repeatable (`elasticsearch` and/or
  `postgres-vector`); with more than one, the enriched document fans out. The
  primary downstream subcommand-style single sink is preserved for
  compatibility.
- **Dimension check.** Optional `--embed-dim` (config `embed.dim`): when set, a
  returned vector of a different length fails the sink stage (logged; lenient
  drops, strict aborts). This guards the fixed ES dense_vector(1024) mapping
  without adding an ES-calls dependency.
- **Idempotence interaction.** The dedupe cache keys on the message id before
  composition, unchanged. An embed or write failure happens after composition,
  so lenient mode requeues and strict mode aborts, consistent with the existing
  sink-stage table; the message id is recorded only after a successful send.
- **CLI and config surface.** New `consume` args: `--embed-url`,
  `--embed-api`, `--embed-model`, `--embed-field`, `--embed-template`,
  `--embed-output-field`, `--embed-dim`, `--vector-table`, and a repeatable
  `--sink`. Matching config under `[connections.<name>.consume]`:
  `embed = { url, api, model, field, template, output_field, dim }`,
  `vector_table`, `additional_sinks`. Merged with the existing CLI-wins
  precedence in `EffectiveConsumeConfig`. Startup fails fast when `--embed-url`
  is set without `--embed-model`, or a vector sink is set without an embed URL.

## Testing Decisions

A good test for this feature exercises external behavior: a composed document
with a `content` field reaches the sink carrying an `embedding` array whose
values match the embedder's output, after the configured template; a wrong-dim
embedder produces a sink-stage failure with the expected settle action; the
pgvector target upserts one row per id. Tests assert what lands in the sink and
the database, not how the decorator is implemented.

- **Primary seam — `EmbeddingSink` unit tests** with a fake `Embedder`,
  asserting: the inner sink receives the doc with the vector under
  `output_field`; the template selects/interpolates fields; missing `--embed-url`
  fails construction with a clear error; a wrong-dim vector errors at send time.
  Prior art: `error_policy_tests` and `effective_config_tests` in
  `src/commands/consume.rs` (inline `#[cfg(test)]` modules with fake
  sinks/compose) and `src/consumer/kv.rs`'s pure-logic tests.
- **`EmbedClient` wire formats** — pure unit tests on the request builders and
  response parsers for both Ollama and OpenAI shapes (happy path, empty
  embedding, malformed body). Prior art: `src/downstream/contract.rs`
  parsing tests.
- **Template interpolation** — pure unit tests on `{field}` / dotted-path /
  missing-field cases.
- **`PostgresVectorConsumeSink`** — unit tests on the vector-literal rendering
  and the `id`-derivation fallback (explicit field wins, then message id).
- **End-to-end scripted test** — a new `scripts/test_consume_embedding.sh`
  following `test_consume_idempotent.sh`: seed Postgres (`documents` +
  `chunk_embeddings` schema from rag-mcp migrations) and Elasticsearch (the rag
  mapping: `ik_max_word` content + `dense_vector(1024)` cosine), run a mock
  `/api/embed` responder (or the real Ollama when `RAG_MCP_OLLAMA_URL` is set),
  publish a contract message, and assert exactly one ES document carrying
  `embedding` and exactly one `chunk_embeddings` row. Re-publish the same
  message and assert still one of each (idempotent).

## Out of Scope

- In-process embedding computation (no ONNX/ORT dependency in pgx); the
  embedding API is external (Ollama / OpenAI-compatible).
- Batching multiple documents into one embed call (per-message embed for v1;
  throughput tuning is a follow-up).
- WAL-delete mirroring, a `replicate` → Elasticsearch sink, and contract-shaped
  emission from `replicate` — this spec covers the enrichment stage of the
  existing `consume` pipeline, not a new WAL sink.
- `pg_cell_to_json` vector-column rendering — unnecessary here, since vectors
  come from the embedding API as float arrays, not from Postgres columns.
- Embedding caching, model download/tuning, and semantic dedupe.
- A standalone `embed` CLI command.

## Further Notes

- The ES `documents` index mapping is owned by the agentic-rag-mcp seed /
  `ensure_index` (dense_vector(1024), cosine). Operators must use a 1024-dim
  model (BGE-M3 by default) or set `--embed-dim` to catch mismatches.
- The two write targets serve both rag-mcp backend shapes: ES dense_vector for
  the current BM25/kNN/RRF funnel, and pgvector `chunk_embeddings` (rag-mcp
  migration 0002) for a Postgres ANN backend.
- "Listen to data change" entry points already exist and are unchanged: `listen`
  (NOTIFY contract messages), `export -m rabbitmq --event-type <Name>` for
  backfills, or any broker producer of ContractMessages. `consume` is the
  listening + enrichment loop this feature extends; a full re-index is
  `export … --event-type Full` then `consume --embed-url … --sink elasticsearch`.
- The decorator seam means the stage applies to every consume sink (stdout,
  webhook, KV) for free, though only ES and pgvector currently consume vectors.
