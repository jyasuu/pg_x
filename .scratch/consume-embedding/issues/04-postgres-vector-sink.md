# 04 — PostgresVectorConsumeSink

Type: task
Status: ready-for-agent

## Problem

The enriched document's vector must also land in the Postgres pgvector
`chunk_embeddings(id, embedding)` table as an upsert-by-id, using the
consume session's existing `QueryConn` pool (spec user stories 7, 8).

## Scope

- `PostgresVectorConsumeSink { pool: Arc<QueryConn>, table: String,
  id_field: Option<String> }` implementing `ConsumeSink`:
  - `id` derivation (pure): explicit `id_field` string wins, then `msg_id`
    (idempotent mode), else an error for the document.
  - Vector rendered as a pgvector literal `'[0.1,0.2,…]'::vector` from the
    float array.
  - `INSERT INTO <table> (id, embedding) VALUES ($1, $2::vector)
    ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding`,
    executed on the pool.
- Add `QueryConn::execute_cached` (mirrors `query_cached` but uses
  `client.execute` for non-SELECT statements).
- Table name configurable (default `chunk_embeddings`).

## Verification

- Unit tests on the pure vector-literal rendering and `id`-derivation
  fallback (explicit field wins, then message id, then error).
- `cargo test postgres_vector`.
