# Spec: material-similarity agent skill

Status: ready-for-agent

Canonical proposal: https://github.com/jyasuu/pg_x/issues/18

## Summary

Package a repeatable SOP (agent skill) for finding duplicate/near-duplicate
ERP material master records via embeddings. Docs + scripts only over the
shipped `consume` embedding stage — no new Rust code.

- `skills/material-similarity/scripts/seed_materials.py` — synthetic materials
  (extended schema: brand, part_no, spec, dims) with injected near-duplicate
  clusters; default 100k rows.
- `skills/material-similarity/scripts/embed_materials.sh` — env-overridden
  wrapper over `scripts/backfill_embedding.sh`: export → RabbitMQ → consume
  (MaterialFull composition incl. new attributes) → `{{ toon(doc) }}` embed
  template → bge-m3@1024 via `RAG_MCP_OLLAMA_URL` (mock fallback) →
  postgres-vector sink into `chunk_embeddings`.
- `skills/material-similarity/scripts/find_similar_materials.sh` — HNSW top-K
  cosine pairs above threshold, joined to materials attributes, written as a
  JSONL dataset (`similar_materials.jsonl`).
- `skills/material-similarity/SKILL.md` — the SOP tying it together.
- `skills/material-similarity/graphql/` — pgx asset overlay extending the
  Material type/resolvers with the four new columns.

Decisions: model bge-m3 (1024-dim), output JSONL, remote Ollama endpoint only
via env var (tunnel URLs are ephemeral), deterministic seed data.

Out of scope (per #18): fingerprint/deterministic matching pre-pass, new sink
types or embedding-stage changes, auto-merge workflows.
