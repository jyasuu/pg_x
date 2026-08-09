# 10 — Make the embedding e2e script poll instead of fixed sleeps

Type: task
Status: ready-for-agent

**What to build:** `scripts/test_consume_embedding.sh` relies on a hardcoded
`sleep 8` to let the ES `documents` index flush before asserting. On slow CI
this is flaky, and on fast machines it wastes time. The script should poll the
ES doc and the pgvector row for the expected shape up to a timeout instead of
sleeping a fixed duration.

**Blocked by:** None — can start immediately.

## Acceptance criteria

- [ ] Replace the fixed ES flush sleep with a poll loop (e.g. up to 30s) that
      retries the ES query until the expected document (1024-dim embedding,
      `name: "Premium Cotton Canvas"`) appears or the timeout fails the test.
- [ ] The pgvector assertion polls for the `chunk_embeddings` row with
      `vector_dims(embedding) = 1024` the same way.
- [ ] The idempotent re-publish assertion still passes (exactly one doc and one
      row after re-publishing).
- [ ] Script stays green in both real-Ollama and mock-embedder modes; no
      `sleep` longer than ~1s remains in the script.
