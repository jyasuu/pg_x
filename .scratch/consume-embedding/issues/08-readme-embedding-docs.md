# 08 — README: document the embedding stage, postgres-vector sink, and fanout

Type: task
Status: ready-for-agent

**What to build:** Operator-facing documentation for the embedding-enriched
consume pipeline. Today the README does not mention the embedding stage, the
`postgres-vector` sink, or that `--sink` is repeatable (fan-out), so an
operator cannot configure or troubleshoot the feature from the docs alone. The
new section must also document the `chunk_embeddings → documents` FK
dependency: the content document row must exist before the sink upserts its
embedding, otherwise the FK violation requeues forever under the lenient
policy.

**Blocked by:** None — can start immediately.

## Acceptance criteria

- [ ] README "consume" section documents: `--embed-url`, `--embed-api`
      (ollama/openai), `--embed-model`, `--embed-field`, `--embed-template`,
      `--embed-output-field`, `--embed-dim`, `--vector-table`.
- [ ] Documents that `--sink` is repeatable and that ordering defines fan-out
      behavior.
- [ ] Documents the `postgres-vector` sink, its `chunk_embeddings(id,
      embedding)` shape, the FK dependency on an existing `documents` row, and
      that the vector column must be `vector(N)` matching `--embed-dim`.
- [ ] Includes one end-to-end example: `consume` from RabbitMQ → embed with
      Ollama → sink to both elasticsearch and postgres-vector.
- [ ] Includes a troubleshooting note for the "embeddings is empty" symptom
      (empty template text) pointing to the template flags.
