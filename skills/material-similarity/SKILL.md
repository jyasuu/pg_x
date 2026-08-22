---
name: material-similarity
description: Find duplicate or near-duplicate material master data (ERP MDM dedup / entity resolution on materials) using embeddings and pgvector. Use this skill whenever the user wants to detect duplicate materials, clean up a material or product catalog, check whether "the same item was entered twice", find similar master-data records, generate synthetic sample material data with known duplicates for testing or demos, embed Postgres rows into pgvector via the pgx pipeline, or export a dataset of similar record pairs — even if they never say "dedup" or "entity resolution". Also covers running the packaged material-similarity SOP end-to-end (seed → embed → analyze → dataset) against any Ollama/OpenAI-compatible embedding endpoint.
---

# Material similarity — SOP for finding duplicate/near-duplicate materials via embeddings

ERP-style systems accumulate the same real-world material under different
`mat_no`s: descriptions vary in casing, word order, unit spacing, and synonyms
(`M8 BOLT SS304` vs `M8 Bolt SUS304`). This skill packages a repeatable
end-to-end loop over pgx's existing pipeline — **no new Rust code**, only
scripts + GraphQL assets + this SOP:

```text
seed_materials.py          embed_materials.sh                find_similar_materials.sh
      │                           │                                    │
 synthetic materials      export → RabbitMQ → consume        HNSW top-K cosine pairs
 + injected near-dupe  →  → GraphQL compose (MaterialFull) →  above threshold   →   JSONL dataset
 (same brand/part_no/     → toon(doc) template → bge-m3      joined back to
  spec/dims, messy text)  → postgres-vector sink             materials attributes
```

## Prerequisites

- Local stack: `docker compose up -d postgres rabbitmq elasticsearch`
  (Elasticsearch is required because the shared backfill script writes both
  sinks; see Non-goals.)
- Built binary: `cargo build --release` (`embed` is a default feature).
- Tools on PATH: `psql`, `curl`.
- An embedding API. Either:
  - **Real**: an Ollama host with `bge-m3` pulled — verify with
    `curl $RAG_MCP_OLLAMA_URL/api/tags` (it must list `bge-m3`; if it returns
    `{"models":[]}`, run `ollama pull bge-m3` on that host first). The URL is
    passed as an env var — never hardcode it: tunnel URLs are ephemeral.
  - **Mock** (default): when `RAG_MCP_OLLAMA_URL` is unset, a local mock
    `/api/embed` responder verifies pipeline wiring only — its vectors are
    fake and NOT semantically meaningful.

## Step 1 — Seed synthetic materials with injected near-duplicates

```bash
python3 skills/material-similarity/scripts/seed_materials.py
```

| Env           | Default                                            | Purpose                          |
| ------------- | -------------------------------------------------- | -------------------------------- |
| `ROWS`        | `100000`                                           | total rows incl. dup variants    |
| `DUPE_PCT`    | `10`                                               | percent of rows that are near-dupes |
| `TRUNCATE`    | `1`                                                | wipe `materials` (+ children) first |

Extends `materials` with `brand`, `part_no`, `spec`, `dims` columns and
generates deterministic data (same env → same data): industrial vocab
(bolts, o-rings, bearings, hoses…), structured identity per row, then
`DUPE_PCT%` variant rows that share the **same** brand/part_no/spec/dims but
corrupt only the description — casing, word order, `X`→` x ` spacing,
SS304↔SUS304 synonym swaps, pack-quantity noise. These variants are the
ground-truth duplicates the pipeline should surface.

## Step 2 — Embed every row into pgvector

```bash
# Real endpoint (recommended for meaningful vectors):
export RAG_MCP_OLLAMA_URL="https://<your-ollama-host>"   # e.g. a cloudflare tunnel
skills/material-similarity/scripts/embed_materials.sh

# Pipeline smoke test (mock embeddings):
skills/material-similarity/scripts/embed_materials.sh
```

Defaults: `EMBED_MODEL=bge-m3`, `EMBED_DIM=1024`,
`BACKFILL_QUERY="SELECT mat_no, name, status, brand, part_no, spec, dims FROM materials"`,
`EMBED_TEMPLATE='{{ toon(doc) }}'` — the built-in `toon()` helper serializes
the whole composed `MaterialFull` document (name/brand/part_no/spec/dims plus
nested sizes/colorways/features) into the embed text.

Under the hood this wraps `scripts/backfill_embedding.sh`, which:

1. mirrors each row into `documents` so the `chunk_embeddings.id` FK resolves,
2. declares the RabbitMQ exchange/queue,
3. runs `pgx export -q "$BACKFILL_QUERY" -m rabbitmq --event-type MaterialFull`
   to publish one contract message per row,
4. runs `pgx consume --source rabbitmq --query-mode contract --id-field mat_no
   --embed-url … --embed-model bge-m3 --embed-template '{{ toon(doc) }}'
   --embed-dim 1024 --sink elasticsearch --sink postgres-vector
   --vector-table chunk_embeddings --idempotent`.

Re-runs are idempotent: rows are keyed by `mat_no` and upserted, not appended.

**Throughput note:** embedding is one API call per message. Through a remote
tunnel expect ~100–300 ms/row — 100k rows is hours. Options: lower `ROWS` for
dev runs; or start additional `pgx consume` processes on the same queue
(RabbitMQ competing consumers share the load); or point at a LAN/local Ollama.

## Step 3 — Score pairwise similarity into a JSONL dataset

```bash
SIM_THRESHOLD=0.85 TOP_K=20 OUTPUT=./similar_materials.jsonl \
  skills/material-similarity/scripts/find_similar_materials.sh
```

Creates the HNSW index (`vector_cosine_ops`) if missing, takes each embedded
row's top-`TOP_K` nearest neighbours via `embedding <=>`, keeps symmetric
pairs with cosine similarity `1 - distance >= SIM_THRESHOLD`, joins back to
`materials`, and writes one JSON object per line:

```json
{"mat_no_a": "M0000042", "name_a": "HEX BOLT M8X30 SS304", "brand_a": "SKF", "spec_a": "SS304", "dims_a": "M8X30", "mat_no_b": "M0091231", "name_b": "hex bolt m8 x 30mm ss304", "brand_b": "SKF", "spec_b": "SS304", "dims_b": "M8X30", "similarity": 0.9731, "matched_fields": ["brand", "part_no", "spec", "dims"]}
```

`matched_fields` lists which attributes agree between the pair (`name` case-
insensitive, `brand`, `part_no`, `spec`, `dims`) — pairs matching on
structured fields while their texts differ are exactly the injected dupes.

Empty output means nothing cleared the threshold: lower `SIM_THRESHOLD`
(try `0.7`), but remember mock embeddings make thresholds meaningless.

## Calibrating the threshold

Keep `SIM_THRESHOLD` low (~0.85) — its job is candidate *generation*, not the
verdict. Measured on a real bge-m3 run (2000 rows, 15% injected dupes): all
300 planted pairs scored 0.88–1.00, but same-type non-duplicates reach 0.98,
so no similarity cutoff alone separates them. The structured
`matched_fields` check is the verdict stage — a pair matching on `part_no`
(uniquely derived from brand/type/dims/spec identity) is a duplicate:

| rule | precision | recall |
| --- | --- | --- |
| similarity >= 0.85 alone | 1.7% | 100% |
| `matched_fields` contains `part_no` | **100%** | **100%** |

Downstream consumers should therefore filter the JSONL on
`'part_no' in matched_fields` (jq: `select(.matched_fields | index("part_no"))`)
before any auto-merge action; lower-confidence buckets (`brand`+`spec`+`dims`
without `part_no`) belong in human review.

(Background: entity-resolution guidance in jyasuu/note `200.md`. Deliberately
out of scope here: deterministic fingerprint pre-pass, auto-merge workflows.)

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `/api/tags` returns `{"models":[]}` | `ollama pull bge-m3` on the embedding host |
| Tunnel URL stopped working | tunnel URLs are ephemeral — re-export `RAG_MCP_OLLAMA_URL` |
| All similarities ≈ 1.0 | you ran with the **mock** embedder; use a real model for real scores |
| FK violation on `chunk_embeddings` | let `embed_materials.sh` create/mirror `documents` — don't point `VECTOR_TABLE` at a foreign-FK table |
| Slow backfill | reduce `ROWS`, add competing consumers, or use a LAN Ollama |

## Files

- `scripts/seed_materials.py` — synthetic data + injected near-dupes
- `scripts/embed_materials.sh` — env-overridden wrapper over `scripts/backfill_embedding.sh`
- `scripts/find_similar_materials.sh` — pgvector pair scoring → JSONL dataset
- `graphql/` — schema/query/resolver assets extended with `brand`/`part_no`/`spec`/`dims`
- `evals/evals.json` — test prompts for exercising the full SOP
