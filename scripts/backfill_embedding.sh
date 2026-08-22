#!/usr/bin/env bash
# Backfill embeddings for an existing Postgres table into Elasticsearch
# (dense_vector) and Postgres (pgvector chunk_embeddings).
#
# Each row of BACKFILL_QUERY is published as a ContractMessage
# (event_type=EVENT_TYPE), composed through the GraphQL query of the same name,
# embedded via EMBED_URL / EMBED_MODEL, and upserted into both sinks.
#
# Re-runs are idempotent: --id-field keys both sinks by a row column, so the
# same row overwrites instead of appending.
#
# Prerequisites: docker compose up -d, cargo build --release.
# Embedding endpoint: a real API (Ollama) when RAG_MCP_OLLAMA_URL is set,
# otherwise a local mock on :18082.

set -euo pipefail

PGURL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/postgres}"
PGX="${PGX_BINARY:-./target/release/pgx}"
AMQP_URL="${AMQP_URL:-amqp://guest:guest@localhost:5672/%2F}"
ES_URL="${ES_URL:-http://localhost:9200}"
EMBED_URL="${RAG_MCP_OLLAMA_URL:-}"
EMBED_MODEL="${EMBED_MODEL:-bge-m3}"
EMBED_DIM="${EMBED_DIM:-1024}"

BACKFILL_QUERY="${BACKFILL_QUERY:-SELECT mat_no, name, status FROM materials}"
EVENT_TYPE="${EVENT_TYPE:-MaterialFull}"
ID_FIELD="${ID_FIELD:-mat_no}"
ES_INDEX="${ES_INDEX:-documents}"
VECTOR_TABLE="${VECTOR_TABLE:-chunk_embeddings}"
EMBED_TEMPLATE="${EMBED_TEMPLATE:-{{name}}}"
ROUTING_KEY="${ROUTING_KEY:-pgx.embed}"
QUEUE="${QUEUE:-pgx-backfill}"

cleanup() {
  local pid=$1
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

ROW_COUNT=$(psql -At "$PGURL" -c "SELECT count(*) FROM (${BACKFILL_QUERY}) t;")
if [ "$ROW_COUNT" = "0" ]; then
  echo "==> backfill-embedding: BACKFILL_QUERY returned no rows — nothing to do"
  exit 1
fi

# Wait for Elasticsearch to be ready.
for i in $(seq 1 20); do
  if curl -s -m 2 "$ES_URL" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "==> backfill-embedding: setting up schema directory"
# Override the asset source to reuse this flow with different GraphQL
# schema/queries/resolvers (e.g. skills/material-similarity/graphql).
ASSETS_DIR="${PGX_ASSETS_DIR:-examples/graphql/pgx}"
mkdir -p ~/.pgx/schema ~/.pgx/queries
cp -r "$ASSETS_DIR"/schema/* ~/.pgx/schema/
cp -r "$ASSETS_DIR"/queries/* ~/.pgx/queries/
[ -f ~/.pgx/config.toml ] || cp "$ASSETS_DIR"/config.toml ~/.pgx/config.toml

echo "==> backfill-embedding: preparing vector + documents schema"
psql "$PGURL" -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null
psql "$PGURL" <<SQL >/dev/null
CREATE TABLE IF NOT EXISTS documents (
    id            TEXT PRIMARY KEY,
    source        TEXT NOT NULL,
    language      TEXT,
    content       TEXT NOT NULL,
    metadata      JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS $VECTOR_TABLE (
    id        TEXT PRIMARY KEY REFERENCES documents(id) ON DELETE CASCADE,
    embedding vector($EMBED_DIM) NOT NULL
);
SQL
# The vector sink upserts into $VECTOR_TABLE whose id is a FK to documents;
# seed the documents rows so the FK resolves before the first embedding lands.
psql "$PGURL" -c "
INSERT INTO documents (id, source, language, content)
SELECT t.$ID_FIELD::text, 'backfill', 'en', COALESCE(t.name::text, t.$ID_FIELD::text)
FROM (${BACKFILL_QUERY}) t
ON CONFLICT (id) DO NOTHING;" >/dev/null
# The ES index is recreated above; keep the vector table in sync by clearing
# the backfill set so re-runs start from the same clean slate.
psql "$PGURL" -c "
DELETE FROM $VECTOR_TABLE WHERE id IN (SELECT t.$ID_FIELD::text FROM (${BACKFILL_QUERY}) t);" >/dev/null

echo "==> backfill-embedding: recreating the $ES_INDEX index"
curl -s -X DELETE "$ES_URL/$ES_INDEX" >/dev/null 2>&1 || true
CREATE=$(curl -s -X PUT "$ES_URL/$ES_INDEX" -H 'Content-Type: application/json' -d "{
  \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0,
    \"analysis\": { \"analyzer\": { \"ik\": { \"type\": \"custom\", \"tokenizer\": \"ik_max_word\" } } } },
  \"mappings\": { \"properties\": {
    \"source\": { \"type\": \"keyword\" },
    \"content\": { \"type\": \"text\", \"analyzer\": \"ik_max_word\" },
    \"embedding\": { \"type\": \"dense_vector\", \"dims\": $EMBED_DIM, \"index\": true, \"similarity\": \"cosine\" }
  } }
}")
if echo "$CREATE" | grep -q '"error"'; then
  curl -s -X PUT "$ES_URL/$ES_INDEX" -H 'Content-Type: application/json' -d "{
    \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 },
    \"mappings\": { \"properties\": {
      \"source\": { \"type\": \"keyword\" },
      \"content\": { \"type\": \"text\", \"analyzer\": \"standard\" },
      \"embedding\": { \"type\": \"dense_vector\", \"dims\": $EMBED_DIM, \"index\": true, \"similarity\": \"cosine\" }
    } }
  }" >/dev/null
fi

echo "==> backfill-embedding: declaring RabbitMQ exchange 'pgx'"
curl -u guest:guest -X PUT http://localhost:15672/api/exchanges/%2F/pgx \
  -H "content-type: application/json" \
  -d '{"type":"topic","durable":true}' 2>/dev/null || true

# ── Embedding endpoint: mock by default, real API when RAG_MCP_OLLAMA_URL ───
if [ -n "$EMBED_URL" ]; then
  echo "==> backfill-embedding: using real embedding API at $EMBED_URL"
  EMBED_PID=
else
  echo "==> backfill-embedding: starting mock /api/embed responder"
  python3 - "$EMBED_DIM" <<'PY' &
import http.server, json, sys
dim = int(sys.argv[1])
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")
        text = body.get("input", "")
        vec = [ (i + 1) * 0.001 + len(text) for i in range(dim) ]
        resp = json.dumps({"embeddings": [vec]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(resp)
    def log_message(self, *a):
        pass
http.server.HTTPServer(("127.0.0.1", 18082), H).serve_forever()
PY
  EMBED_PID=$!
  EMBED_URL="http://127.0.0.1:18082"
  sleep 1
fi

echo "==> backfill-embedding: starting pgx consume (elasticsearch + postgres-vector, idempotent)"
$PGX -U "$PGURL" consume \
  --source rabbitmq \
  --amqp-url "$AMQP_URL" \
  --queue "$QUEUE" \
  --exchange pgx \
  --routing-key "$ROUTING_KEY" \
  --sink elasticsearch \
  --sink postgres-vector \
  --es-url "$ES_URL" \
  --index "$ES_INDEX" \
  --id-field "$ID_FIELD" \
  --embed-url "$EMBED_URL" \
  --embed-model "$EMBED_MODEL" \
  --embed-template "$EMBED_TEMPLATE" \
  --embed-dim "$EMBED_DIM" \
  --vector-table "$VECTOR_TABLE" \
  --query-mode contract \
  --idempotent > /tmp/pgx_backfill_embed.log 2>&1 &
CONSUME_PID=$!
# Wait for the consumer to declare + bind the queue before backfilling, or the
# messages are dropped (no queue bound to the exchange yet).
for _ in $(seq 1 20); do
  if curl -s -m 2 -u guest:guest "http://localhost:15672/api/queues/%2F/$QUEUE" 2>/dev/null \
    | grep -q '"name"'; then
    break
  fi
  sleep 1
done

backfill() {
  echo "==> backfill-embedding: backfilling $ROW_COUNT rows via export -> $EVENT_TYPE"
  $PGX -U "$PGURL" export \
    -q "$BACKFILL_QUERY" \
    -m rabbitmq \
    --amqp-url "$AMQP_URL" \
    --exchange pgx \
    --routing-key "$ROUTING_KEY" \
    --event-type "$EVENT_TYPE"
}

es_count() {
  curl -s -X POST "$ES_URL/$ES_INDEX/_refresh" >/dev/null
  curl -s "$ES_URL/$ES_INDEX/_count" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])"
}

pg_count() {
  psql -At "$PGURL" -c "SELECT count(*) FROM $VECTOR_TABLE WHERE id IN (SELECT t.$ID_FIELD::text FROM (${BACKFILL_QUERY}) t);" 2>/dev/null || echo 0
}

fail_verify() {
  local msg=$1
  cleanup $CONSUME_PID
  [ -n "${EMBED_PID:-}" ] && cleanup "$EMBED_PID"
  echo "==> backfill-embedding: FAIL — $msg"
  cat /tmp/pgx_backfill_embed.log
  exit 1
}

wait_for_counts() {
  for _ in $(seq 1 60); do
    if [ "$(es_count)" = "$ROW_COUNT" ] && [ "$(pg_count)" = "$ROW_COUNT" ]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

backfill
if ! wait_for_counts; then
  fail_verify "expected $ROW_COUNT ES documents and $ROW_COUNT $VECTOR_TABLE rows (got es=$(es_count) pg=$(pg_count))"
fi

# Every stored vector must be $EMBED_DIM-dimensional.
BAD_DIMS=$(psql -At "$PGURL" -c "SELECT count(*) FROM $VECTOR_TABLE WHERE id IN (SELECT t.$ID_FIELD::text FROM (${BACKFILL_QUERY}) t) AND vector_dims(embedding) <> $EMBED_DIM;")
[ "$BAD_DIMS" = "0" ] || fail_verify "$BAD_DIMS $VECTOR_TABLE rows have the wrong dimension (expected $EMBED_DIM)"
echo "==> backfill-embedding: $ROW_COUNT ES documents carry ${EMBED_DIM}-dim embeddings"
echo "==> backfill-embedding: $VECTOR_TABLE has $ROW_COUNT rows, all ${EMBED_DIM}-dim"

# ── Idempotent re-backfill: counts stay stable ───────────────────────────────
echo "==> backfill-embedding: re-running the backfill (idempotent upsert)"
backfill
if ! wait_for_counts; then
  fail_verify "idempotent re-backfill drifted (es=$(es_count) pg=$(pg_count))"
fi
EC2=$(es_count)
PGC2=$(pg_count)
if [ "$EC2" != "$ROW_COUNT" ] || [ "$PGC2" != "$ROW_COUNT" ]; then
  fail_verify "idempotent re-backfill left duplicates (es=$EC2 pg=$PGC2)"
fi
echo "==> backfill-embedding: idempotent re-backfill left exactly $ROW_COUNT of each"

echo "==> backfill-embedding: stopping"
cleanup $CONSUME_PID
[ -n "${EMBED_PID:-}" ] && cleanup "$EMBED_PID"
rm -f /tmp/pgx_backfill_embed.log

echo "==> backfill-embedding: PASS"
