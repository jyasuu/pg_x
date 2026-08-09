#!/usr/bin/env bash
# Test pgx consume embedding stage end-to-end.
# Prerequisites: Postgres (pgvector) + Elasticsearch + RabbitMQ reachable,
# cargo build --release.
# Tests:
#   1. A published contract message is composed, embedded, and written to BOTH
#      the Elasticsearch `documents` index (dense_vector embedding) and the
#      Postgres `chunk_embeddings` table.
#   2. Re-publishing the same message (--idempotent) leaves exactly one ES
#      document and one chunk_embeddings row.

set -euo pipefail

PGURL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/postgres}"
PGX="${PGX_BINARY:-./target/release/pgx}"
AMQP_URL="${AMQP_URL:-amqp://guest:guest@localhost:5672/%2F}"
ES_URL="${ES_URL:-http://localhost:9200}"
# Use a real embedding API (e.g. Ollama) when set; otherwise a mock responder.
EMBED_URL="${RAG_MCP_OLLAMA_URL:-}"
EMBED_MODEL="${EMBED_MODEL:-bge-m3}"
EMBED_DIM=1024

PAYLOAD='{"meta":{"event_type":"MaterialFull","schema_version":"1"},"data":{"mat_no":"M001"}}'
MESSAGE_ID="embed-msg"
DOC_ID="$MESSAGE_ID"

cleanup() {
  local pid=$1
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# Publish `$PAYLOAD` to the `pgx` exchange under `pgx.embed` with a stable
# native AMQP message_id so idempotent mode dedupes redeliveries.
publish_once() {
  local body
  body=$(python3 - "$PAYLOAD" "$MESSAGE_ID" <<'PY'
import json, sys
print(json.dumps({
    "properties": {"message_id": sys.argv[2]},
    "routing_key": "pgx.embed",
    "payload": sys.argv[1],
    "payload_encoding": "string",
}))
PY
)
  curl -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F/pgx/publish \
    -H "content-type: application/json" \
    -d "$body" 2>/dev/null \
    | python3 -c "import sys; d=__import__('json').load(sys.stdin); assert d.get('routed'), f'publish failed: {d}'"
}

# Wait for Elasticsearch to be ready.
for i in $(seq 1 20); do
  if curl -s -m 2 "$ES_URL" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "==> consume-embedding: setting up schema directory"
mkdir -p ~/.pgx/schema ~/.pgx/queries
cp -r examples/graphql/pgx/schema/* ~/.pgx/schema/
cp -r examples/graphql/pgx/queries/* ~/.pgx/queries/
[ -f ~/.pgx/config.toml ] || cp examples/graphql/pgx/config.toml ~/.pgx/config.toml

echo "==> consume-embedding: seeding documents + chunk_embeddings schema"
psql "$PGURL" -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null
psql "$PGURL" <<SQL >/dev/null
CREATE TABLE IF NOT EXISTS documents (
    id            TEXT PRIMARY KEY,
    source        TEXT NOT NULL,
    language      TEXT,
    content       TEXT NOT NULL,
    metadata      JSONB NOT NULL DEFAULT '{}'::jsonb,
    search_vector tsvector GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS chunk_embeddings (
    id        TEXT PRIMARY KEY REFERENCES documents(id) ON DELETE CASCADE,
    embedding vector(1024) NOT NULL
);
DELETE FROM chunk_embeddings WHERE id = '$DOC_ID';
DELETE FROM documents WHERE id = '$DOC_ID';
INSERT INTO documents (id, source, language, content) VALUES
    ('$DOC_ID', 'test', 'en', 'Premium Cotton Canvas');
SQL

echo "==> consume-embedding: recreating the documents index"
curl -s -X DELETE "$ES_URL/documents" >/dev/null 2>&1 || true
# The rag mapping is `ik_max_word` content + dense_vector(1024) cosine; fall
# back to a standard analyzer when the analysis-ik plugin is not installed.
CREATE=$(curl -s -X PUT "$ES_URL/documents" -H 'Content-Type: application/json' -d '{
  "settings": { "number_of_shards": 1, "number_of_replicas": 0,
    "analysis": { "analyzer": { "ik": { "type": "custom", "tokenizer": "ik_max_word" } } } },
  "mappings": { "properties": {
    "source": { "type": "keyword" },
    "content": { "type": "text", "analyzer": "ik_max_word" },
    "embedding": { "type": "dense_vector", "dims": 1024, "index": true, "similarity": "cosine" }
  } }
}')
if echo "$CREATE" | grep -q '"error"'; then
  curl -s -X PUT "$ES_URL/documents" -H 'Content-Type: application/json' -d '{
    "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
    "mappings": { "properties": {
      "source": { "type": "keyword" },
      "content": { "type": "text", "analyzer": "standard" },
      "embedding": { "type": "dense_vector", "dims": 1024, "index": true, "similarity": "cosine" }
    } }
  }' >/dev/null
fi

echo "==> consume-embedding: declaring RabbitMQ exchange 'pgx'"
curl -u guest:guest -X PUT http://localhost:15672/api/exchanges/%2F/pgx \
  -H "content-type: application/json" \
  -d '{"type":"topic","durable":true}' 2>/dev/null || true

# ── Embedding endpoint: mock by default, real API when RAG_MCP_OLLAMA_URL ───
if [ -n "$EMBED_URL" ]; then
  echo "==> consume-embedding: using real embedding API at $EMBED_URL"
  EMBED_PID=
else
  echo "==> consume-embedding: starting mock /api/embed responder"
  python3 - <<'PY' &
import http.server, json
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")
        text = body.get("input", "")
        vec = [ (i + 1) * 0.001 + len(text) for i in range(1024) ]
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

echo "==> consume-embedding: starting pgx consume (elasticsearch + postgres-vector, idempotent)"
$PGX -U "$PGURL" consume \
  --source rabbitmq \
  --amqp-url "$AMQP_URL" \
  --queue pgx-embedding \
  --exchange pgx \
  --routing-key pgx.embed \
  --sink elasticsearch \
  --sink postgres-vector \
  --es-url "$ES_URL" \
  --index documents \
  --embed-url "$EMBED_URL" \
  --embed-model "$EMBED_MODEL" \
  --embed-template "{name}" \
  --embed-dim "$EMBED_DIM" \
  --query-mode contract \
  --idempotent > /tmp/pgx_consume_embed.log 2>&1 &
CONSUME_PID=$!
sleep 3

echo "==> consume-embedding: publishing the contract message (first time)"
publish_once

# The ES bulk buffer flushes on a 5s ticker.
sleep 8

echo "==> consume-embedding: verifying exactly one ES document with an embedding"
ES_COUNT=$(curl -s "$ES_URL/documents/_count" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])")
if [ "$ES_COUNT" != "1" ]; then
  cleanup $CONSUME_PID
  [ -n "${EMBED_PID:-}" ] && cleanup "$EMBED_PID"
  echo "==> consume-embedding: FAIL — expected 1 ES document, got $ES_COUNT"
  cat /tmp/pgx_consume_embed.log
  exit 1
fi
curl -s "$ES_URL/documents/_doc/$DOC_ID" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
src = d['_source']
emb = src.get('embedding')
assert isinstance(emb, list) and len(emb) == 1024, f'embedding missing or wrong dim: {src.get(\"embedding\")}'
assert src.get('name') == 'Premium Cotton Canvas', src.get('name')
print('==> consume-embedding: ES document carries a 1024-dim embedding')
"

echo "==> consume-embedding: verifying exactly one chunk_embeddings row"
PG_COUNT=$(psql -At "$PGURL" -c "SELECT count(*) FROM chunk_embeddings WHERE id = '$DOC_ID';")
if [ "$PG_COUNT" != "1" ]; then
  cleanup $CONSUME_PID
  [ -n "${EMBED_PID:-}" ] && cleanup "$EMBED_PID"
  echo "==> consume-embedding: FAIL — expected 1 chunk_embeddings row, got $PG_COUNT"
  cat /tmp/pgx_consume_embed.log
  exit 1
fi
PG_DIM=$(psql -At "$PGURL" -c "SELECT vector_dims(embedding) FROM chunk_embeddings WHERE id = '$DOC_ID';")
if [ "$PG_DIM" != "$EMBED_DIM" ]; then
  cleanup $CONSUME_PID
  [ -n "${EMBED_PID:-}" ] && cleanup "$EMBED_PID"
  echo "==> consume-embedding: FAIL — expected embedding dim $EMBED_DIM, got $PG_DIM"
  cat /tmp/pgx_consume_embed.log
  exit 1
fi
echo "==> consume-embedding: chunk_embeddings row has a ${PG_DIM}-dim vector"

# ── Idempotent re-publish: still exactly one of each ────────────────────────
echo "==> consume-embedding: re-publishing the same message"
publish_once
sleep 8

ES_COUNT2=$(curl -s "$ES_URL/documents/_count" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])")
PG_COUNT2=$(psql -At "$PGURL" -c "SELECT count(*) FROM chunk_embeddings WHERE id = '$DOC_ID';")
if [ "$ES_COUNT2" != "1" ] || [ "$PG_COUNT2" != "1" ]; then
  cleanup $CONSUME_PID
  [ -n "${EMBED_PID:-}" ] && cleanup "$EMBED_PID"
  echo "==> consume-embedding: FAIL — idempotent re-publish left duplicates (es=$ES_COUNT2 pg=$PG_COUNT2)"
  cat /tmp/pgx_consume_embed.log
  exit 1
fi
echo "==> consume-embedding: idempotent re-publish left exactly one of each"

echo "==> consume-embedding: stopping"
cleanup $CONSUME_PID
[ -n "${EMBED_PID:-}" ] && cleanup "$EMBED_PID"
rm -f /tmp/pgx_consume_embed.log

echo "==> consume-embedding: PASS"
