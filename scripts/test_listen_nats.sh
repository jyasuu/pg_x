#!/usr/bin/env bash
# Test the pgx listen → NATS → consume loop end-to-end.
# Prerequisites: postgres + nats (docker compose up -d postgres nats),
#                cargo build --release, psql client
# Tests: LISTEN → NATS JetStream sink (listen nats, contract mode)
#        → NATS source (consume --source nats) → GraphQL composition → stdout

set -euo pipefail

PGURL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/postgres}"
PGX="${PGX_BINARY:-./target/release/pgx}"
NATS_URL="${NATS_URL:-nats://localhost:4222}"

cleanup() {
  for pid in "$@"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
}

# Unique stream/consumer per run so reruns never resume a stale cursor.
RUN_ID=$$
STREAM="pgx-events-${RUN_ID}"
CONSUMER="pgx-consume-${RUN_ID}"
SUBJECT="pgx.notify.${RUN_ID}"

CONSUME_OUT=$(mktemp)
LISTEN_OUT=$(mktemp)
trap 'cleanup $LISTEN_PID $CONSUME_PID; rm -f "$LISTEN_OUT" "$CONSUME_OUT"' EXIT

echo "==> listen-nats: setting up schema directory"
mkdir -p ~/.pgx/schema ~/.pgx/queries
cp -r examples/graphql/pgx/schema/* ~/.pgx/schema/
cp -r examples/graphql/pgx/queries/* ~/.pgx/queries/
cp examples/graphql/pgx/config.toml ~/.pgx/config.toml

echo "==> listen-nats: starting pgx listen forwarding channel 'orders' to NATS"
$PGX -U "$PGURL" listen \
  -C orders \
  nats \
  --nats-url "$NATS_URL" \
  --nats-subject "$SUBJECT" \
  --nats-stream "$STREAM" \
  --nats-create-stream \
  --mode contract > "$LISTEN_OUT" 2>&1 &
LISTEN_PID=$!
sleep 3

echo "==> listen-nats: starting pgx consume reading the stream back"
$PGX -U "$PGURL" consume \
  --source nats \
  --nats-url "$NATS_URL" \
  --nats-stream "$STREAM" \
  --nats-consumer "$CONSUMER" \
  --nats-create-stream \
  --sink stdout \
  --query-mode contract > "$CONSUME_OUT" 2>&1 &
CONSUME_PID=$!
sleep 3

echo "==> listen-nats: NOTIFY a ContractMessage on channel 'orders'"
if command -v psql > /dev/null 2>&1; then
  PSQL=(psql "$PGURL")
else
  # Fall back to the client inside the compose postgres container.
  PSQL=(docker exec pgx_postgres psql -U postgres -d postgres)
fi
"${PSQL[@]}" -c "NOTIFY orders, '{\"meta\":{\"event_type\":\"MaterialFull\",\"schema_version\":\"1\"},\"data\":{\"mat_no\":\"M001\"}}'" > /dev/null

# Wait for the event to traverse listen → JetStream → consume
FOUND=0
for _ in $(seq 1 20); do
  if grep -q '"mat_no": "M001"' "$CONSUME_OUT"; then
    FOUND=1
    break
  fi
  sleep 1
done

echo "==> listen-nats: stopping"
cleanup $LISTEN_PID $CONSUME_PID

echo "=== consume output ==="
cat "$CONSUME_OUT"
echo "=== end output ==="

if [ "$FOUND" = "1" ] && \
   grep -q '"sizes"' "$CONSUME_OUT" && \
   grep -q '"colorways"' "$CONSUME_OUT"; then
  echo "==> listen-nats: PASS"
else
  echo "==> listen-nats: FAIL — NOTIFY payload never reached the consume sink"
  exit 1
fi
