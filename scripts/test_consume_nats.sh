#!/usr/bin/env bash
# Test pgx consume command end-to-end over NATS JetStream.
# Prerequisites: postgres + nats (docker compose up -d postgres nats),
#                cargo build --release
# Tests: NATS JetStream source → GraphQL composition → stdout sink

set -euo pipefail

PGURL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/postgres}"
PGX="${PGX_BINARY:-./target/release/pgx}"
NATS_URL="${NATS_URL:-nats://localhost:4222}"

cleanup() {
  local pid=$1
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# Unique stream/consumer per run so reruns never resume a stale cursor.
RUN_ID=$$
STREAM="pgx-events-${RUN_ID}"
CONSUMER="pgx-consume-${RUN_ID}"

# Capture output
OUTFILE=$(mktemp)
echo "Output file: $OUTFILE"

echo "==> consume-nats: setting up schema directory"
mkdir -p ~/.pgx/schema ~/.pgx/queries
cp -r examples/graphql/pgx/schema/* ~/.pgx/schema/
cp -r examples/graphql/pgx/queries/* ~/.pgx/queries/
cp examples/graphql/pgx/config.toml ~/.pgx/config.toml

echo "==> consume-nats: starting pgx consume with nats source and stdout sink"
$PGX -U "$PGURL" consume \
  --source nats \
  --nats-url "$NATS_URL" \
  --nats-stream "$STREAM" \
  --nats-consumer "$CONSUMER" \
  --nats-create-stream \
  --sink stdout \
  --query-mode contract > "$OUTFILE" 2>&1 &
CONSUME_PID=$!
sleep 3

echo "==> consume-nats: publishing ContractMessage to JetStream"
python3 - "$NATS_URL" "$STREAM" <<'PYEOF'
import json
import socket
import sys
import time

url, stream = sys.argv[1], sys.argv[2]
payload = (
    '{"meta":{"event_type":"MaterialFull","schema_version":"1"},'
    '"data":{"mat_no":"M001"}}'
).encode()

host = url.split("://", 1)[1].rsplit(":", 1)[0]
port = int(url.rsplit(":", 1)[1])

s = socket.create_connection((host, port), timeout=5)
s.settimeout(5)
f = s.makefile("rb")
info = f.readline()  # server INFO
assert info.startswith(b"INFO"), info[:60]

s.sendall(b'CONNECT {"verbose": false}\r\n')
inbox = f"_INBOX.pgx-test.{time.time_ns()}".encode()
s.sendall(b"SUB " + inbox + b" 1\r\n")

# Publish to the stream's captured subject with a reply inbox; the server
# answers with {"stream": ..., "seq": ...} once the message is persisted.
s.sendall(b"PUB " + stream.encode() + b" " + inbox + b" " +
          str(len(payload)).encode() + b"\r\n" + payload + b"\r\n")

deadline = time.time() + 10
while time.time() < deadline:
    line = f.readline()
    if not line:
        break
    if line.startswith(b"PING"):
        s.sendall(b"PONG\r\n")
        continue
    if line.startswith(b"MSG"):
        size = int(line.split()[-1])
        body = f.read(size)
        f.read(2)  # CRLF
        resp = json.loads(body)
        if "seq" in resp:
            print(f"published to {resp.get('stream')} at seq {resp['seq']}")
            sys.exit(0)
        sys.exit(f"JetStream publish rejected: {resp}")

sys.exit(
    f"No JetStream ack for stream '{stream}' (timeout) — "
    "is the NATS server running with JetStream enabled (-js)?"
)
PYEOF

sleep 3

echo "==> consume-nats: stopping"
cleanup $CONSUME_PID

# Verify the output contained expected GraphQL-composed fields
OUTPUT=$(cat "$OUTFILE")
echo "=== consume output ==="
echo "$OUTPUT"
echo "=== end output ==="

if echo "$OUTPUT" | grep -q '"mat_no": "M001"' && \
   echo "$OUTPUT" | grep -q '"sizes"' && \
   echo "$OUTPUT" | grep -q '"colorways"'; then
  rm "$OUTFILE"
  echo "==> consume-nats: PASS"
else
  rm "$OUTFILE"
  echo "==> consume-nats: FAIL — output missing expected fields"
  exit 1
fi
