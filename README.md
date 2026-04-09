# pgx — PostgreSQL Power CLI

A feature-rich PostgreSQL CLI tool — beyond psql.

## Features

| Command   | Description |
|-----------|-------------|
| `query`   | Run SQL and display results as a table or JSON |
| `export`  | Export SQL results to Excel / CSV / JSON |
| `info`    | Show server version, databases, tables, connections |
| `listen`  | **Subscribe to NOTIFY channels and forward to downstream sinks** |

---

## Installation

```bash
# Default build (RabbitMQ + Webhook enabled)
cargo build --release

# With Kafka support (requires librdkafka)
cargo build --release --features kafka

# Minimal build (shell downstream only)
cargo build --release --no-default-features
```

The binary is placed at `target/release/pgx`.

---

## Connection

```bash
# Via URL flag
pgx -U postgres://user:pass@localhost:5432/mydb <command>

# Via environment variable
export DATABASE_URL=postgres://user:pass@localhost:5432/mydb
pgx <command>

# Via named profile in ~/.pgx/config.toml
pgx -c myprofile <command>
```

### ~/.pgx/config.toml

```toml
default = "local"

[connections.local]
url = "postgres://postgres:postgres@localhost:5432/mydb"
description = "Local dev database"

[connections.staging]
url = "postgres://user:pass@staging-host:5432/mydb"
```

---

## listen — PostgreSQL NOTIFY → Downstream

The `listen` command connects to PostgreSQL, subscribes to one or more NOTIFY channels,
and forwards every notification to a chosen downstream sink.

### Two forwarding modes

| Mode       | Description |
|------------|-------------|
| `simple`   | Pass the raw NOTIFY payload as the message body |
| `contract` | Parse the payload as a structured `ContractMessage` and use embedded routing hints |

---

### Downstream: RabbitMQ

```bash
# Simple mode — fixed exchange + routing key
pgx -U $DATABASE_URL listen \
  -C orders \
  rabbitmq \
  --amqp-url amqp://guest:guest@localhost:5672/%2F \
  --exchange events \
  --routing-key order.notify \
  --mode simple

# Contract mode — exchange/routing-key/headers driven by the payload
pgx -U $DATABASE_URL listen \
  -C orders -C inventory \
  rabbitmq \
  --amqp-url amqp://guest:guest@localhost:5672/%2F \
  --exchange events \
  --routing-key default.notify \
  --mode contract
```

**Contract payload example** (sent via `pg_notify('orders', '...')`):

```json
{
  "meta": {
    "routing": {
      "rabbitmq_exchange": "orders",
      "rabbitmq_routing_key": "order.created",
      "rabbitmq_headers": { "x-priority": "1", "x-tenant": "acme" }
    },
    "schema_version": "1",
    "event_type": "order.created"
  },
  "data": {
    "order_id": 42,
    "customer": "Alice",
    "total": 99.95
  }
}
```

In contract mode the `data` object becomes the AMQP message body, and the `routing` hints
become AMQP headers + exchange/routing-key selection. `x-event-type`, `x-pg-channel`, and
`x-schema-version` are always injected automatically.

---

### Downstream: Kafka (requires `--features kafka`)

```bash
# Simple mode
pgx -U $DATABASE_URL listen \
  -C orders \
  kafka \
  --brokers localhost:9092 \
  --topic pgx-notify \
  --mode simple

# Contract mode — topic/key/headers from payload
pgx -U $DATABASE_URL listen \
  -C orders \
  kafka \
  --brokers localhost:9092 \
  --topic pgx-notify \
  --mode contract
```

**Contract payload (Kafka)**:

```json
{
  "meta": {
    "routing": {
      "kafka_topic": "orders",
      "kafka_key": "order-42",
      "kafka_headers": { "x-source": "pg_notify" }
    },
    "event_type": "order.created"
  },
  "data": { "order_id": 42 }
}
```

---

### Downstream: Webhook

```bash
# Simple mode
pgx -U $DATABASE_URL listen \
  -C alerts \
  webhook \
  --url https://example.com/hooks/alerts \
  --header "Authorization=Bearer mytoken" \
  --mode simple

# Contract mode — URL and headers can be overridden per message
pgx -U $DATABASE_URL listen \
  -C alerts \
  webhook \
  --url https://example.com/hooks/default \
  --mode contract
```

**Contract payload (Webhook)**:

```json
{
  "meta": {
    "routing": {
      "webhook_url": "https://example.com/hooks/orders",
      "webhook_headers": { "X-Event-Type": "order.created" }
    }
  },
  "data": { "order_id": 42 }
}
```

---

### Downstream: Shell

```bash
# Simple mode — PGX_PAYLOAD holds the raw string
pgx -U $DATABASE_URL listen \
  -C deployments \
  shell \
  --command 'echo "[$PGX_CHANNEL] $PGX_PAYLOAD" >> /var/log/pg_notify.log' \
  --mode simple

# Contract mode — structured env vars + shell_env from payload
pgx -U $DATABASE_URL listen \
  -C deployments \
  shell \
  --command './scripts/handle_deploy.sh' \
  --env "SLACK_WEBHOOK=https://hooks.slack.com/..." \
  --mode contract
```

In contract mode the following environment variables are available in your script:

| Variable           | Source |
|--------------------|--------|
| `PGX_CHANNEL`      | NOTIFY channel name |
| `PGX_PID`          | Sending backend PID |
| `PGX_PAYLOAD`      | Business data JSON (the `data` field) |
| `PGX_EVENT_TYPE`   | `meta.event_type` |
| `PGX_SCHEMA_VERSION` | `meta.schema_version` |
| *custom*           | Any keys in `meta.routing.shell_env` |

---

## Architecture

```
src/
├── main.rs                     # CLI entry-point, command dispatch
├── commands/
│   ├── listen.rs               # `listen` command + CLI args
│   ├── export.rs
│   ├── query.rs
│   └── info.rs
├── downstream/
│   ├── sink.rs                 # Downstream trait (Send + Sync + async)
│   ├── contract.rs             # NotifyEvent, SimpleMessage, ContractMessage
│   ├── rabbitmq.rs             # Simple + Contract RabbitMQ sinks
│   ├── kafka.rs                # Simple + Contract Kafka sinks
│   ├── webhook.rs              # Simple + Contract HTTP webhook sinks
│   └── shell.rs                # Simple + Contract shell command sink
└── utils/
    ├── config.rs
    ├── db.rs
    └── ...
```

### Adding a new downstream

1. Create `src/downstream/mydownstream.rs` implementing the `Downstream` trait:

```rust
#[async_trait]
impl Downstream for MyDownstream {
    fn name(&self) -> &str { "my-downstream" }
    async fn send(&self, event: &NotifyEvent) -> Result<()> {
        // use event.payload / ContractMessage::try_parse(&event.payload)
        Ok(())
    }
}
```

2. Add a variant to `DownstreamCommand` in `commands/listen.rs`.
3. Wire it up in `build_downstream()`.
4. Optionally gate behind a Cargo feature.

---

## Other commands

```bash
# Run a query
pgx -U $DATABASE_URL query -q "SELECT * FROM users LIMIT 10"
pgx -U $DATABASE_URL query -q "SELECT count(*) FROM orders" --json

# Export to Excel
pgx -U $DATABASE_URL export -q "SELECT * FROM orders" -o orders.xlsx

# Export to CSV
pgx -U $DATABASE_URL export -q "SELECT * FROM orders" -m csv -o orders.csv

# Server info
pgx -U $DATABASE_URL info --version --databases --tables
```

---

## Cargo features

| Feature    | Default | Enables |
|------------|---------|---------|
| `rabbitmq` | ✅      | RabbitMQ downstream via `lapin` |
| `webhook`  | ✅      | HTTP webhook downstream via `reqwest` |
| `kafka`    | ❌      | Kafka downstream via `rdkafka` (requires librdkafka) |
| `tls`      | ❌      | TLS support for PostgreSQL connections |

---

## replicate — PostgreSQL Logical Replication (WAL streaming)

The `replicate` command connects using the PostgreSQL **replication protocol**,
subscribes to a publication via a `pgoutput` logical replication slot, and
forwards every `INSERT`, `UPDATE`, `DELETE`, and `TRUNCATE` event — decoded
from the binary WAL stream — to any downstream sink.

Unlike `listen` (which requires explicit `pg_notify()` calls), `replicate`
captures every data change automatically, with full before/after row values.

### Comparison: `listen` vs `replicate`

| | `listen` | `replicate` |
|---|---|---|
| Source | `pg_notify()` calls | WAL (any INSERT/UPDATE/DELETE) |
| Payload | Whatever app puts in NOTIFY | Full row images, before + after |
| Setup | None | `wal_level=logical` + publication |
| Durability | At-most-once | Exactly-once (replication slot) |
| Resume | No | Yes (via LSN checkpoint) |

---

### PostgreSQL prerequisites

```sql
-- In postgresql.conf:
wal_level = logical

-- Restart PostgreSQL, then create a publication:
CREATE PUBLICATION my_pub FOR TABLE orders, inventory;

-- Or replicate every table in the database:
CREATE PUBLICATION my_pub FOR ALL TABLES;

-- The connecting user must have the REPLICATION privilege:
ALTER USER myuser REPLICATION;
```

---

### Downstream: stdout (debugging)

```bash
# Pretty-print all WAL events
pgx -U $DATABASE_URL replicate \
  --slot pgx_slot \
  --publication my_pub \
  stdout --pretty

# Filter to INSERT and UPDATE on a specific table
pgx -U $DATABASE_URL replicate \
  --slot pgx_slot \
  --publication my_pub \
  --table public.orders \
  --op insert --op update \
  stdout --pretty
```

**Example output:**

```json
{
  "op": "insert",
  "rel_id": 16384,
  "schema": "public",
  "table": "orders",
  "new": {
    "id": "42",
    "customer": "Alice",
    "status": "pending",
    "total": "99.95"
  }
}
```

---

### Downstream: shell

```bash
pgx -U $DATABASE_URL replicate \
  --slot pgx_slot \
  --publication my_pub \
  shell \
  --command 'echo "[$PGX_OP] $PGX_SCHEMA.$PGX_TABLE new=$PGX_NEW"'
```

**Available environment variables:**

| Variable      | Description |
|---------------|-------------|
| `PGX_OP`      | Operation: `insert`, `update`, `delete`, `truncate`, `begin`, `commit`, `relation` |
| `PGX_SCHEMA`  | Schema name (DML events) |
| `PGX_TABLE`   | Table name  (DML events) |
| `PGX_LSN`     | WAL position (e.g. `0/1A2B3C`) |
| `PGX_XID`     | Transaction ID (BEGIN events) |
| `PGX_NEW`     | JSON of new row values (INSERT / UPDATE) |
| `PGX_OLD`     | JSON of old row values (UPDATE / DELETE) |
| `PGX_PAYLOAD` | Full event JSON |

---

### Downstream: webhook

```bash
pgx -U $DATABASE_URL replicate \
  --slot pgx_slot \
  --publication my_pub \
  --op insert --op update \
  webhook \
  --url https://example.com/hooks/wal \
  --header "Authorization=Bearer mytoken"
```

The full event JSON is POSTed as the request body with `Content-Type: application/json`.

---

### Downstream: RabbitMQ

```bash
pgx -U $DATABASE_URL replicate \
  --slot pgx_slot \
  --publication my_pub \
  rabbitmq \
  --amqp-url amqp://guest:guest@localhost:5672/%2F \
  --exchange wal-events \
  --routing-key pgx.wal
```

AMQP headers `pgx-op`, `pgx-schema`, `pgx-table`, `pgx-lsn` are injected automatically.

---

### Downstream: Kafka

```bash
pgx -U $DATABASE_URL replicate \
  --slot pgx_slot \
  --publication my_pub \
  kafka \
  --brokers localhost:9092 \
  --topic pgx-wal
```

The Kafka message key is set to the table name, making it easy to partition events by table.

---

### Slot management flags

| Flag | Description |
|------|-------------|
| `--slot <name>` | Slot name (default: `pgx_slot`). Created automatically if absent. |
| `--reset-slot` | Drop and recreate the slot before starting. Loses acknowledged progress. |
| `--temporary` | Create a temporary slot — dropped when the session ends. |
| `--start-lsn <A/BB>` | Resume from a specific WAL position. |

---

### Filtering

```bash
# Only orders table, only inserts
pgx -U $DATABASE_URL replicate \
  --slot pgx_slot --publication my_pub \
  --table public.orders \
  --op insert \
  stdout

# Include transaction boundaries and schema events
pgx -U $DATABASE_URL replicate \
  --slot pgx_slot --publication my_pub \
  --emit-txn-boundaries \
  --emit-schema \
  stdout --pretty
```

---

### Architecture: new files

```
src/
├── commands/
│   └── replicate.rs          # CLI args + run() + per-sink WalSink impls
├── replication/
│   ├── mod.rs                # module root
│   ├── event.rs              # WalEvent enum (Begin/Commit/Insert/Update/Delete/…)
│   ├── decoder.rs            # pgoutput binary → WalEvent parser
│   └── slot.rs               # ensure_slot / drop_slot / list_slots / parse_lsn
```

### `WalEvent` JSON schema

All downstream sinks receive events serialised as JSON with an `"op"` discriminant:

```jsonc
// INSERT
{ "op": "insert", "rel_id": 16384, "schema": "public", "table": "orders",
  "new": { "id": "1", "status": "pending" } }

// UPDATE
{ "op": "update", "rel_id": 16384, "schema": "public", "table": "orders",
  "old": { "id": "1", "status": "pending" },   // present when REPLICA IDENTITY FULL
  "new": { "id": "1", "status": "shipped" } }

// DELETE
{ "op": "delete", "rel_id": 16384, "schema": "public", "table": "orders",
  "old": { "id": "1", "status": "shipped" } }

// TRUNCATE
{ "op": "truncate", "rel_ids": [16384], "tables": ["public.orders"],
  "cascade": false, "restart_seqs": false }

// BEGIN
{ "op": "begin", "lsn": "0/1A2B3C", "commit_time": 759638400000000, "xid": 742 }

// COMMIT
{ "op": "commit", "lsn": "0/1A2B40", "end_lsn": "0/1A2B68", "commit_time": 759638400000000 }
```

> **Tip — get full old-row values on UPDATE/DELETE:**
> By default PostgreSQL only includes the primary key columns in the old tuple.
> To get all columns before the change, run:
> ```sql
> ALTER TABLE orders REPLICA IDENTITY FULL;
> ```
