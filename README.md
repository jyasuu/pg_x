# pgx — PostgreSQL Power CLI

A feature-rich Rust CLI for PostgreSQL that goes beyond `psql` and `pg_*` utilities.

## Features

| Feature | Status |
|---|---|
| Export SQL → Excel (.xlsx) | ✅ |
| Export SQL → CSV | ✅ |
| Export SQL → JSON | ✅ |
| Pretty terminal query viewer | ✅ |
| Named connection profiles | ✅ |
| DB / table / server info | ✅ |
| Progress spinner | ✅ |
| Auto-fit columns, freeze header, autofilter | ✅ |
| Striped rows, blue header | ✅ |

---

## Install

```bash
git clone <repo>
cd pgx
cargo build --release
# Binary at: ./target/release/pgx
sudo cp target/release/pgx /usr/local/bin/
```

---

## Quick Start

```bash
# One-shot export
pgx -U postgres://user:pass@localhost/mydb export \
    --query "SELECT 1 AS A, 2 AS B, 3 AS C" \
    --output result.xlsx

# Export with a .sql file
pgx -U postgres://... export --file report.sql --output report.xlsx

# Export to CSV
pgx -U postgres://... export -q "SELECT * FROM orders" -m csv -o orders.csv

# Export to JSON
pgx -U postgres://... export -q "SELECT * FROM users LIMIT 100" -m json -o users.json

# Pretty terminal table
pgx -U postgres://... query -q "SELECT 1 A, 2 B, 3 C"

# Server info
pgx -U postgres://... info --version --databases --tables
```

---

## Named Connection Profiles

Create `~/.pgx/config.toml`:

```toml
default = "local"

[connections.local]
url = "postgres://postgres:secret@localhost:5432/mydb"
description = "Local dev database"

[connections.prod]
url = "postgres://user:pass@prod.host:5432/proddb"
description = "Production (read-only replica)"
```

Then use:

```bash
pgx -c local export -q "SELECT * FROM users" -o users.xlsx
pgx -c prod  query  -q "SELECT count(*) FROM orders"
```

---

## Export Options

```
pgx export [OPTIONS]

Options:
  -q, --query <SQL>          SQL query string
  -f, --file  <PATH>         Path to .sql file
  -o, --output <PATH>        Output file (default: export_<timestamp>.xlsx)
  -m, --format <FORMAT>      excel | csv | json  [default: excel]
      --sheet <NAME>         Excel sheet name  [default: "Query Result"]
      --freeze-header        Freeze header row  [default: true]
      --autofit              Auto-fit column widths  [default: true]
      --stripe               Alternating row colors  [default: true]
      --limit <N>            Max rows (0 = unlimited)  [default: 0]
```

---

## Roadmap

- [ ] `pgx dump` — smarter pg_dump wrapper with compression + progress
- [ ] `pgx copy` — fast COPY IN/OUT with format options
- [ ] `pgx watch` — poll a query and diff results
- [ ] `pgx bench` — simple query benchmarking
- [ ] `pgx explain` — EXPLAIN ANALYZE with visual plan
- [ ] TLS / mTLS support (`--feature tls`)
