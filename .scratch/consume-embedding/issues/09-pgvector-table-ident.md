# 09 — Harden pgvector upsert table identifier

Type: task
Status: ready-for-agent

**What to build:** The `postgres-vector` sink builds its upsert with the table
name interpolated verbatim from `--vector-table` / `vector_table`. A table name
containing special characters (or a malicious config) can break the statement
or inject SQL. The identifier should be quoted with PostgreSQL's `quote_ident`
when the upsert SQL is built.

**Blocked by:** None — can start immediately.

## Acceptance criteria

- [ ] `upsert_sql` (or the identifier handling around it) applies `quote_ident`
      to the table name in the generated statement.
- [ ] Unit test asserts a table name needing quoting (e.g. contains a hyphen,
      uppercase, or spaces) yields a valid quoted identifier, and a
      value containing an injection attempt (e.g. `"; DROP TABLE ...`) is
      neutralized as an identifier, not executed.
- [ ] Existing `postgres_vector_sink_tests` keep passing.
