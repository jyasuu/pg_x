# 07 — Startup guard for multi-sink fanout without stable sink keys

Type: task
Status: ready-for-agent

**What to build:** When the consumer fans out to more than one sink without an
idempotent configuration, retries can duplicate records in the non-idempotent
sinks. In particular `elasticsearch` without `--idempotent` and without an
explicit `--id-field` lets ES auto-generate `_id`, so a sink failure later in
the fan-out that triggers a requeue produces duplicate documents on the
retried publish. Operators need a startup warning (or error) that tells them
to pass `--idempotent` (or an explicit stable key) whenever multiple sinks are
configured.

**Blocked by:** None — can start immediately.

## Acceptance criteria

- [ ] At startup, when more than one sink is resolved (`build_sinks` fan-out)
      and neither `--idempotent` nor an explicit sink key (`--id-field`) is
      set, the consumer logs a clear warning naming the affected sinks and the
      remediation (`--idempotent` / `--id-field`).
- [ ] Single-sink runs and fan-out runs with `--idempotent` or `--id-field`
      produce no warning.
- [ ] Unit test asserts the warning fires exactly in the flagged
      configurations.
- [ ] Wording lives in one place so the ES and future sinks stay consistent.
