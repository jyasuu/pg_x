# 05 — CLI + config wiring and validation

Type: task
Status: ready-for-agent

## Problem

The embedding stage and multi-sink must be configured from the CLI and
`[connections.<name>.consume]` in `~/.pgx/config.toml`, with CLI-wins
precedence and fail-fast startup validation (spec user stories 1–14, 18–20).

## Scope

- `ConsumeArgs`: `--sink` becomes repeatable (`Vec<ConsumeSinkType>`);
  add `--embed-url`, `--embed-api`, `--embed-model`, `--embed-field`,
  `--embed-template`, `--embed-output-field`, `--embed-dim`,
  `--vector-table`.
- `ConsumeSinkType::PostgresVector` (value `postgres-vector`).
- Config: `ConsumeConfig.embed: Option<EmbedConfig>` (url/api/model/field/
  template/output_field/dim), `ConsumeConfig.additional_sinks: Vec<ConsumeSinkKind>`,
  `ConsumeConfig.vector_table: Option<String>`,
  `ConsumeSinkKind::PostgresVector { table: Option<String> }`.
- `EffectiveEmbedConfig` merge: CLI wins over config, defaults
  (api=ollama, field=content, template=`{content}`, output_field=embedding).
- Sink resolution: CLI `--sink` list wins; else config `sink` +
  `additional_sinks`; else stdout. `build_sink` → `build_sinks` returning a
  single sink or a fan-out, then wrapped in `EmbeddingSink` when the embed
  stage is active.
- Fail-fast validation:
  - embed configured (any `--embed-*` or `consume.embed`) but no URL → error
  - URL set but no model → error
  - `postgres-vector` sink without an active embed stage → error
- Update `effective_config_tests` constructor for the new fields and keep the
  CLI-wins precedence tests.

## Verification

- Unit tests: merge precedence, defaults, each validation error.
- `cargo build --release` succeeds; `cargo test`.
