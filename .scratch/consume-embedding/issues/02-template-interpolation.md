# 02 — Template interpolation for embeddable text

Type: task
Status: ready-for-agent

## Problem

The embeddable text must be derivable from the composed document via a
template: `{{field}}` and dotted `{{a.b}}` placeholders interpolated from the
document; missing paths render empty. Default template is `{{content}}`
(spec user stories 3, 15).

## Scope

- Pure function `interpolate(template: &str, doc: &Value) -> Result<String>` in
  `src/embed/`, rendered via the `minijinja` template engine (Jinja2 syntax):
  - `{{field}}` → the top-level string/rendered value; missing → empty.
  - `{{a.b}}` → nested object lookup; missing → empty.
  - Non-placeholder text preserved verbatim.
  - A malformed template returns `Err` (fails the sink stage).
- `default_template(field: &str) -> String` returning `{{field}}`.

## Verification

- Unit tests: plain `{{content}}`, dotted path, missing path renders empty,
  mixed text, no-placeholder passthrough, single-brace passthrough, malformed
  template errors.
- `cargo test template`.
