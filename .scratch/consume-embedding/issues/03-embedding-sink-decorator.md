# 03 — EmbeddingSink decorator + fan-out

Type: task
Status: ready-for-agent

## Problem

The embedding stage must wrap the delivery sink as a `ConsumeSink`
decorator: derive text via template, embed, attach the vector under
`output_field` on a cloned doc, forward to the inner sink. An optional
`--embed-dim` check fails the stage on a dimension mismatch. Multiple
targets fan out (spec user stories 6, 10, 17, and the decorator seam in
Implementation Decisions).

## Scope

- `EmbeddingSink { inner: Arc<dyn ConsumeSink>, embedder: Arc<dyn Embed>,
  template: String, output_field: String, dim: Option<usize> }` implementing
  `ConsumeSink`:
  - `text = interpolate(&template, doc)`
  - `vec = embedder.embed(&text)`
  - if `dim` set and `vec.len() != dim` → error (sink-stage failure)
  - `enriched = doc.clone()` with `enriched[output_field] = vec`
  - `inner.send(&enriched, msg_id)`
- `FanoutConsumeSink { sinks: Vec<Arc<dyn ConsumeSink>> }` sending to each
  sink in order; first error short-circuits.
- No changes to the session loop, dedupe, or settle protocol.

## Verification

- Unit tests with a fake `Embed` (prior art: `error_policy_seam_tests` in
  `consume_session.rs`):
  - inner sink receives doc with vector under `output_field`
  - template selects the embedded text
  - wrong-dim embedder errors at send time
  - fan-out delivers to every sink
- `cargo test embedding_sink`.
