# 06 — Fail fast on empty embeddable text

Type: task
Status: ready-for-agent

**What to build:** A composed document whose template renders empty or
whitespace-only text must fail the sink stage with a clear error instead of
silently entering an infinite requeue loop. Today the default template is
`{{content}}`; when the document has no such field the interpolated text is
empty, and a real embedding API like Ollama responds HTTP 200 with
`embeddings: []`, which surfaces as a confusing "embeddings is empty" error and
is requeued forever under the lenient policy.

**Blocked by:** None — can start immediately.

## Acceptance criteria

- [ ] `EmbeddingSink::send` rejects empty/whitespace-only interpolated text
      with an error that names the rendered text length and the template
      (e.g. "template rendered empty text; check --embed-template / content field").
- [ ] A non-empty template still embeds and forwards the enriched document
      unchanged (existing `EmbeddingSink` unit tests keep passing).
- [ ] Unit test covers empty text, whitespace-only text, and the non-empty path.
- [ ] No change to the error-policy: the failure is still a sink-stage failure
      (lenient requeues, strict aborts), so retry semantics stay consistent.
