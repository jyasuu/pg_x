#!/usr/bin/env bash
# Score pairwise material similarity via pgvector and write a JSONL dataset of
# candidate duplicate/similar pairs.
#
# For each embedded material (chunk_embeddings row) the top-K nearest other
# materials are fetched through the HNSW index (cosine distance <=>), joined
# back to materials for names/attributes, and pairs whose cosine similarity
# (= 1 - distance) clears SIM_THRESHOLD are emitted, one JSON object per line:
#
#   {"mat_no_a": "M0000042", "name_a": "HEX BOLT M8X30 SS304", ...,
#    "similarity": 0.9731, "matched_fields": ["brand", "part_no", "dims"]}
#
# Env:
#   DATABASE_URL     postgres DSN
#   SIM_THRESHOLD    minimum cosine similarity to report (default 0.85)
#   TOP_K            nearest neighbours considered per row (default 20)
#   OUTPUT           output dataset path (default ./similar_materials.jsonl)

set -euo pipefail

PGURL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/postgres}"
SIM_THRESHOLD="${SIM_THRESHOLD:-0.85}"
TOP_K="${TOP_K:-20}"
OUTPUT="${OUTPUT:-similar_materials.jsonl}"

if ! command -v psql >/dev/null; then
  echo "find-similar-materials: psql not found" >&2; exit 1
fi

COUNT=$(psql -At "$PGURL" -c "SELECT count(*) FROM chunk_embeddings;" || echo 0)
if [ "$COUNT" = "0" ]; then
  echo "find-similar-materials: chunk_embeddings is empty — run embed_materials.sh first" >&2
  exit 1
fi
echo "==> find-similar-materials: $COUNT embedded rows"

echo "==> find-similar-materials: ensuring HNSW index on chunk_embeddings.embedding"
psql -v ON_ERROR_STOP=1 "$PGURL" -q -c \
  "CREATE INDEX IF NOT EXISTS chunk_embeddings_hnsw_idx ON chunk_embeddings USING hnsw (embedding vector_cosine_ops);"

mkdir -p "$(dirname "$OUTPUT")"

echo "==> find-similar-materials: scoring pairs (threshold=$SIM_THRESHOLD top_k=$TOP_K)"
psql -v ON_ERROR_STOP=1 -At "$PGURL" > "$OUTPUT" <<SQL
WITH near AS (
  SELECT a.id AS id_a,
         n.id AS id_b,
         a.embedding <=> n.embedding AS dist
  FROM chunk_embeddings a
  CROSS JOIN LATERAL (
    SELECT c.id, c.embedding
    FROM chunk_embeddings c
    WHERE c.id <> a.id
    ORDER BY c.embedding <=> a.embedding
    LIMIT $TOP_K
  ) n
)
SELECT json_build_object(
    'mat_no_a',   ma.mat_no,
    'name_a',     ma.name,
    'brand_a',    ma.brand,
    'spec_a',     ma.spec,
    'dims_a',     ma.dims,
    'mat_no_b',   mb.mat_no,
    'name_b',     mb.name,
    'brand_b',    mb.brand,
    'spec_b',     mb.spec,
    'dims_b',     mb.dims,
    'similarity', ROUND((1 - near.dist)::numeric, 4),
    'matched_fields', ARRAY_REMOVE(ARRAY[
        CASE WHEN LOWER(ma.name) = LOWER(mb.name) THEN 'name' END,
        CASE WHEN ma.brand IS NOT NULL AND ma.brand = mb.brand THEN 'brand' END,
        CASE WHEN ma.part_no IS NOT NULL AND ma.part_no = mb.part_no THEN 'part_no' END,
        CASE WHEN ma.spec IS NOT NULL AND LOWER(ma.spec) = LOWER(mb.spec) THEN 'spec' END,
        CASE WHEN ma.dims IS NOT NULL AND LOWER(ma.dims) = LOWER(mb.dims) THEN 'dims' END
    ], NULL)
)
FROM near
JOIN materials ma ON ma.mat_no = near.id_a
JOIN materials mb ON mb.mat_no = near.id_b
WHERE near.id_a < near.id_b
  AND (1 - near.dist) >= $SIM_THRESHOLD
ORDER BY near.dist ASC;
SQL

PAIRS=$(wc -l < "$OUTPUT")
if [ "$PAIRS" -eq 0 ]; then
  echo "==> find-similar-materials: no pairs above similarity $SIM_THRESHOLD — dataset is empty"
  echo "    hint: lower SIM_THRESHOLD (e.g. 0.7), or check that embeddings are real (not mock)"
else
  echo "==> find-similar-materials: wrote $PAIRS candidate pairs to $OUTPUT"
  head -3 "$OUTPUT" | sed 's/^/    /'
fi
