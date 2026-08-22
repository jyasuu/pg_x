#!/usr/bin/env bash
# Embed every materials row into pgvector (chunk_embeddings) through the
# existing pgx embedding pipeline:
#
#   export rows -> RabbitMQ -> consume (GraphQL MaterialFull composition)
#               -> embed via Ollama/OpenAI API -> postgres-vector sink
#
# Thin wrapper over scripts/backfill_embedding.sh — it only sets the
# material-similarity defaults (query, template, model, queue names) and
# installs this skill's GraphQL assets (schema/queries/resolvers extended
# with brand/part_no/spec/dims) into ~/.pgx/.
#
# Env (all optional):
#   DATABASE_URL          default docker-compose stack
#   RAG_MCP_OLLAMA_URL    real embedding API base URL; when EMPTY a local mock
#                         /api/embed responder is used instead (pipeline test
#                         only — vectors are fake)
#   EMBED_MODEL           default bge-m3
#   EMBED_DIM             default 1024
#   VECTOR_TABLE          default chunk_embeddings
#   EMBED_TEMPLATE        default '{{ toon(doc) }}' — serializes the whole
#                         composed material doc via the built-in toon() helper
#   ES_INDEX              ES index written alongside pgvector (default documents)
#   QUEUE / ROUTING_KEY   RabbitMQ names for this pipeline
#   PGX_BINARY            path to the pgx binary (default target/release/pgx)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILL_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(cd "$SKILL_DIR/../.." && pwd)"

export DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/postgres}"
export PGX="${PGX_BINARY:-$REPO_ROOT/target/release/pgx}"
export AMQP_URL="${AMQP_URL:-amqp://guest:guest@localhost:5672/%2F}"
export ES_URL="${ES_URL:-http://localhost:9200}"

export EMBED_MODEL="${EMBED_MODEL:-bge-m3}"
export EMBED_DIM="${EMBED_DIM:-1024}"
export VECTOR_TABLE="${VECTOR_TABLE:-chunk_embeddings}"
export EVENT_TYPE="MaterialFull"
export ID_FIELD="mat_no"
export ES_INDEX="${ES_INDEX:-documents}"
export ROUTING_KEY="${ROUTING_KEY:-pgx.materialsim}"
export QUEUE="${QUEUE:-pgx-materialsim}"
export BACKFILL_QUERY="${BACKFILL_QUERY:-SELECT mat_no, name, status, brand, part_no, spec, dims FROM materials}"
export EMBED_TEMPLATE="${EMBED_TEMPLATE:-{{ toon(doc) }}}"

if [ ! -x "$PGX" ]; then
  echo "embed-materials: pgx binary not found at $PGX — run 'cargo build --release' first" >&2
  exit 1
fi

echo "==> embed-materials: installing skill GraphQL assets (extended Material type + resolvers)"
mkdir -p ~/.pgx/schema ~/.pgx/queries
cp -r "$SKILL_DIR/graphql/schema/"* ~/.pgx/schema/
cp -r "$SKILL_DIR/graphql/queries/"* ~/.pgx/queries/
cp "$SKILL_DIR/graphql/config.toml" ~/.pgx/config.toml

# Skill assets win over examples inside backfill_embedding.sh too.
export PGX_ASSETS_DIR="$SKILL_DIR/graphql"

if [ -n "${RAG_MCP_OLLAMA_URL:-}" ]; then
  echo "==> embed-materials: embedding via $RAG_MCP_OLLAMA_URL (model=$EMBED_MODEL dim=$EMBED_DIM)"
else
  echo "==> embed-materials: RAG_MCP_OLLAMA_URL not set — using LOCAL MOCK embeddings (fake vectors)"
fi

exec bash "$REPO_ROOT/scripts/backfill_embedding.sh"
