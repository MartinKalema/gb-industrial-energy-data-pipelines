#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/infrastructure/compose.yaml"
ENV_FILE="$PROJECT_ROOT/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  printf 'Missing %s. Create it from .env.example first.\n' "$ENV_FILE" >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  printf 'Docker is not running. Start Docker Desktop and retry.\n' >&2
  exit 1
fi

compose() {
  docker compose \
    --project-directory "$PROJECT_ROOT" \
    --env-file "$ENV_FILE" \
    --file "$COMPOSE_FILE" \
    "$@"
}

stop_query_engine() {
  compose stop trino >/dev/null 2>&1 || true
}
trap stop_query_engine EXIT

compose config --quiet
compose up --detach --wait trino

compose exec --no-TTY trino \
  trino --file /opt/industrial-energy/smoke/trino_prepare.sql

export SMOKE_RUN_ID
SMOKE_RUN_ID="$(openssl rand -hex 16)"

# The second container uses the same checkpoint and must not append duplicates.
compose run --rm spark-smoke
compose run --rm spark-smoke

visible_rows="$({
  compose exec --no-TTY trino trino \
    --output-format TSV \
    --execute "SELECT count(*) FROM r2.industrial_energy_smoke.structured_stream_roundtrip WHERE smoke_run_id = '$SMOKE_RUN_ID'"
} | tail -n 1 | tr -d '[:space:]')"

if [[ "$visible_rows" != "3" ]]; then
  printf 'Expected three non-duplicated Spark rows in Trino; observed count=%s\n' "$visible_rows" >&2
  exit 1
fi

compose exec --no-TTY trino \
  trino --file /opt/industrial-energy/smoke/trino_verify.sql

printf 'LAKEHOUSE_SMOKE_OK run_id=%s spark_rows_visible_in_trino=%s\n' \
  "$SMOKE_RUN_ID" "$visible_rows"
