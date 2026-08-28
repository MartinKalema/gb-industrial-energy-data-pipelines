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
feature_run_id="$(openssl rand -hex 16)"

compose exec --no-TTY trino trino --execute "
CREATE SCHEMA IF NOT EXISTS r2.industrial_energy_smoke
"

compose exec --no-TTY trino trino --execute "
CREATE TABLE IF NOT EXISTS r2.industrial_energy_smoke.catalog_feature_matrix (
  smoke_run_id VARCHAR,
  item_id BIGINT,
  metric_value BIGINT
) WITH (format = 'PARQUET', format_version = 2)
"

compose exec --no-TTY trino trino --execute "
ALTER TABLE r2.industrial_energy_smoke.catalog_feature_matrix
ADD COLUMN IF NOT EXISTS evolved_note VARCHAR
"

compose exec --no-TTY trino trino --execute "
INSERT INTO r2.industrial_energy_smoke.catalog_feature_matrix
  (smoke_run_id, item_id, metric_value, evolved_note)
VALUES
  ('$feature_run_id', 1, 100, 'initial'),
  ('$feature_run_id', 2, 200, 'initial')
"

initial_snapshot="$({
  compose exec --no-TTY trino trino --output-format TSV --execute \
    'SELECT snapshot_id FROM r2.industrial_energy_smoke."catalog_feature_matrix$snapshots" ORDER BY committed_at DESC LIMIT 1'
} | tail -n 1 | tr -d '[:space:]')"

compose exec --no-TTY trino trino --execute "
MERGE INTO r2.industrial_energy_smoke.catalog_feature_matrix AS target
USING (
  VALUES
    ('$feature_run_id', BIGINT '1', BIGINT '110', 'merged-update'),
    ('$feature_run_id', BIGINT '3', BIGINT '300', 'merged-insert')
) AS source (smoke_run_id, item_id, metric_value, evolved_note)
ON target.smoke_run_id = source.smoke_run_id
AND target.item_id = source.item_id
WHEN MATCHED THEN UPDATE SET
  metric_value = source.metric_value,
  evolved_note = source.evolved_note
WHEN NOT MATCHED THEN INSERT
  (smoke_run_id, item_id, metric_value, evolved_note)
VALUES
  (source.smoke_run_id, source.item_id, source.metric_value, source.evolved_note)
"

compose exec --no-TTY trino trino --execute "
DELETE FROM r2.industrial_energy_smoke.catalog_feature_matrix
WHERE smoke_run_id = '$feature_run_id' AND item_id = 2
"

current_count="$({
  compose exec --no-TTY trino trino --output-format TSV --execute \
    "SELECT count(*) FROM r2.industrial_energy_smoke.catalog_feature_matrix WHERE smoke_run_id = '$feature_run_id'"
} | tail -n 1 | tr -d '[:space:]')"

historical_count="$({
  compose exec --no-TTY trino trino --output-format TSV --execute \
    "SELECT count(*) FROM r2.industrial_energy_smoke.catalog_feature_matrix FOR VERSION AS OF $initial_snapshot WHERE smoke_run_id = '$feature_run_id'"
} | tail -n 1 | tr -d '[:space:]')"

if [[ "$current_count" != "2" || "$historical_count" != "2" ]]; then
  printf 'Iceberg feature test failed: current=%s historical=%s\n' \
    "$current_count" "$historical_count" >&2
  exit 1
fi

printf 'ICEBERG_FEATURES_OK run_id=%s snapshot=%s current_rows=%s historical_rows=%s\n' \
  "$feature_run_id" "$initial_snapshot" "$current_count" "$historical_count"
