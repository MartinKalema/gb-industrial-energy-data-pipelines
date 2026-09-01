#!/usr/bin/env bash
set -euo pipefail

usage() {
  local command_name
  command_name="$(basename "$0")"
  echo "Usage: ${command_name} --start-date YYYY-MM-DD --end-date YYYY-MM-DD --generation-time-utc RFC3339-Z --confirm-rebuild"
  echo
  echo "Starts the batch services and triggers the existing bounded pipeline."
  echo "It does not delete ClickHouse, R2, Iceberg, Airflow state, or Docker volumes."
}

start_date=""
end_date=""
generation_time_utc=""
confirm_rebuild="false"

while (($#)); do
  case "$1" in
    --start-date)
      start_date="${2:-}"
      shift 2
      ;;
    --end-date)
      end_date="${2:-}"
      shift 2
      ;;
    --generation-time-utc)
      generation_time_utc="${2:-}"
      shift 2
      ;;
    --confirm-rebuild)
      confirm_rebuild="true"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "${confirm_rebuild}" != "true" ]]; then
  echo "Refusing to trigger a rebuild without --confirm-rebuild." >&2
  exit 2
fi

if [[ -z "${start_date}" || -z "${end_date}" || -z "${generation_time_utc}" ]]; then
  usage >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd "${script_dir}/.." && pwd)"

run_config="$(
  python3 - "${start_date}" "${end_date}" "${generation_time_utc}" <<'PY'
import json
import sys
from datetime import date, datetime

start_text, end_text, generation_text = sys.argv[1:]
try:
    start = date.fromisoformat(start_text)
    end = date.fromisoformat(end_text)
except ValueError as error:
    raise SystemExit("start and end dates must use YYYY-MM-DD") from error
if end < start:
    raise SystemExit("end date must not be before start date")
if (end - start).days + 1 > 31:
    raise SystemExit("the bounded rebuild cannot exceed 31 inclusive days")
if not generation_text.endswith("Z"):
    raise SystemExit("generation time must be an RFC 3339 UTC timestamp ending in Z")
try:
    datetime.fromisoformat(generation_text[:-1] + "+00:00")
except ValueError as error:
    raise SystemExit("generation time must be an RFC 3339 UTC timestamp ending in Z") from error

print(
    json.dumps(
        {
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "seed": 20260828,
            "generation_time_utc": generation_text,
        },
        separators=(",", ":"),
    )
)
PY
)"

compose=(
  docker compose
  --project-directory "${project_dir}"
  -f "${project_dir}/infrastructure/compose.yaml"
)

echo "Starting the batch services without deleting any data..."
"${compose[@]}" --profile batch up -d --build --wait airflow

echo "Triggering the bounded pipeline with the supplied original run identity..."
"${compose[@]}" exec -T airflow \
  airflow dags trigger steam_delivery_data_pipeline --conf "${run_config}"

echo
echo "The rebuild was triggered. Follow steam_delivery_data_pipeline in Airflow."
echo "Recovery is complete only after test_complete_dimensional_mart_with_dbt and"
echo "publish_tested_dimensional_mart_to_clickhouse both succeed."
