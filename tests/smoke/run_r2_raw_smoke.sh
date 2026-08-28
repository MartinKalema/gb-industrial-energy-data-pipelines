#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  printf 'Missing %s. Create it from .env.example first.\n' "$ENV_FILE" >&2
  exit 1
fi

set -a
source "$ENV_FILE"
set +a

smoke_id="$(openssl rand -hex 8)"
smoke_body="gb-industrial-energy-r2-smoke-$smoke_id"
smoke_url="$R2_ENDPOINT/$R2_RAW_BUCKET/smoke/r2-connectivity-$smoke_id.txt"

remove_smoke_object() {
  curl --silent --show-error \
    --request DELETE \
    --aws-sigv4 'aws:amz:auto:s3' \
    --user "$R2_ACCESS_KEY_ID:$R2_SECRET_ACCESS_KEY" \
    "$smoke_url" >/dev/null 2>&1 || true
}
trap remove_smoke_object EXIT

curl --silent --show-error --fail-with-body \
  --request PUT \
  --aws-sigv4 'aws:amz:auto:s3' \
  --user "$R2_ACCESS_KEY_ID:$R2_SECRET_ACCESS_KEY" \
  --data-binary "$smoke_body" \
  "$smoke_url" >/dev/null

retrieved_body="$(curl --silent --show-error --fail-with-body \
  --aws-sigv4 'aws:amz:auto:s3' \
  --user "$R2_ACCESS_KEY_ID:$R2_SECRET_ACCESS_KEY" \
  "$smoke_url")"

if [[ "$retrieved_body" != "$smoke_body" ]]; then
  printf 'R2 raw roundtrip returned unexpected content.\n' >&2
  exit 1
fi

remove_smoke_object
trap - EXIT
printf 'R2_RAW_ROUNDTRIP_OK object_removed_after_verification=true\n'
