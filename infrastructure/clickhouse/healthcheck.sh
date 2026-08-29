#!/usr/bin/env bash
set -euo pipefail

: "${CLICKHOUSE_API_PASSWORD:?CLICKHOUSE_API_PASSWORD is required}"

# Authenticate as the same least-privileged account used by the product API.
# This verifies more than process liveness: the database, user, password, and
# read-only query path must all be usable before dependent services start.
exec clickhouse-client \
    --host 127.0.0.1 \
    --user historical_delivery_api \
    --password "${CLICKHOUSE_API_PASSWORD}" \
    --database industrial_energy_serving \
    --query "SELECT 1" \
    --format Null
