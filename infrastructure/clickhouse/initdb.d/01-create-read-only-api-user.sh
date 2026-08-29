#!/usr/bin/env bash
set -euo pipefail

: "${CLICKHOUSE_USER:?CLICKHOUSE_USER is required}"
: "${CLICKHOUSE_PASSWORD:?CLICKHOUSE_PASSWORD is required}"
: "${CLICKHOUSE_PUBLISHER_PASSWORD:?CLICKHOUSE_PUBLISHER_PASSWORD is required}"
: "${CLICKHOUSE_API_PASSWORD:?CLICKHOUSE_API_PASSWORD is required}"

# The password is passed as a typed query parameter rather than interpolated
# into SQL, so punctuation, whitespace, and quotes remain data. Reapplying the
# definition on every start also makes an intentional local password rotation
# take effect without deleting the persistent ClickHouse data volume.
clickhouse-client \
    --host 127.0.0.1 \
    --user "${CLICKHOUSE_USER}" \
    --password "${CLICKHOUSE_PASSWORD}" \
    --param_publisher_password "${CLICKHOUSE_PUBLISHER_PASSWORD}" \
    --param_api_password "${CLICKHOUSE_API_PASSWORD}" \
    --multiquery \
    --query '
        CREATE USER IF NOT EXISTS industrial_energy_publisher
            IDENTIFIED WITH sha256_password BY {publisher_password:String};
        ALTER USER industrial_energy_publisher
            IDENTIFIED WITH sha256_password BY {publisher_password:String};
        REVOKE ALL ON *.* FROM industrial_energy_publisher;
        GRANT CREATE TABLE, SELECT, INSERT
            ON industrial_energy_serving.* TO industrial_energy_publisher;

        CREATE USER IF NOT EXISTS historical_delivery_api
            IDENTIFIED WITH sha256_password BY {api_password:String};
        ALTER USER historical_delivery_api
            IDENTIFIED WITH sha256_password BY {api_password:String}
            SETTINGS readonly = 2;
        REVOKE ALL ON *.* FROM historical_delivery_api;
        GRANT SELECT ON industrial_energy_serving.* TO historical_delivery_api;
    '
