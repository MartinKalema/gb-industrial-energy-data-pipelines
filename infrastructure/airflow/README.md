# Local Airflow runtime

This image runs Apache Airflow 3.3.1 in `standalone` mode for the bounded local
development pipeline. It uses the Airflow 3 public DAG-authoring interface from
`airflow.sdk`, an embedded SQLite metadata database, and SimpleAuthManager.

It is deliberately not a production deployment. Keep the published API port
bound to `127.0.0.1`; SimpleAuthManager stores its password in plaintext and is
intended only for development and testing.

## Compose service contract

The root Compose service should use the following settings:

```yaml
airflow:
  build:
    context: .
    dockerfile: infrastructure/airflow/Dockerfile
  profiles: [batch]
  environment:
    AIRFLOW__API__HOST: 0.0.0.0
    AIRFLOW__API__PORT: 8080
    AIRFLOW__CORE__DAGS_FOLDER: /opt/industrial-energy/orchestration/dags
    AIRFLOW__CORE__DAG_RUN_CONF_OVERRIDES_PARAMS: "True"
    AIRFLOW__CORE__LOAD_EXAMPLES: "False"
    AIRFLOW__CORE__PARALLELISM: "2"
    AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS: "False"
    AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE: /opt/airflow/simple_auth_manager_passwords.json
    AIRFLOW_SIMPLE_AUTH_USERNAME: admin
    PIPELINE_WORK_ROOT: /opt/airflow/work
    PIPELINE_MAX_BATCH_DAYS: "31"
    PYTHONPATH: /opt/industrial-energy
    R2_ACCESS_KEY_ID: ${R2_ACCESS_KEY_ID:?R2_ACCESS_KEY_ID is required}
    R2_SECRET_ACCESS_KEY: ${R2_SECRET_ACCESS_KEY:?R2_SECRET_ACCESS_KEY is required}
    R2_ENDPOINT: ${R2_ENDPOINT:?R2_ENDPOINT is required}
    R2_REGION: ${R2_REGION:-auto}
    R2_RAW_BUCKET: ${R2_RAW_BUCKET:?R2_RAW_BUCKET is required}
    R2_PIPELINE_PREFIX: ${R2_PIPELINE_PREFIX:-industrial-energy}
    TRINO_URL: http://trino:8080
    TRINO_USER: airflow
    TRINO_QUERY_TIMEOUT_SECONDS: "300"
    CLICKHOUSE_HOST: clickhouse
    CLICKHOUSE_PORT: "8123"
    CLICKHOUSE_DATABASE: industrial_energy_serving
    CLICKHOUSE_PUBLISHER_USER: industrial_energy_publisher
    CLICKHOUSE_PUBLISHER_PASSWORD: ${CLICKHOUSE_PUBLISHER_PASSWORD:?CLICKHOUSE_PUBLISHER_PASSWORD is required}
    CLICKHOUSE_READY_VERSIONS_TO_KEEP: ${CLICKHOUSE_READY_VERSIONS_TO_KEEP:-2}
    DBT_PROFILES_DIR: /opt/industrial-energy/transformations
    DBT_PROJECT_DIR: /opt/industrial-energy/transformations
    DBT_LOG_PATH: /opt/airflow/dbt/logs
    DBT_TARGET_PATH: /opt/airflow/dbt/target
    DBT_EXECUTABLE: /home/airflow/.local/bin/dbt
    DBT_TARGET: dev
    DBT_BUILD_TIMEOUT_SECONDS: "7200"
    DBT_TRINO_CLEANUP_TIMEOUT_SECONDS: "60"
    DBT_TRINO_HOST: trino
    DBT_TRINO_PORT: "8080"
    DBT_TRINO_USER: airflow
    DBT_TRINO_CATALOG: r2
    DBT_TRINO_SCHEMA: industrial_energy
    ICEBERG_CATALOG: r2
    ICEBERG_VALIDATED_SCHEMA: industrial_energy_validated
    TRINO_INSERT_BATCH_SIZE: "100"
  ports:
    - "127.0.0.1:${AIRFLOW_PORT:-8081}:8080"
  volumes:
    - .:/opt/industrial-energy:ro
    - airflow-state:/opt/airflow
  depends_on:
    trino:
      condition: service_healthy
    clickhouse:
      condition: service_healthy
  healthcheck:
    test: [CMD, curl, --fail, http://localhost:8080/api/v2/monitor/health]
    interval: 10s
    timeout: 5s
    retries: 30
    start_period: 30s
  mem_limit: 2g

volumes:
  airflow-state:
```

The service needs the repository mounted read-only at
`/opt/industrial-energy`. Airflow state, task logs, generated working bundles,
and the generated authentication file live in the named `airflow-state`
volume. None of them belong in Git. The R2 values come from the ignored root
`.env`; the image contains no credentials.

The startup wrapper migrates the local metadata database and idempotently
creates the one-slot `iceberg_writer` pool before Airflow starts. The source
load, coverage publication, six dbt tasks, ClickHouse publication, and serving
cleanup tasks all use it. The publisher and cleanup task do not write Iceberg,
but sharing the pool prevents
another project writer from changing the tested mart while its separate Trino
export queries run. The DAG's `max_active_runs=1` setting also serializes its
runs. A standalone host-side dbt command is outside the pool and must not run
concurrently with the corresponding Airflow task.

The health endpoint above is Airflow's public API health check. If port `8080`
is already used by Trino, the default host-side Airflow port is `8081`.

## Start and sign in

From the repository root, build and start the batch profile:

```bash
docker compose --project-directory . -f infrastructure/compose.yaml \
  --profile batch up --build airflow
```

On the first start, the wrapper creates a 256-bit URL-safe password for the
configured username. It never uses a checked-in placeholder password, and it
reuses the same password after restarts. Read the local credential explicitly:

```bash
docker compose --project-directory . -f infrastructure/compose.yaml \
  exec airflow python -c \
  'import json; print(json.load(open("/opt/airflow/simple_auth_manager_passwords.json"))["admin"])'
```

Open `http://127.0.0.1:8081`, sign in as `admin`, and manually trigger
`steam_delivery_data_pipeline`. Supply an inclusive local operating-date
range on or after `2026-08-26`, the fixed project seed `20260828`, and a fixed
UTC generation timestamp. Reuse the seed across dates; it identifies one
continuous fictional meter timeline and is not a per-run random value.

The Grid view shows 15 ordered tasks: seven source/control tasks followed by
six dbt tasks, one ClickHouse publication task, and one serving-retention task.
The dbt sequence prepares
and tests loaded data (9 models and
235 tests), prepares and tests delivery calculations (33 models and 8 tests),
builds the current fact, builds the history fact, builds 13 dimension tables,
and runs 70 final mart tests. Dimensions follow the facts because
`dim_data_status` reads both facts on a clean catalog.

After the 70 tests pass,
`publish_tested_dimensional_mart_to_clickhouse` copies the certified current
and history product projections to a new candidate version. It makes that
version visible only after validating it and writing the ready marker last.
The task has a 20-minute limit and up to two retries after one minute. If it
fails, retry only that task; the tested mart remains available for publication
and the API keeps using the previous ready version.

`remove_old_clickhouse_serving_versions` then retains at least the current and
previous ready publications and removes older or incomplete serving rows. It
uses the same one-slot writer pool. If it fails after publication, the new
version remains visible; retry only the cleanup task.

Each dbt task streams its own model or test output into its Airflow log and
stores `run_results.json` plus `dbt.log` below `/opt/airflow/dbt` in a distinct
folder for each checkpoint and Airflow try. Its XCom contains only compact
invocation and result counts. If one section fails, Airflow retries only that
section; the earlier green tasks stay successful.
Clearing the failed task inside that same DAG run keeps those earlier task
states. Triggering a separate new DAG run starts the complete sequence again.

Every dbt task has a 120-minute subprocess limit inside a 125-minute Airflow
task limit. The complete DAG still has a 180-minute limit, so an automatic
retry runs only while enough whole-run time remains. The dbt child receives
only the local Trino/dbt settings and basic process environment; it does not
inherit the Airflow container's R2 access key or secret. Each Airflow try gives
its Trino queries an exact task-and-attempt tag. If dbt times out, exits with an
error, or is interrupted, Airflow stops the local process group, cancels only
active queries with that user and tag, and waits until Trino reports no active
match. If Trino cannot confirm that cleanup, the failed task does not retry
automatically because a retry could overlap unfinished work; verify Trino first
and rerun that task manually.

`plan_run_from_airflow()` rejects a date before the synthetic timeline, an end
date before the start date, and ranges longer than `PIPELINE_MAX_BATCH_DAYS`,
so neither a UI mistake nor an API call can turn this DAG into an unbounded
historical load.

## Retired DAG-name cleanup

The earlier local-only DAG ID `industrial_energy_bounded_batch` was replaced by
the clearer `steam_delivery_data_pipeline`. An existing Airflow volume may
retain the old run history even though its Python file is gone. After confirming
the new DAG appears, remove only that retired metadata with:

```bash
docker compose --project-directory . -f infrastructure/compose.yaml \
  exec airflow airflow dags delete industrial_energy_bounded_batch --yes
```

This removes the retired DAG's local Airflow task/run history. It does not
delete raw R2 evidence, Iceberg source tables, coverage rows, or dimensional
marts.

## Authentication reset

The password is intentionally persistent. To rotate it, stop the service and
remove only the `simple_auth_manager_passwords.json` file from the named volume;
the next start generates a new random value. Removing the entire volume also
deletes the local Airflow metadata database and task history.
