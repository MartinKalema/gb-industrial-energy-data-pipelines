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
creates the one-slot `iceberg_writer` pool before Airflow starts. The Iceberg
load task uses that pool so future writers have one shared serialization
boundary rather than relying only on one DAG's `max_active_runs` setting.

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
`industrial_energy_bounded_batch`. Supply an inclusive local operating-date
range on or after `2026-08-26`, the fixed project seed `20260828`, and a fixed
UTC generation timestamp. Reuse the seed across dates; it identifies one
continuous fictional meter timeline and is not a per-run random value.

`plan_run_from_airflow()` rejects a date before the synthetic timeline, an end
date before the start date, and ranges longer than `PIPELINE_MAX_BATCH_DAYS`,
so neither a UI mistake nor an API call can turn this DAG into an unbounded
historical load.

## Authentication reset

The password is intentionally persistent. To rotate it, stop the service and
remove only the `simple_auth_manager_passwords.json` file from the named volume;
the next start generates a new random value. Removing the entire volume also
deletes the local Airflow metadata database and task history.
