# Local ClickHouse serving database

ClickHouse is a disposable, rebuildable serving copy of the tested dimensional
mart. Cloudflare R2 and Apache Iceberg remain canonical. The product API reads
native ClickHouse tables so an interactive page request does not wait for R2,
Iceberg metadata, Trino planning, or object downloads.

The Compose service pins the official ClickHouse `26.3.25.2` image from the
26.3 LTS line. Its data persists in the ignored `clickhouse-data` named volume.
It has three deliberately separate accounts:

- `clickhouse_bootstrap` reconciles local users at container startup and is
  never passed to Airflow or the product API;
- `industrial_energy_publisher` can create, read, insert, and run controlled
  retention deletes only on serving tables in `industrial_energy_serving`; and
- `historical_delivery_api` has `SELECT` only on
  `industrial_energy_serving.*` and the ClickHouse `readonly` setting.

Set different high-entropy `CLICKHOUSE_BOOTSTRAP_PASSWORD`,
`CLICKHOUSE_PUBLISHER_PASSWORD`, and `CLICKHOUSE_API_PASSWORD` values in the
ignored root `.env`. Do not place real values in Compose or this directory.
Single-quote a `.env` value when it contains `$`, `#`, whitespace, or `!`; the
startup scripts pass passwords as arguments or typed query parameters rather
than inserting them into SQL.

The database and read-only user are reconciled on every container start, so a
password rotation takes effect after recreating the service. The health check
authenticates as the read-only API user and runs a query in the serving
database. Airflow and the API do not start until this succeeds.

Start ClickHouse with either the batch or product profile:

```bash
docker compose --project-directory . -f infrastructure/compose.yaml \
  --profile product up -d clickhouse
```

The HTTP and native protocols bind only to localhost, on ports `8123` and
`9000` by default. The publication task shares Airflow's one-slot
`iceberg_writer` pool with dbt. This keeps the tested Iceberg mart unchanged
while the publisher's separate Trino queries read and compare it with the last
ready ClickHouse version.

## Publication boundary

The publication task is `publish_tested_dimensional_mart_to_clickhouse`. It runs after
`test_complete_dimensional_mart_with_dbt` and writes four native `MergeTree`
tables in `industrial_energy_serving`:

- `delivery_interval_current`;
- `delivery_interval_history`;
- `data_publication_change_summary`; and
- `data_publication`.

The first publication, or a publication without a healthy base, sends every
tested row into ClickHouse. Later publications read the complete tested marts
through Trino and compare them with the newest healthy ready version. They
clone unchanged rows inside ClickHouse, remove deleted or replaced keys from a
hidden new copy, and insert only new or updated rows into ClickHouse.

Current and history rows carry a new `load_attempt_id`. They do not become
visible to the API unless complete validation passes and the task writes a
final ready marker with the same value as its `publication_id`. Before that
marker, the task stores source, inserted, updated, deleted, and unchanged row
counts in `data_publication_change_summary`. Partial rows therefore stay
hidden, and a failed attempt does not disturb the previous marked-ready
version. An exact retry reuses its existing ready publication.

This is checkpointed incremental publication after a tested batch. It is not
continuous CDC and does not watch Iceberg between Airflow runs.

The final Airflow task, `remove_old_clickhouse_serving_versions`, then protects
the newest two ready publications when they exist and removes older or
incomplete serving copies. A cleanup failure does not hide a publication that
was already made ready, but the Airflow run remains failed until cleanup is
retried successfully.

For the full startup order, validation contract, and local verification
evidence, see the
[ClickHouse frontend serving architecture](../../docs/architecture/clickhouse-frontend-serving-layer.md).
The accepted decision and alternatives are in
[ADR-004](../../docs/architecture/adr-004-incremental-clickhouse-serving-publication.md).
For marker-first cleanup and a non-destructive rebuild procedure, see the
[serving retention and recovery runbook](../../docs/operations/clickhouse-serving-retention-and-recovery.md).
