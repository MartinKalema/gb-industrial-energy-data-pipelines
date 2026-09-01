# Local infrastructure

All compute services in this project run on this computer. Cloudflare R2 is the
only object-storage runtime, and Cloudflare R2 Data Catalog supplies the remote
Iceberg catalog. There is no MinIO or other local object-store service.

The current Compose profiles are:

- `query`: local Trino connected to the Cloudflare R2 Data Catalog;
- `batch`: local Trino, ClickHouse, and Airflow standalone for the date-range
  data pipeline and tested serving-data publication;
- `product`: local ClickHouse plus the read-only historical delivery API and
  web product;
- `stream`: the Spark Structured Streaming feasibility service; and
- `smoke`: the same Spark service used by the cross-engine smoke test.

The `airflow-state` named volume contains only local Airflow metadata, logs,
generated working files, and its authentication file. The `clickhouse-data`
volume contains the rebuildable frontend-serving copy. Spark named volumes
hold test checkpoints and downloaded dependencies. None of these volumes is a
local data lake; durable raw, validation, quarantine, quality, and Iceberg
table data is stored in R2.

The first executable feasibility slice pins Spark 3.5.3, Iceberg 1.6.1, and
Trino 478. These conservative versions align with Cloudflare's published Spark
example and the Trino version tested by the selected dbt-trino release. An
upgrade is a separate measured change after the catalog test is green.

The single-node Trino configuration raises `query.max-stage-count` from its
default 150 to 400. The accepted dbt baseline keeps reusable intermediate
relations as views, and the current source-knowledge history expands to 288
stages during a full build. The higher ceiling is scoped to this isolated local
engine; it is not a production scaling recommendation and must be revisited if
data size, concurrency, or engine topology grows. Trino documents both the
default and the cluster-stability warning in its
[query-management properties](https://trino.io/docs/current/admin/properties-query-management.html#query-max-stage-count).

Run the cross-engine catalog smoke test from the repository root:

```bash
bash tests/smoke/run_lakehouse_smoke.sh
```

The test creates the namespace/table through Trino, processes three file events
through Spark Structured Streaming using catalog-vended credentials, restarts
the Spark job from the same checkpoint to prove it does not duplicate those
events, and reads the committed rows through Trino. Trino stops when the test
finishes. Credentials enter through the ignored `.env` and environment
substitution; they are not stored in the Compose or catalog files.

## Run the bounded Airflow pipeline

After filling the ignored `.env`, start the batch profile:

```bash
docker compose --project-directory . -f infrastructure/compose.yaml \
  --profile batch up --build airflow
```

This builds Apache Airflow 3.3.1, starts Trino 478 and ClickHouse 26.3 LTS, and
exposes the Airflow UI only on `127.0.0.1:${AIRFLOW_PORT:-8081}`. Airflow
creates a high-entropy local password on first startup and persists it in the
ignored named volume. ClickHouse requires the separate publisher and read-only
API passwords documented in the [ClickHouse runtime guide](clickhouse/README.md).
See the [Airflow runtime guide](airflow/README.md) for sign-in and reset
instructions.

The batch service receives R2 credentials from `.env` at runtime. Neither the
image nor Compose contains secret values. It mounts the repository read-only;
task output is written to the Airflow state volume and durable evidence is
written to R2.

The bounded DAG is manual, accepts no more than 31 inclusive dates by default,
writes accepted source revisions through Trino to typed Iceberg tables, and
then builds and tests the dimensional marts in six restartable dbt sections
after coverage publication. If one section fails, Airflow retries that section
without repeating the earlier successful dbt work.
See the [pipeline runbook](../docs/architecture/bounded-airflow-r2-iceberg-pipeline.md)
for the exact contract, R2 object prefixes, retry behavior, and recovery steps.

The Compose-driven milestone was verified against real R2 and the R2-backed
Iceberg catalog on 2026-08-28. The first run inserted 313 rows into nine tables;
the exact replay reused all 313 identities with no conflict.

## Run the historical steam-delivery product

The product reads a tested serving copy of the governed dimensional marts.
First run the bounded Airflow pipeline and confirm its final dbt checkpoint,
`test_complete_dimensional_mart_with_dbt`, and the following ClickHouse
publication task, `publish_tested_dimensional_mart_to_clickhouse`, succeeded. A
failed publication remains invisible to the API, which continues serving the
previous successful release. Confirm the final
`remove_old_clickhouse_serving_versions` task also completed. If cleanup alone
fails, the validated publication may already be visible, but the Airflow run
stays failed; repair and retry only cleanup.

The ready publication must also be younger than the product profile's default
30-hour age limit. For an intentionally static historical demonstration only,
set `PRODUCT_MAX_PUBLICATION_AGE_SECONDS=0`; do not use that as a production
default.

With the ignored `.env` configured with the ClickHouse publisher and API
passwords, start the product profile:

```bash
docker compose --project-directory . -f infrastructure/compose.yaml \
  --profile product up --build
```

The profile starts three local services and does not start Trino:

- ClickHouse serves the last successfully published native-table release;
- `historical-delivery-api` runs bounded, parameterized, read-only queries with
  the dedicated `historical_delivery_api` account; and
- `historical-delivery-web` server-renders the investigation interface and
  calls the API on the internal Compose network.

Default local endpoints are:

| Surface | URL |
|---|---|
| Historical delivery product | <http://127.0.0.1:3000> |
| Web health | <http://127.0.0.1:3000/healthz> |
| API OpenAPI documentation | <http://127.0.0.1:8000/docs> |
| API process health | <http://127.0.0.1:8000/health/live> |
| API mart readiness | <http://127.0.0.1:8000/health/ready> |
| API process metrics | <http://127.0.0.1:8000/health/metrics> |

Change the host ports with `PRODUCT_WEB_PORT` and `PRODUCT_API_PORT` in the
ignored `.env` if the defaults are already in use. ClickHouse's HTTP and native
ports default to `8123` and `9000` and bind only to localhost.

The local profile enables an explicit demo-identity adapter. The available
actors are `commercial-manager`, `customer-cust-001`, and
`customer-cust-002`. Customer actors are restricted by the API to their own
fictional tenant, customer, site, and delivery point. The web persona selector
is not a production login and is never the security boundary; a production
deployment must disable demo mode and provide verified identity.

The product profile also sets a 30-hour ready-publication age limit. This is a
backstop for the local daily-publication assumption; the scheduled daily DAG
must own the exact operating-date deadline. The web starts only after the API's
identity, serving-count, repository, and freshness readiness checks pass. See
the [API operational checks](../docs/operations/api-production-readiness.md)
for alert and capacity-evidence commands and the unresolved production choices.

Known energy and gross financial subtotals may remain visible with a
provisional state. Official SLA, availability, penalty/credit, and net values
remain `null` until their governed finality gates pass. The web product displays
those `null` values as **Unavailable**, not zero. The API also preserves
Europe/London reporting dates separately from UTC interval timestamps.

See the [API guide](../apps/api/README.md),
[web guide](../apps/web/README.md), and
[product architecture](../docs/architecture/historical-steam-delivery-product.md).
The publication and recovery contract is in the
[ClickHouse serving architecture](../docs/architecture/clickhouse-frontend-serving-layer.md)
guide.
