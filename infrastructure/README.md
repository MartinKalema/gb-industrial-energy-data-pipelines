# Local infrastructure

All compute services in this project run on this computer. Cloudflare R2 is the
only object-storage runtime, and Cloudflare R2 Data Catalog supplies the remote
Iceberg catalog. There is no MinIO or other local object-store service.

The current Compose profiles are:

- `query`: local Trino connected to the Cloudflare R2 Data Catalog;
- `batch`: local Trino plus Airflow standalone for the bounded batch DAG;
- `stream`: the Spark Structured Streaming feasibility service; and
- `smoke`: the same Spark service used by the cross-engine smoke test.

The `airflow-state` named volume contains only local Airflow metadata, logs,
generated working files, and its authentication file. Spark named volumes hold
test checkpoints and downloaded dependencies. These volumes are not a local
data lake; durable raw, validation, quarantine, quality, and Iceberg table data
is stored in R2.

The first executable feasibility slice pins Spark 3.5.3, Iceberg 1.6.1, and
Trino 478. These conservative versions align with Cloudflare's published Spark
example and the Trino version tested by the selected dbt-trino release. An
upgrade is a separate measured change after the catalog test is green.

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

This builds Apache Airflow 3.3.1, starts Trino 478, and exposes the Airflow UI
only on `127.0.0.1:${AIRFLOW_PORT:-8081}`. Airflow creates a high-entropy local
password on first startup and persists it in the ignored named volume. See the
[Airflow runtime guide](airflow/README.md) for sign-in and reset instructions.

The batch service receives R2 credentials from `.env` at runtime. Neither the
image nor Compose contains secret values. It mounts the repository read-only;
task output is written to the Airflow state volume and durable evidence is
written to R2.

The bounded DAG is manual, accepts no more than 31 inclusive dates by default,
and writes accepted source revisions through Trino to typed Iceberg tables.
See the [pipeline runbook](../docs/architecture/bounded-airflow-r2-iceberg-pipeline.md)
for the exact contract, R2 object prefixes, retry behavior, and recovery steps.

The Compose-driven milestone was verified against real R2 and the R2-backed
Iceberg catalog on 2026-08-28. The first run inserted 313 rows into nine tables;
the exact replay reused all 313 identities with no conflict.
