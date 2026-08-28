# Airflow orchestration

Airflow orchestrates finite workflows:

- historical extracts and backfills;
- raw-to-validated batch loads;
- `dbt build` through `dbt-trino`;
- batch/stream reconciliation;
- Iceberg compaction and metadata maintenance;
- source freshness and quality checks;
- recovery workflows for known missing intervals.

Long-running IRIS, Redpanda, and Spark consumers run as supervised services. Airflow observes their health and triggers bounded recovery actions.

## Implemented bounded batch DAG

`industrial_energy_bounded_batch` is the first implemented DAG. In simple
English, one manual run takes a small period of fictional industrial-energy
source data from generation to durable raw evidence, validation/quarantine,
typed Iceberg source tables, final count reconciliation, and a queryable
successful-run coverage declaration.

The task order is:

```text
plan_run
  -> generate_source_bundle
  -> land_immutable_raw_bundle
  -> validate_and_quarantine
  -> load_validated_rows_to_iceberg
  -> reconcile_evidence_counts
  -> publish_batch_run_coverage
```

Each task exchanges only a small JSON summary through XCom. Records and bulk
files do not pass through the Airflow metadata database.

The final task writes exactly one row per canonical pipeline run to
`r2.industrial_energy_control.batch_run_coverage`, and only runs after count
reconciliation succeeds. This technical timetable is what lets dbt build
expected half-hour rows even when all business evidence for an interval is
missing. Exact Airflow replays reuse the row and retain its first successful
attempt lineage; a changed stable payload under the same run identity is
rejected.

The DAG has no schedule, permits one active run, and retries each failed task
once after one minute. Each task has a 20-minute timeout and the whole run has
a 45-minute timeout. Its parameters are:

- `start_date`: first operating date, inclusive;
- `end_date`: last operating date, inclusive and no earlier than the start;
- `seed`: fixed project seed `20260828`; and
- `generation_time_utc`: fixed UTC timestamp ending in `Z`.

The normal maximum is 31 inclusive days. The run planner checks this again in
Python, so a direct API trigger cannot bypass the bound enforced by the UI.

It is manual because it is the controlled learning/backfill workflow: the
operator chooses which fictional operating dates should exist. Triggering a
run does not automatically mean new business data. Reusing all four inputs
under the same generator version is an exact replay; changing only the
generation timestamp creates a new evidence run around the same source rows;
choosing later dates with the fixed
`20260828` project seed appends new interval evidence. Changing the seed for an
Airflow run is rejected because it would create a discontinuous meter timeline.

The generator now represents one continuous synthetic timeline beginning on
the `2026-08-26` Europe/London operating date. Daily ranges and combined
backfills compose to the same rows, including the shared cumulative-meter
boundary. If recurring growth is accepted later, add a separate daily DAG that
derives a completed operating date from its Airflow data interval and calls the
same workflow; keep this DAG manual for replays and backfills.

Iceberg does not enforce a unique source-revision identity. This DAG's
`max_active_runs=1` setting serializes its runs, and the load task uses the
provisioned one-slot `iceberg_writer` pool. Any future manual/backfill DAG that
loads the same tables must also use that pool.
Insert-only merges and post-write verification make completed chunks safe to
retry, but they do not make two concurrent first writers safe.

## Start and trigger it locally

First configure the ignored `.env` from [`.env.example`](../.env.example). Then
start the `batch` profile from the repository root:

```bash
docker compose --project-directory . -f infrastructure/compose.yaml \
  --profile batch up --build airflow
```

Airflow waits for local Trino to become healthy. The UI is available at
`http://127.0.0.1:8081` by default. The runtime creates a persistent random
256-bit password for the `admin` development user; it does not use a
`change-me` password. Use the documented command in the
[local Airflow runtime guide](../infrastructure/airflow/README.md) to read it.

In the UI, open `industrial_energy_bounded_batch`, choose **Trigger DAG w/
config**, and provide an inclusive date range, seed, and fixed UTC generation
time. Reusing all four values is an intentional idempotency test: it produces
the same pipeline identity, reuses identical R2 evidence, and skips exact
Iceberg replays.

The complete object layout, validation rules, lineage columns, failure
recovery, and scale trade-offs are documented in the
[bounded pipeline architecture and runbook](../docs/architecture/bounded-airflow-r2-iceberg-pipeline.md).

## Local development boundary

Airflow standalone mode uses an embedded SQLite metadata database and a named
volume for task history, logs, working files, and the local authentication
file. That volume is not object storage. R2 remains the only durable
object-storage runtime, and the Iceberg tables are also stored on R2. There is
no MinIO service or local object-store profile.

Standalone mode is for this local learning project. Revisit the metadata
database and executor if runs become concurrent, multi-machine, or production
operated.
