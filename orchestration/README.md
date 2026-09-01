# Airflow orchestration

Airflow orchestrates finite workflows:

- historical extracts and backfills;
- raw-to-validated batch loads;
- bounded dbt build, run, and test tasks through `dbt-trino`;
- batch/stream reconciliation;
- Iceberg compaction and metadata maintenance;
- source freshness and quality checks;
- recovery workflows for known missing intervals.

Long-running IRIS, Redpanda, and Spark consumers run as supervised services. Airflow observes their health and triggers bounded recovery actions.

## Implemented steam-delivery data pipeline

`steam_delivery_data_pipeline` is the first implemented DAG. One manual run
takes a small period of fictional industrial-energy
source data from generation to original files in R2, row checks with failures
saved separately, typed Iceberg source tables, final count reconciliation, and
a queryable successful-run coverage declaration. Six smaller dbt tasks then
prepare and test the loaded data and calculations, build the dimensional
tables, and test the complete mart through local Trino. A publication task
copies that tested mart to the ClickHouse database used by the frontend. A
separate maintenance task then keeps the newest serving versions and removes
older or incomplete ClickHouse copies.

The task order is:

```text
validate_run_parameters
  -> generate_synthetic_source_files
  -> save_original_source_files_to_r2
  -> validate_source_rows_and_save_failures_separately
  -> load_validated_rows_to_iceberg
  -> verify_every_source_row_was_handled
  -> record_successfully_loaded_date_range
  -> prepare_and_test_loaded_data_with_dbt
  -> prepare_and_test_delivery_calculations_with_dbt
  -> build_current_delivery_fact_with_dbt
  -> build_delivery_history_fact_with_dbt
  -> build_dimension_tables_with_dbt
  -> test_complete_dimensional_mart_with_dbt
  -> publish_tested_dimensional_mart_to_clickhouse
  -> remove_old_clickhouse_serving_versions
```

Each name says what the operator should expect:

| Task | Plain-English result |
|---|---|
| `validate_run_parameters` | The dates, fixed seed, timestamp, and local work folder are safe to use. |
| `generate_synthetic_source_files` | Nine fictional business-source files and one file list with counts and hashes exist. |
| `save_original_source_files_to_r2` | The exact original files and their evidence details are durably saved in R2. |
| `validate_source_rows_and_save_failures_separately` | Valid rows are accepted; failed rows are preserved separately with reasons. |
| `load_validated_rows_to_iceberg` | Accepted rows are available in typed Iceberg source tables. |
| `verify_every_source_row_was_handled` | Counts prove that no original row silently disappeared. |
| `record_successfully_loaded_date_range` | dbt can see which operating dates the reconciled load covered. |
| `prepare_and_test_loaded_data_with_dbt` | Nine revision-preserving source views are ready, and their 235 source and staging checks passed. |
| `prepare_and_test_delivery_calculations_with_dbt` | Thirty-three reusable calculation views are ready, and their eight focused checks passed. |
| `build_current_delivery_fact_with_dbt` | The current 30-minute delivery fact is ready. |
| `build_delivery_history_fact_with_dbt` | The as-known delivery-history fact is ready. |
| `build_dimension_tables_with_dbt` | The 13 physical dimension and revision-audit tables are ready. |
| `test_complete_dimensional_mart_with_dbt` | All 70 final mart and reconciliation checks passed. |
| `publish_tested_dimensional_mart_to_clickhouse` | A validated, versioned copy of the tested current and history marts is ready for the frontend. |
| `remove_old_clickhouse_serving_versions` | The new publication is retained, the newest two ready versions are protected when they exist, and older or incomplete ClickHouse copies have been removed. |

Each task exchanges only a small JSON summary through XCom. Records and bulk
files do not pass through the Airflow metadata database.

The final source/control task writes exactly one row per canonical pipeline run to
`r2.industrial_energy_control.batch_run_coverage`, and only runs after count
reconciliation succeeds. This technical timetable is what lets dbt build
expected half-hour rows even when all business evidence for an interval is
missing. Exact Airflow replays reuse the row and retain its first successful
attempt lineage; a changed stable payload under the same run identity is
rejected.

The six downstream dbt tasks accept only a created or exactly reused coverage
row for the same pipeline identity. They run in a fixed order:

1. Build the nine staging views and run the 235 tests that are safe at that
   point.
2. Build the 33 intermediate calculation views and run their eight focused
   tests.
3. Build the current delivery fact.
4. Build the source-knowledge delivery-history fact.
5. Build the 13 dimension and revision-audit tables. The data-status dimension
   reads the completed facts, so this section must follow both fact sections on
   a clean catalog.
6. Run the 70 final dimensional-mart and reconciliation tests.

The first two tasks use dbt's cautious test selection. dbt
runs a test only when every model that test needs is available in that section;
it does not pull a later mart model into an earlier task. All six use
`--no-populate-cache`, write artifacts into the persistent Airflow volume in a
separate folder for each task and attempt, and return only compact result counts
through XCom. If a task fails, Airflow retries only that task. Earlier
successful dbt tasks stay successful and their relations remain available.
This applies to a retry or task clear inside the same Airflow run. Triggering a
brand-new DAG run intentionally starts the 15-task sequence again, with the
source layers safely reusing identical evidence and rows.

A successful coverage row proves the source load reconciled. The dimensional
mart is ready only when `test_complete_dimensional_mart_with_dbt` succeeds. The
frontend serving copy is ready only when
`publish_tested_dimensional_mart_to_clickhouse` then succeeds.
The cleanup task does not make data visible. If it fails after publication,
the new version remains available and cleanup can be retried without rerunning
the source or dbt tasks. The complete Airflow run remains failed until that
maintenance task succeeds.

The publication task reads only the tested Trino mart projections. It writes
current and history rows under a new ClickHouse `load_attempt_id`, reads them
back, and checks counts, unique keys, tenant scopes, date coverage, and exact
content hashes. Its final write is a ready marker whose `publication_id`
matches that attempt. Without the marker, a partial or failed candidate is
invisible to the API and the previous good version remains live. Retrying an
exact successful fingerprint reuses its ready version.

The DAG has no schedule and permits one active run. Source/control tasks retry
once after one minute and have 20-minute execution limits. Each dbt task has a
120-minute subprocess limit, a 125-minute Airflow limit, and one retry after two
minutes. The extra five minutes leave room for up to one minute of local
process cleanup, one minute of Trino cleanup, and scheduler margin. A timeout
stops that task's dbt process and local child-process group before the writer
pool is released, cancels only Trino queries carrying that exact task
attempt's tag, and waits until no active match remains. If Trino cannot confirm
the cleanup, Airflow disables the automatic retry so a new writer cannot
overlap an uncertain old one. The whole run has a bounded practical ceiling of
180 minutes. A dbt retry happens only if enough DAG-run time remains; the
180-minute limit does not guarantee a complete second attempt. The ClickHouse
publication and cleanup tasks each have a 20-minute limit and up to two retries
after one minute; marker-gated publication and marker-first cleanup make those
retries safe. The DAG parameters are:

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
boundary. Recurring growth now uses the separate daily DAG described below;
this DAG stays manual for controlled replays and backfills.

## Daily schedule and freshness contract

`daily_steam_delivery_data_pipeline` is the separate scheduled entry point. It
does not copy or replace the manual pipeline above. At **12:00
Europe/London** each day it chooses the previous completed London operating
date, passes that one-day request to `steam_delivery_data_pipeline`, and waits
for the complete 15-task child run, including ClickHouse publication and
serving-retention cleanup, to succeed.

The schedule uses Airflow's resolved `data_interval_start` and
`data_interval_end`, not the task's wall clock or the old `execution_date`
name. Its noon-to-noon interval can contain 23, 24, or 25 real hours when the UK
clock changes. The code therefore uses the two London calendar dates and still
chooses exactly one operating day.

The freshness contract is:

| Promise | Rule |
|---|---|
| Source allowance | The current synthetic sources can publish a correction on the following morning. Noon is after the latest generated publication, including the UK clock-change fixtures. If a future generator publishes later, its existing generation-time check fails safely instead of loading evidence from the future. |
| Scheduled coverage | A run scheduled at noon on day D+1 covers London operating date D. |
| Ready result | The existing manual DAG must finish all validation, reconciliation, dbt tests, ClickHouse publication, and serving-retention cleanup. |
| Freshness deadline | The complete child workflow for D must finish by **16:00 Europe/London on D+1**. The final daily task checks the wall-clock time after the child succeeds and fails after that time so the breach is visible in Airflow. This is not a direct measurement of the ready marker's timestamp. |
| Missed scheduler days | `catchup=False` prevents a restarted laptop from silently launching many expensive historical runs. Use the manual DAG for an explicit missing-date backfill. |
| Manual trigger | The daily DAG rejects manual runs because Airflow 3 does not guarantee that a manually triggered run's data interval represents the requested date. Use the manual DAG instead. |

The daily wrapper uses a stable child run ID and deterministic generation time
derived from the schedule boundary. It does not reset or skip an existing child
run. If an operator clears the wrapper after the child already exists, Airflow
will report that conflict rather than silently treating an unknown child state
as success. Inspect or repair the existing `steam_delivery_data_pipeline` run
directly.

Iceberg does not enforce a unique source-revision identity. This DAG's
`max_active_runs=1` setting serializes its runs, and the load task uses the
provisioned one-slot `iceberg_writer` pool. Any future manual/backfill DAG that
loads the same tables must also use that pool.
Insert-only merges and post-write verification make completed chunks safe to
retry, but they do not make two concurrent first writers safe.
The ClickHouse publication task also uses the one-slot `iceberg_writer` pool.
Although it does not write Iceberg, holding the same pool prevents another
source or dbt writer from changing the tested mart between the final dbt test
and the publisher's separate Trino export queries. The DAG's
`max_active_runs=1` setting also prevents two runs of this project pipeline from
overlapping.

## Start and trigger it locally

First configure the ignored `.env` from [`.env.example`](../.env.example),
including three different high-entropy ClickHouse passwords. Then start the
`batch` profile from the repository root:

```bash
docker compose --project-directory . -f infrastructure/compose.yaml \
  --profile batch up --build airflow
```

Airflow waits for local Trino to become healthy. The UI is available at
`http://127.0.0.1:8081` by default. The runtime creates a persistent random
256-bit password for the `admin` development user; it does not use a
`change-me` password. Use the documented command in the
[local Airflow runtime guide](../infrastructure/airflow/README.md) to read it.

In the UI, open `steam_delivery_data_pipeline`, choose **Trigger DAG w/
config**, and provide an inclusive date range, seed, and fixed UTC generation
time. Reusing all four values is an intentional idempotency test: it produces
the same pipeline identity, reuses identical R2 evidence, and skips exact
Iceberg source replays before running the restartable dbt sections.

Open any of the six dbt tasks in the Grid view to follow that section's model or
test output. If one fails, fix the cause and retry or clear only that failed
task; the earlier green sections do not need to run again. Do not run the
standalone dbt command at the same time as an Airflow dbt task: the Airflow
`iceberg_writer` pool serializes in-DAG work, but cannot serialize an unrelated
host process.

After the final dbt test is green, open
`publish_tested_dimensional_mart_to_clickhouse`. Its compact result shows the
publication version, whether it was created or reused, current/history counts,
hashes, date coverage, and publication time. If only this task fails, repair
the ClickHouse problem and retry this task; do not restart the successful source
and dbt work.

The final `remove_old_clickhouse_serving_versions` task never deletes the
newest two ready versions and removes older ready markers before their rows. A
first publication naturally has only one ready version. It
also removes incomplete attempts that never received a ready marker. It runs in
the same one-slot writer pool as publication, so it cannot mistake an active
publication for an abandoned one. If only cleanup fails, retry only cleanup;
the newly published version is already visible.

The complete object layout, validation rules, lineage columns, failure
recovery, and scale trade-offs are documented in the
[bounded pipeline architecture and runbook](../docs/architecture/bounded-airflow-r2-iceberg-pipeline.md).
The final serving release is documented in the
[ClickHouse frontend serving architecture](../docs/architecture/clickhouse-frontend-serving-layer.md).

## Local development boundary

Airflow standalone mode uses an embedded SQLite metadata database and a named
volume for task history, logs, working files, and the local authentication
file. That volume is not object storage. R2 remains the only durable
object-storage runtime, and the Iceberg tables are also stored on R2. There is
no MinIO service or local object-store profile.

Standalone mode is for this local learning project. Revisit the metadata
database and executor if runs become concurrent, multi-machine, or production
operated.
