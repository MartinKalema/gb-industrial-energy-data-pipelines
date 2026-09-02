# ClickHouse serving retention and recovery

## What this protects

ClickHouse is a fast, disposable copy of the tested dimensional mart. Cloudflare
R2 and Iceberg remain the source of truth. Retention and recovery in this guide
may change only these ClickHouse serving tables:

- `industrial_energy_serving.delivery_interval_current`;
- `industrial_energy_serving.delivery_interval_history`;
- `industrial_energy_serving.data_publication_change_summary`; and
- `industrial_energy_serving.data_publication`.

The cleanup code has no R2 or Trino write operation. It never deletes raw R2
evidence, quarantine evidence, Iceberg source tables, dbt models, or Iceberg
snapshots.

## What a normal publication does

The daily Airflow workflow runs the ClickHouse publisher after the complete dbt
mart passes its tests. A manual replay or backfill uses the same publisher. This
is a checkpointed incremental step after a tested batch, not a continuous CDC
service. Checkpointed means the new version records the ready version it used as
its base and becomes ready only after complete validation.

The publisher compares every key and field in both dbt marts with the
newest healthy ready version. For its normal `incremental` mode it:

1. creates a hidden new version;
2. clones the previous ready rows inside ClickHouse;
3. removes updated and deleted keys from the hidden copy;
4. sends only inserted and updated row payloads from the publisher into
   ClickHouse;
5. validates the complete hidden result against the complete tested marts;
6. saves inserted, updated, deleted, and unchanged counts in
   `data_publication_change_summary`; and
7. writes the ready marker last.

The Airflow task output also reports `publication_mode`,
`base_publication_id`, and per-dataset `change_counts`. The database summary is
the durable audit record; the task output is a convenient run view.

The publisher still reads every tested current and history row from Trino so it
can compare and validate the complete results. Incremental mode saves the
second full-row transfer into ClickHouse and avoids inserting unchanged rows
through ClickHouse's HTTP interface.

If there is no ready base, or the newest ready base no longer matches the
counts and hashes in its marker, the publisher uses `full` mode. It transfers
the complete tested marts, performs the same full validation, records the
change counts, and writes the marker last. It does not use fallback to hide a
connection, query, clone, delete, or insert error. Those errors fail the task.

## Accepted local retention rule

The local rule is count based:

1. Keep the newest ready publication.
2. Keep the ready publication immediately before it.
3. Keep more ready publications when
   `CLICKHOUSE_READY_VERSIONS_TO_KEEP` is greater than `2`.
4. Never accept a value below `2`.
5. Remove data and change-summary rows from failed or interrupted attempts that
   have no ready marker.

When two or more ready versions exist, the newest two let a current page finish
on its pinned data version while preserving one previous marked-ready copy for
comparison or fallback. Retention trusts marker order; it does not re-run the
publisher's health checks on old versions. The project has not chosen a
time-based client-session promise, so the code does not claim that two versions
are sufficient for every future production client.

## Why cleanup needs the writer pool

An unfinished publication has candidate rows but no ready marker. From the rows
alone, cleanup cannot tell whether that attempt failed yesterday or is still
being written now.

Applied cleanup must therefore run in an Airflow task that holds the same
one-slot `iceberg_writer` pool as dbt and the ClickHouse publisher. The cleanup
function requires an explicit lock confirmation and refuses an applied cleanup
without it. A dry run does not create tables or otherwise mutate ClickHouse and
does not require the lock; it fails if the serving schema is not already
present.

After a successful publication, cleanup also protects that exact publication
ID. If it is missing or would fall outside the retained set, cleanup stops
before deleting anything.

## Safe deletion order

For an old ready version, cleanup performs the operations in this order:

1. Remove its ready marker with a synchronous ClickHouse mutation.
2. Confirm that the marker is gone and both retained markers still exist.
3. Remove its current, history, and change-summary rows.
4. Confirm that the removed attempt IDs no longer exist.

Marker-first order matters. Once the marker is gone, the API cannot select that
version. If row cleanup then fails, the remaining rows are invisible and the
next cleanup can safely remove them. Deleting rows first could leave a ready
marker pointing at incomplete data, so the implementation does not permit that
order.

An interrupted publication can leave hidden current rows, history rows, or
change-summary rows. They are harmless because they have no ready marker. The
next successful serialized cleanup removes them.

The implementation is in
[`clickhouse_retention.py`](../../ingestion/batch/pipeline/clickhouse_retention.py).
Its focused tests cover dry runs, the two-version minimum, lock enforcement,
marker-first deletion, interrupted deletion, unsafe IDs, cleanup of change
summaries, and protection of the just-published version.

## Recover after ClickHouse data is lost

Use this procedure when the `clickhouse-data` volume is empty, lost, or has
been deliberately replaced after a confirmed serving-database problem.

1. Confirm that the canonical R2/Iceberg tables are healthy through Trino. If
   they are not healthy, stop: the ClickHouse rebuild is not a canonical-data
   restore.
2. Configure the ignored `.env` with valid R2, catalog, and three separate
   ClickHouse credentials.
3. Find the original bounded run's `start_date`, `end_date`, and
   `generation_time_utc`. Use the same values so raw evidence is reused and the
   source replay remains exact.
4. Run the non-destructive rebuild trigger from the repository root:

   ```bash
   ./scripts/rebuild_clickhouse_serving_copy.sh \
     --start-date 2026-08-26 \
     --end-date 2026-08-26 \
     --generation-time-utc 2026-08-28T12:00:00Z \
     --confirm-rebuild
   ```

5. Follow `steam_delivery_data_pipeline` in Airflow. Recovery data is available
   after `test_complete_dimensional_mart_with_dbt` and
   `publish_tested_dimensional_mart_to_clickhouse` both succeed. The bounded
   Airflow run is fully complete only after
   `remove_old_clickhouse_serving_versions` also succeeds.
6. Check API readiness:

   ```bash
   curl --fail http://127.0.0.1:8000/health/ready
   ```

The script starts the batch services and triggers the existing bounded
pipeline. It does **not** remove databases, Docker volumes, Airflow history, R2
objects, or Iceberg tables. Source loading is replay-safe, dbt rebuilds the
governed mart, and the publisher recreates and validates the ClickHouse copy
before writing its ready marker. When ClickHouse has no ready base, the
publisher automatically uses `full` mode. That full publication becomes the
base for later incremental publications.

If only ClickHouse was lost and the successful Airflow run still exists, an
operator may instead clear and rerun only
`publish_tested_dimensional_mart_to_clickhouse` after confirming its upstream
test result and XCom values still exist. Rerunning the full bounded pipeline is
the clearer recovery when Airflow state was also lost.

## Recover a failed incremental publication

Do not restart the complete pipeline when only the ClickHouse publication task
failed and its tested dbt inputs are still available and unchanged.

1. Confirm that `test_complete_dimensional_mart_with_dbt` succeeded in the same
   Airflow run.
2. Confirm that no later pipeline run changed the tested Iceberg marts.
3. Read the publisher error. Repair the ClickHouse, credential, schema, or
   network problem instead of forcing full mode to hide it.
4. Clear and retry only
   `publish_tested_dimensional_mart_to_clickhouse`.
5. Confirm that `disposition` is `created` or `reused`. When a version was
   created, confirm that `publication_mode` is `incremental` or `full`. Inspect
   its `base_publication_id` and `change_counts`.
6. Confirm `remove_old_clickhouse_serving_versions` succeeds. It removes any
   hidden rows left by the failed attempt.

The previous ready version stays visible throughout the failure. A failed
candidate must never be made ready by hand. The publisher must rebuild or reuse
it through its normal validation and final-marker rule.

## Read the change evidence

For each new publication, keep these values with the Airflow run record:

- `publication_mode`: `incremental` or `full`;
- `base_publication_id`: the ready version cloned by an incremental run, or
  empty for a full run;
- total source rows;
- inserted rows;
- updated rows;
- deleted rows; and
- unchanged rows.

These counts exist separately for the current and history datasets. They are
stored in `data_publication_change_summary` before the final marker is written.
Use them to explain transfer volume and unexpected changes. Do not treat the
counts alone as proof of correctness: the complete candidate count, keys,
tenant scope, date coverage, and content hashes must also pass.

A large unchanged count with small inserted and updated counts is expected for
a normal daily run. A sudden full publication, a missing base ID, or a large
change count should be investigated, even if validation passed.

To inspect the stored evidence for one publication, run this read-only SQL with
an account allowed to read the serving database:

```sql
SELECT
    load_attempt_id,
    dataset_name,
    publication_mode,
    base_publication_id,
    source_row_count,
    inserted_row_count,
    updated_row_count,
    deleted_row_count,
    unchanged_row_count,
    recorded_at_utc
FROM industrial_energy_serving.data_publication_change_summary
WHERE load_attempt_id = 'publication-replace-with-the-real-id'
ORDER BY dataset_name;
```

## Do not use ClickHouse as a backup

The serving tables are denormalized and do not contain all raw evidence or all
Iceberg history. They cannot restore R2/Iceberg after canonical data loss.

This project still needs separate decisions before a production claim:

- R2/Iceberg backup location and protection from accidental deletion;
- recovery point objective: how much canonical data may be lost;
- recovery time objective: how quickly service must return;
- Iceberg catalog-metadata recovery and restore testing;
- the maximum time a browser may keep an old pinned publication;
- whether failed invisible attempts need an investigation grace period; and
- retention periods for raw evidence, quarantine evidence, Iceberg snapshots,
  Airflow logs, and audit records.

Those are business, legal, cost, and operating-policy choices. The current
ClickHouse cleanup deliberately does not invent them.

## Current limits

- The dbt marts are still rebuilt and tested in full.
- The publisher still reads all tested mart rows from Trino and compares every
  source and destination key/hash. It saves publisher-to-ClickHouse row transfer
  and insertion work, not all network, compute, or reads.
- Cloning unchanged rows still uses ClickHouse CPU, disk, and storage I/O.
- Changes become available only after the scheduled or manually triggered batch
  finishes. Nothing watches Iceberg continuously.
- A full fallback can take longer because it transfers every row.
- A production continuous CDC design would need an upstream change stream or
  append-only change table, ordering and delete rules, durable checkpoints,
  continuous schema handling, monitoring, and a long-running stream processor.

The architecture decision and alternatives are recorded in
[ADR-004](../architecture/adr-004-incremental-clickhouse-serving-publication.md).
