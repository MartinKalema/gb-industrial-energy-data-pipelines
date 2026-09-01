# ClickHouse serving retention and recovery

## What this protects

ClickHouse is a fast, disposable copy of the tested dimensional mart. Cloudflare
R2 and Iceberg remain the source of truth. Retention and recovery in this guide
may change only these ClickHouse serving tables:

- `industrial_energy_serving.delivery_interval_current`;
- `industrial_energy_serving.delivery_interval_history`; and
- `industrial_energy_serving.data_publication`.

The cleanup code has no R2 or Trino write operation. It never deletes raw R2
evidence, quarantine evidence, Iceberg source tables, dbt models, or Iceberg
snapshots.

## Accepted local retention rule

The local rule is count based:

1. Keep the newest ready publication.
2. Keep the ready publication immediately before it.
3. Keep more ready publications when
   `CLICKHOUSE_READY_VERSIONS_TO_KEEP` is greater than `2`.
4. Never accept a value below `2`.
5. Remove rows from failed or interrupted attempts that have no ready marker.

When two or more ready versions exist, the newest two let a current page finish on its pinned data version
while preserving one previous good copy for quick comparison or fallback. The
project has not chosen a time-based client-session promise, so the code does not
claim that two versions are sufficient for every future production client.

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
3. Remove its current and history rows.
4. Confirm that the removed attempt IDs no longer exist.

Marker-first order matters. Once the marker is gone, the API cannot select that
version. If row cleanup then fails, the remaining rows are invisible and the
next cleanup can safely remove them. Deleting rows first could leave a ready
marker pointing at incomplete data, so the implementation does not permit that
order.

The implementation is in
[`clickhouse_retention.py`](../../ingestion/batch/pipeline/clickhouse_retention.py).
Its focused tests cover dry runs, the two-version minimum, lock enforcement,
marker-first deletion, interrupted deletion, unsafe IDs, and protection of the
just-published version.

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
before writing its ready marker.

If only ClickHouse was lost and the successful Airflow run still exists, an
operator may instead clear and rerun only
`publish_tested_dimensional_mart_to_clickhouse` after confirming its upstream
test result and XCom values still exist. Rerunning the full bounded pipeline is
the clearer recovery when Airflow state was also lost.

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
