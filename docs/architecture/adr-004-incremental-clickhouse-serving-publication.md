# ADR-004: Checkpointed incremental ClickHouse serving publication

**Status:** Accepted

**Date:** 2026-09-03

**Deciders:** Martin and project collaborator

## Context

The tested dbt marts are the source for the ClickHouse serving copy. The first
publisher read every current and history row through Trino, then sent every row
into ClickHouse for every new version. That is easy to understand and remains a
useful recovery path, but its ClickHouse transfer and insert work grows with the
complete mart even when a daily run changes only a few rows. The new design
still reads every tested row through Trino; it changes what the publisher sends
into ClickHouse.

The dbt models deliberately rebuild and test the complete marts. This is the
correctness baseline. A corrected cumulative meter boundary can change two
delivery results, and a changed dimension can affect many product rows. Making
the dbt build incremental only to reduce ClickHouse transfer would mix two
different decisions and could weaken that baseline.

The project is self-hosted. ClickHouse documents ClickPipes as a ClickHouse
Cloud service, and its lakehouse architecture guide describes an Iceberg CDC
connector as upcoming. ClickHouse 26.3 can read an Iceberg snapshot, but it
does not provide the managed Iceberg CDC path described in that future design.

## Decision

Keep the full dbt rebuild and tests. Change only the publication from the tested
marts into ClickHouse.

After every successful scheduled batch, and after a successful manual replay or
backfill, the publisher compares every stable key and every governed field in
the current and history marts with the newest usable ready ClickHouse version.

The normal publication mode is `incremental`:

1. Create a new `load_attempt_id` with no ready marker.
2. Clone the previous ready rows to that hidden ID inside ClickHouse with
   server-side `INSERT INTO ... SELECT`.
3. Remove keys that were updated or deleted from the hidden copy. Wait for the
   delete mutation to finish.
4. Send and insert into ClickHouse only rows that are new or changed in the
   tested marts.
5. Read and validate the complete new version against the complete tested
   marts. Check counts, keys, null rules, tenant scope, reporting dates, exact
   values, and deterministic content hashes.
6. Save one change summary for each dataset.
7. Write the ready marker last.

The durable `data_publication_change_summary` record stores:

- `publication_mode`: `incremental` or `full`;
- `base_publication_id`: the version cloned by an incremental run, or empty for
  a full run; and
- source, inserted, updated, deleted, and unchanged row counts.

The same values are returned in the Airflow task output for convenient
inspection. The database rows remain the durable record.

Use `full` mode when there is no ready version, or when the latest ready
version's stored data does not match its marker counts and hashes. A full run
transfers every tested row and performs the same final validation. A technical
error while connecting, querying, cloning, deleting, or inserting fails the
task. It must not be hidden by changing to full mode.

This is **checkpointed incremental publication after a tested batch**. It is
not continuous CDC. No process watches Iceberg for changes between Airflow
runs. Checkpointed means each incremental run records its base publication and
becomes a new ready checkpoint only after complete validation.

## Options considered

| Option | Decision | Main reason |
|---|---|---|
| Send every row on every run | Keep only as the first-load and recovery path | Correct and simple, but ClickHouse transfer and insert work always grows with the complete marts |
| Trino Iceberg `table_changes` | Do not use | It requires retained related snapshots, does not support every delete-file change in Trino 478, and the dbt tables are rebuilt rather than kept as a stable change feed |
| Iceberg CDC ClickPipe | Do not use | ClickPipes is Cloud-only and the Iceberg CDC connector is described as upcoming; this project runs self-hosted ClickHouse 26.3 |
| Full comparison, ClickHouse clone, and changed-row insertion | Selected | It keeps complete validation, sends only new and updated rows into ClickHouse, handles deletes in the hidden copy, and can rebuild without a base |

## Trade-offs and limits

- Every dbt mart is still rebuilt and tested in full.
- The publisher still reads every full tested mart row from Trino. It then
  compares every key and field. Only the second transfer, from the
  publisher into ClickHouse, omits unchanged rows.
- ClickHouse still copies unchanged rows internally, so disk, CPU, and storage
  work remain.
- Each ready version is still a complete queryable product version. Retention
  remains necessary.
- A full fallback is slower but is required for first load and recovery.
- Change counts explain what happened, but only the complete validation proves
  that the candidate equals the tested marts.
- Data freshness is still controlled by the daily Airflow schedule and its
  deadline. Incremental publication does not make it real time.

## What true continuous CDC would require

A continuous design would be a separate architecture change. It would require:

- a source that emits changes continuously, or an append-only Iceberg change
  table/outbox;
- stable event ordering, business keys, and rules for updates and deletes;
- durable checkpoints and replay rules;
- continuous schema-change handling, validation, monitoring, and alerting;
- a long-running stream processor; and
- a tested way to make a group of related changes visible together.

Under the accepted compute boundary, Spark Structured Streaming would own that
long-running processing. Airflow would continue to own finite workflows.

## Consequences

- Daily and manual tested batches publish through the same incremental path.
- The first publication and a damaged or missing base use the full path.
- Unchanged rows are not sent from the publisher into ClickHouse as new row
  payloads.
- New and changed rows are sent into ClickHouse; deleted keys are removed from
  the hidden clone.
- The previous ready version remains visible during any failed attempt.
- Operators can inspect durable per-dataset change counts and the base version.
- R2 and Iceberg remain canonical, dbt remains the owner of business results,
  and ClickHouse remains a disposable serving copy.

## References

- [ClickPipes is available in ClickHouse Cloud](https://clickhouse.com/cloud/clickpipes)
- [ClickHouse lakehouse patterns describe the Iceberg CDC Connector as upcoming](https://clickhouse.com/resources/engineering/data-lakehouse)
- [ClickHouse 26.3 Iceberg table-function documentation](https://github.com/ClickHouse/ClickHouse/blob/v26.3.25.2-lts/docs/en/sql-reference/table-functions/iceberg.md)
- [Trino 478 Iceberg `table_changes` documentation](https://github.com/trinodb/trino/blob/478/docs/src/main/sphinx/connector/iceberg.md)
- [Trino 478 implementation showing supported change-task types](https://github.com/trinodb/trino/blob/478/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/functions/tablechanges/TableChangesSplitSource.java#L127-L138)

## Follow-up work

1. Measure comparison time, transferred rows, ClickHouse clone time, mutation
   time, and complete validation time as data grows.
2. Alert on an unexpected switch to full mode or a large change count.
3. Keep the full-rebuild recovery test current.
4. Revisit continuous CDC only when the business needs changes between daily
   releases.
