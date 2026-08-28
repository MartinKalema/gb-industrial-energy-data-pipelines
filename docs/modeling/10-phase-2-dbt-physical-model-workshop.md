# Phase 2 dbt physical-model workshop

## Goal

Translate the accepted steam-delivery grain, dimensions, measures, source
contracts, and revision rules into exact dbt models over Iceberg. This workshop
chooses physical implementation details; it does not reopen accepted business
definitions.

## Accepted rules carried into this workshop

- One current business fact row represents one delivery point during one
  non-overlapping 30-minute interval.
- Every accepted source revision remains auditable.
- The current result uses the latest valid approved revision, while an as-known
  result respects publication time.
- Event time selects the applicable customer, site, delivery point, meter, and
  contract versions.
- Missing official delivery remains null and provisional rather than becoming
  zero.
- The eight logical dimensions, twelve fact measures, nine metric contracts,
  and three reconciliation scenarios remain unchanged.

## Decisions for us to make together

Preserve these questions exactly as written. When an agreement is reached,
record it only in the **Accepted decisions** section and the separate dbt
physical decision log.

1. **Physical names and schemas:** What exact dbt model names and Trino/Iceberg
   schemas represent source declarations, staging models, intermediate models,
   dimensions, facts, and governed metric outputs?
2. **Numeric representation:** What fixed decimal precision and scale should
   physical energy quantities, rates, and GBP amounts use so calculations do
   not lose precision or overflow?
3. **Warehouse keys:** How should deterministic dimension and fact surrogate
   keys be generated while retaining every durable source natural identifier?
4. **Dimension history:** How should effective business versions and corrected
   source revisions be represented without confusing a real historical change
   with a correction?
5. **Knowledge-time history:** What physical models should expose the current
   accepted answer and the answer that was known at a requested publication
   cutoff?
6. **Materialization and refresh:** Which models should be views, tables, or
   incremental Iceberg models, and how should a correction rebuild both
   affected meter intervals and all dependent results?
7. **Iceberg layout:** Which physical tables need partitioning, what event-time
   field should drive it, and which small dimensions should remain
   unpartitioned?
8. **Fact spine:** Which physical record set should create every expected
   delivery-point interval row so a missing commitment, reading, contract, or
   capacity assessment remains visible rather than disappearing from the mart?
9. **Mart lineage:** Which source identifiers, revision markers, publication
   timestamps, and pipeline evidence fields should remain directly on the
   dimensional fact, and which should stay in supporting audit models?
10. **Executable reconciliation:** How should the three accepted scenarios be
    represented as dbt seeds, unit tests, and data tests so their expected
    interval and aggregate results remain exact?

## Accepted decisions

### DBT-001 — batch-run coverage drives the delivery-interval spine

Accepted on 2026-08-28:

> A queryable Iceberg control record for every successfully reconciled bounded
> Airflow run declares the local operating-date coverage from which dbt creates
> every expected delivery-point and 30-minute interval combination.

In plain English, the control record is the timetable for the batch. It lets
the mart show an expected interval even when every business record for that
interval is missing.

Rules:

- Store the control relation in
  `r2.industrial_energy_control.batch_run_coverage`; it is technical pipeline
  evidence, not a tenth business source.
- Publish coverage only after the bounded run has reconciled its raw,
  validation, quarantine, and Iceberg-load counts.
- Preserve the inclusive `Europe/London` operating-date range, its timezone,
  the pipeline-run identity, generation and ingestion timestamps, manifest
  identity, reconciliation status, and relevant counts.
- Expand local operating dates into real UTC half-hours, including 46-, 48-,
  and 50-interval daylight-saving days.
- Combine those intervals with delivery-point assignments that are effective
  for the complete `[interval_start_utc, interval_end_utc)` period.
- Deduplicate overlapping and replayed coverage by delivery point and UTC
  interval; they must not create duplicate dimensional facts.
- Left join business evidence onto the spine so absent commitments, readings,
  contracts, and capacity remain visible as null/provisional states.
- Failed or unreconciled runs do not declare complete batch coverage.

This additional control table is accepted because deriving the spine only from
available business rows would make a completely absent interval disappear and
could falsely improve completeness metrics.

## Status

The dbt runtime and Trino connection are verified. DBT-001 is accepted;
remaining physical modeling decisions are being reviewed one at a time before
the mart SQL is implemented.
