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

### DBT-002 — layer schemas and model naming

Accepted on 2026-08-28:

> Keep dbt source declarations, staging code, intermediate logic, and marts in
> separate repository folders, and materialize each transformation layer in an
> explicitly named `industrial_energy_*` Trino/Iceberg schema.

Physical structure:

| dbt code | Physical result |
|---|---|
| `models/sources/` | Declares the existing nine tables in `r2.industrial_energy_validated`; creates no relation |
| `models/staging/` | Revision-preserving views in `r2.industrial_energy_staging` named `stg_validated__<source_table>` |
| `models/intermediate/` | Reusable business-logic views in `r2.industrial_energy_intermediate` named `int_<purpose>` |
| `models/marts/` | Governed Iceberg dimensions and facts in `r2.industrial_energy_marts` named `dim_<subject>` and `fct_<process>` |

Rules:

- Use the dbt source name `validated` for
  `r2.industrial_energy_validated`.
- Use dbt's standard custom-schema behavior with the target schema
  `industrial_energy`; do not override `generate_schema_name`. This produces
  the accepted `industrial_energy_staging`, `industrial_energy_intermediate`,
  and `industrial_energy_marts` schema names.
- Staging models preserve every accepted source revision, source-owned column,
  type, and pipeline-lineage field. They do not select current revisions,
  apply business approval filters, or replace missing values.
- Intermediate models own revision selection, event-time joins, interval
  integration, and reusable calculations.
- Analysts and product consumers use governed mart relations rather than
  staging or intermediate models.
- Keep the DBT-001 technical coverage relation separate in
  `r2.industrial_energy_control`.

Separate schemas create more namespaces to browse, but make the transformation
boundary visible and reduce accidental use of source-shaped data as a finished
business result.

### DBT-003 — fixed-decimal quantities and financial calculations

Accepted on 2026-08-28:

> Preserve energy and rate inputs as fixed decimals, calculate interval GBP
> amounts at the full resulting precision, aggregate those exact values, and
> round only when a result is presented.

Physical types:

| Value | Trino/Iceberg type |
|---|---|
| Cumulative registers and energy or mass quantities | `decimal(20,6)` |
| Contract energy and shortfall-penalty rates | `decimal(18,6)` |
| Gross earned revenue, accrued SLA penalty, and net earned revenue | `decimal(38,12)` |

Rules:

- Do not use `double` for energy, rates, contractual quantities, or money.
- Multiplying a `decimal(20,6)` quantity by a `decimal(18,6)` rate produces and
  retains a `decimal(38,12)` interval amount.
- Sum exact interval values before applying presentation rounding. Customer
  displays may round GBP to two decimal places, but that rounded value is not
  written back into the dimensional fact.
- Calculate SLA attainment, contractual availability, and completeness from
  stored additive numerators and denominators; do not store or average rounded
  interval percentages.
- Preserve null measures as null through arithmetic. Missing delivery,
  capacity, or contract inputs must not be converted to numeric zero.
- Keep `delivered_steam_t` nullable as `decimal(20,6)` until an authoritative
  mass measurement or accepted steam-condition calculation exists.
- Geographic coordinates may retain their source `double` representation
  because they are descriptive attributes rather than contractual measures.

Higher-precision monetary storage uses more bytes than a two-decimal amount,
but prevents interval-level rounding from changing period totals.

### DBT-004 — deterministic SHA-256 warehouse keys

Accepted on 2026-08-28:

> Generate replay-stable SHA-256 warehouse keys from the durable source fields
> that define a dimension version or fact grain, while retaining those source
> identifiers as separate columns.

Rules:

- Generate each dimension key from `source_system_id` plus the stable source
  version or assignment-episode identifier that represents that dimension row.
- Generate `delivery_interval_key` from the delivery-point natural identifier
  and canonical UTC interval start that define the accepted fact grain.
- Do not include `source_revision` in a dimension or fact key. A source
  correction updates the accepted representation of the same business version
  or interval instead of creating another dimensional row.
- A genuine new effective business version has a new source version or episode
  identifier and therefore receives a new dimension key.
- Canonicalize key components with explicit field ordering, UTC timestamp
  formatting, unambiguous separators, and no locale-dependent conversion.
- Reject null required key components rather than hashing a null placeholder
  into a valid-looking business key.
- Store the digest as lowercase 64-character hexadecimal text and test it as
  non-null and unique at each declared grain.
- Use a readable integer `date_key` in `YYYYMMDD` form; date is the deliberate
  exception to the hash-key rule.
- Preserve every durable source natural, version, assignment, and interval
  identifier beside the warehouse key for lineage and debugging.

Hexadecimal SHA-256 keys occupy more storage than sequential integers, but they
remain identical across full rebuilds, retries, and future batch/stream paths
without a centralized sequence service.

### DBT-005 — source-driven Type 2 dimension history

Accepted on 2026-08-28:

> Create one dimensional row for each genuine effective business version or
> assignment episode, while collapsing source corrections into the latest
> accepted representation of that same version or episode.

Rules:

- Build Type 2 history directly from source-owned version identifiers and
  `effective_from_utc` / `effective_to_utc`; do not infer it from dbt run time.
- Preserve separate dimension rows for genuine customer, site, contract,
  delivery-point assignment, and meter-assignment versions or episodes.
- Select the latest valid approved source revision within each stable business
  version or assignment identity before building its current accepted
  dimension row.
- Do not create another dimension row merely because a correction increases
  `source_revision`. The deterministic dimension key remains unchanged for
  that corrected version.
- Retain `effective_from_utc`, `effective_to_utc`, and an `is_current` flag on
  effective-dated dimensions. Facts resolve the version covering their full
  event-time interval.
- Keep every earlier source revision queryable in staging and knowledge-time
  history even though it is not another business-history dimension row.
- Do not use dbt snapshots for these dimensions. Snapshot observation time is
  not a substitute for authoritative business effective time or source
  publication time.
- Date, interval, and data-status dimensions do not use Type 2 history.

This representation prevents a correction from inventing a business era while
still preserving genuine historical changes. A separate knowledge-time model
is required to reproduce the description that was known before a correction.

### DBT-006 — separate current and knowledge-time facts

Accepted on 2026-08-28:

> Publish one current accepted delivery fact at the normal business grain and
> a separate revision-aware fact history whose knowledge windows reproduce the
> answer that was valid at a requested publication cutoff.

Rules:

- Build per-source revision histories that preserve the revisions which
  actually became authoritative and calculate `known_from_utc` and
  `known_to_utc` knowledge windows.
- A revision becomes knowledge-eligible only after it is valid, published, and
  approved. Its eligibility time is no earlier than both `published_at_utc`
  and `approved_at_utc`.
- Never let a later-arriving lower source revision displace a higher accepted
  revision. Revisions that never became authoritative do not receive a false
  knowledge window.
- Let accepted cancellations or withdrawals close the preceding knowledge
  window and represent the resulting absent or withdrawn business state; do
  not fall back to an older active revision.
- Keep `fct_steam_delivery_interval` at one current row per delivery point and
  interval.
- Use `fct_steam_delivery_interval_history` for result revisions. Its grain
  additionally includes the result's knowledge window, so it is an audit fact
  rather than the relation used for ordinary current reporting.
- Resolve an as-known query with
  `known_from_utc <= requested_cutoff` and
  (`known_to_utc > requested_cutoff` or `known_to_utc is null`).
- Derive current and historical answers from the same revision-precedence and
  business-calculation logic so they cannot define the metrics differently.
- Do not treat Iceberg snapshot time, Airflow run time, or pipeline ingestion
  time as source knowledge time. Preserve those clocks separately for lineage.

The history relation stores additional rows and costs more to calculate, but
it satisfies the accepted audit question without duplicating revisions in the
current dimensional fact.

### DBT-007 — full-build-first mart materialization

Accepted on 2026-08-28:

> Establish the first dimensional mart as a full-build correctness baseline:
> keep source-preserving staging and reusable intermediate logic as views, and
> rebuild governed dimensions and facts as complete Iceberg tables before
> introducing incremental processing.

Rules:

- Source declarations create no relation; staging and intermediate models are
  views; dimensions and facts are tables.
- A normal `dbt build` recalculates the complete governed mart from the
  accepted Iceberg sources and successful-run coverage.
- Prove the full-build row grain, revision precedence, missing-versus-zero
  behavior, correction propagation, and accepted reconciliation scenarios
  before optimizing refresh behavior.
- Do not introduce an incremental predicate merely because history grows.
  Revisit it only after measured runtime or scan cost is material.
- A future incremental implementation must produce the same result as a full
  rebuild and must recalculate both facts adjacent to a corrected cumulative
  meter boundary plus all affected aggregates.
- Keep the current fact and knowledge-time history fact as separate complete
  tables in this baseline.
- This decision does not close the separate Iceberg partition-layout question;
  initial small tables may remain unpartitioned until measurements justify a
  physical layout.

The first build scans more history than a tuned incremental refresh, but its
simple, complete calculation is the reference result against which every later
optimization can be tested.

## Status

The dbt runtime and Trino connection are verified. DBT-001 through DBT-007 are
accepted. The first full-build mart was implemented and live-verified on
2026-08-28: the one-day slice produces 96 current delivery facts and 582
source-knowledge history windows, and its 78 intermediate/mart tests pass.
Remaining physical modeling decisions stay open until they are reviewed
together. The original workshop questions above are unchanged.
