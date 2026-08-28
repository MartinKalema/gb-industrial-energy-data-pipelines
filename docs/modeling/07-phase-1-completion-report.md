# Phase 1 completion report — dimensional workshop

| Field | Value |
|---|---|
| Status | **Complete** |
| Completed | 2026-08-27 |
| First business process | Steam-delivery performance against a customer commitment |
| Outcome | Accepted logical dimensional and metric contract |
| Implementation state | No physical Iceberg or dbt dimensional schema has been created |
| Next phase | Phase 2 — batch vertical slice |

## Executive outcome

Phase 1 converted the project from a broad energy-industry idea into a precise,
testable agreement about one business process. We can now say exactly what one
delivery result represents, which source is authoritative, how missing and
corrected records behave, which totals are safe to aggregate, how commercial
effects are calculated, and what a customer is allowed to see.

The completed workshop produced:

- 18 explicitly accepted decisions;
- one accepted business process and grain;
- one bus-matrix row with eight contextual subjects;
- eight logical dimensions;
- twelve logical fact measures with additivity rules;
- nine governed metric contracts;
- three hand-calculated reconciliation scenarios with exact expected results;
- one deny-by-default customer data boundary; and
- a reusable plain-English workshop method for every future business process.

This is a completed **logical modeling phase**, not a claim that physical tables
or pipelines already exist.

## Business problem and process

The accepted process is:

> Delivering steam to a customer against an agreed commitment.

It supports two decisions:

- **During delivery:** Is the site on track to meet the current commitment, and
  should an operator intervene?
- **After delivery:** Was the commitment met, what caused any shortfall, and
  what were the commercial consequences?

The [plain-English business-process explainer](02-steam-delivery-business-process-explainer.md)
documents the industrial setting, business problem, process steps, numerical
example, and distinct roles of streaming and batch processing. The
[workshop document](01-business-process-workshop.md) preserves all ten original
questions and records agreements separately so the reasoning remains auditable.

## Accepted business contract

### Grain and authority

- One business result represents one customer delivery point during one
  non-overlapping 30-minute interval.
- The revenue-grade steam meter is authoritative for official delivery.
- Official delivered energy is the accepted closing cumulative register minus
  the accepted opening cumulative register from the same meter.
- SCADA provides a provisional live estimate and diagnostic evidence; it does
  not silently become the contractual result.
- Billing records calculate financial consequences and do not redefine the
  physical quantity delivered.

### Commitment and measurement

- Each delivery point has a scheduled minimum thermal-energy commitment for
  every 30-minute interval.
- Extra delivery in one interval does not erase another interval's shortfall.
- `MWh_th` is canonical for commitment, delivery, and shortfall.
- Steam mass in metric tonnes remains available as a supporting measure.
- Electrical energy is explicitly labeled `MWh_e` so it cannot be confused
  with thermal energy.
- Source values and original units remain preserved before normalization.

### Time, revisions, and missing data

- Event time determines the reporting interval; publication, ingestion, and
  correction times remain separate audit timestamps.
- Interval boundaries are stored in UTC as `[start, end)` and reported to
  customers using the `Europe/London` civil day.
- Daylight-saving days may contain 46, 48, or 50 half-hour intervals; the model
  does not invent or collapse intervals to force 48.
- Duplicate source delivery does not create another accepted business result.
- Late readings return to their original event-time interval.
- All raw revisions are retained; the latest valid revision is current.
- Missing authoritative delivery remains unknown, not zero.
- Invalid resets, rollovers, negative deltas, impossible values, and conflicting
  revisions are quarantined rather than silently repaired.
- Batch and streaming paths use the same identity, revision, and validity rules
  and must converge on the same result.

### Availability and commercial meaning

- Availability means capacity-weighted ability to supply committed thermal
  energy, not simple on/off uptime.
- Planned maintenance leaves the denominator only when the contract approves
  the exclusion and adjusts the commitment.
- Revenue means earned/accrued value in the delivery interval, not invoice value
  or cash received.
- Gross earned revenue, SLA penalty, and net earned revenue remain distinct.
- Invoicing and cash collection are deferred business processes with different
  grains.

### Customer data boundary

- Customer access is deny-by-default and restricted to authenticated customer,
  site, and delivery-point scope.
- Customers may see their own service outcomes, approved cause categories,
  applicable contract terms, projected service charges or SLA credits, and
  data-quality status.
- Raw SCADA, control details, work-order notes, procurement cost, company
  margin, internal pricing logic, other customers, and internal investigation
  notes remain internal.
- APIs, dashboards, exports, and future AI tools must enforce the same row and
  attribute boundaries; UI filtering alone is not authorization.

The complete wording and rationale for these agreements is in the
[decision log](decision-log.md), DM-001 through DM-011.

## Dimensional design delivered

The accepted [bus matrix](03-bus-matrix.md) maps steam-delivery performance to
date, interval, customer, site, delivery point, contract, revenue meter, and
data status.

The accepted [logical dimensional design](04-steam-delivery-dimensional-design.md)
defines:

- eight dimensions: date, interval, customer, site, delivery point, contract,
  meter, and data status;
- event-time-valid dimension history so later descriptive changes do not
  rewrite earlier delivery context;
- twelve measures covering register boundaries, commitment, delivery, steam
  mass, shortfall, excess, deliverable capacity, billable energy, gross earned
  revenue, SLA penalty, and net earned revenue; and
- explicit additivity rules: register boundaries are never summed, interval
  quantities and GBP amounts are additive over non-overlapping rows, and
  percentages are derived from summed numerators and denominators.

Asset, telemetry, work-order, charging, price, and carbon measurements were
deliberately kept out of this fact because they occur at different grains.
This prevents duplicate delivery, false asset attribution, and incorrect cost
or carbon attribution across stored-heat intervals.

## Metric governance delivered

The accepted [metric contracts](05-metric-contracts.md) define nine shared
metrics:

1. delivered energy;
2. committed energy;
3. shortfall energy;
4. SLA attainment;
5. contractual availability;
6. gross earned revenue;
7. accrued SLA penalty;
8. net earned revenue; and
9. delivery-data completeness.

Important safeguards include:

- cap delivery at each interval's commitment before calculating SLA;
- cap deliverable capacity at each interval's commitment before calculating
  availability;
- calculate ratios from summed numerators and denominators rather than averaging
  percentages;
- return `not applicable` when no commitment exists;
- withhold final SLA and financial results until delivery completeness is 100%;
  and
- label any known subtotal as provisional while authoritative delivery is
  incomplete.

These definitions are intended to be implemented once and shared by dbt,
dashboards, APIs, exports, and AI tools.

## Reconciliation evidence

The [three accepted scenarios](06-reconciliation-scenarios.md) provide exact
future test fixtures:

| Scenario | Rule validated | Key accepted result |
|---|---|---|
| 1 — excess and shortfall | Later excess cannot erase an interval miss | 10.0 MWh delivered against 10.0 MWh committed still produces 98% SLA, GBP 20 penalty, and GBP 470 net earned revenue |
| 2 — missing then late | Missing is unknown; late data returns to event time | At 50% completeness final SLA is withheld; after the late reading the result is 95% SLA, GBP 50 penalty, and GBP 425 net earned revenue |
| 3 — corrected shared boundary | A cumulative boundary affects both adjacent deltas | Moving the middle register by 0.2 MWh leaves total delivery at 10.0 MWh but changes SLA from 97% to 99% and net earned revenue from GBP 455 to GBP 485 |

Scenario 3 also records the crucial distinction that a shortfall does not cause
a later excess. Both changed because the same cumulative register closes one
interval and opens the next.

## Decision traceability

| Area | Accepted records |
|---|---|
| Process, grain, authority, units, commitment, availability, revenue, time, corrections, security, delivery event | DM-001 through DM-011 |
| First bus-matrix row | DM-012 |
| Logical dimensions | DM-013 |
| Fact measures and additivity | DM-014 |
| Metric contracts | DM-015 |
| Expected-result scenarios | DM-016 through DM-018 |

All decisions were accepted on 2026-08-27. The immutable workshop-question rule
and reusable [business-process explainer template](business-process-explainer-template.md)
apply to every future business process.

## Phase 1 definition of done

- [x] Business process selected and explained in plain English.
- [x] Operator and historical decisions identified.
- [x] Authoritative delivery event and source precedence accepted.
- [x] Exact fact grain accepted.
- [x] Commitment, units, time, correction, availability, revenue, and security
      rules accepted.
- [x] First bus-matrix row accepted.
- [x] Logical dimensions and fact measures accepted.
- [x] Additivity and missing-data behavior accepted.
- [x] Shared metric contracts accepted.
- [x] Three scenarios calculated by hand and accepted.
- [x] Batch/stream convergence behavior defined.
- [x] Deferred processes and grain boundaries documented.

## Deliberately deferred from Phase 1

Phase 1 did not create or claim completion of:

- physical Iceberg tables, namespaces, partitions, column types, or precision;
- dbt models, seeds, tests, snapshots, or semantic definitions;
- historical synthetic plant, customer, contract, and meter datasets;
- the Elexon batch ingestion DAG;
- the live plant simulator, broker, or Spark streaming service;
- asset-level telemetry, maintenance, and root-cause facts;
- charging-energy, electricity-price, carbon, or thermal-inventory attribution;
- invoice and cash-collection facts; or
- production API, dashboard, and security enforcement.

These are implementation or later-process concerns, not unfinished Phase 1
business definitions.

## Phase 2 entry decisions

The logical business contract is accepted, but four source-contract decisions
must be resolved before all measures can be implemented as final outputs:

1. **Deliverable capacity:** select its authoritative source and define
   derivation, validity, revision, and completeness rules. Availability remains
   provisional until this is accepted.
2. **Shared capacity:** define how capacity serving more than one delivery point
   is allocated so it cannot be counted more than once.
3. **Commitment and contract revisions:** define precedence and effective-time
   behavior for revised schedules and retroactive contract changes, comparable
   to the accepted meter-revision policy.
4. **Approved excess orders:** define the authoritative order identifier,
   effective interval, version, and audit rule for the exception that permits
   excess delivery to become billable.

These are explicit Phase 2 entry decisions rather than rules to invent inside a
dbt model. Record them in the
[Phase 2 source-contract workshop](08-phase-2-source-contract-workshop.md) and
[source-contract decision log](source-contract-decision-log.md).

## Phase 2 handoff

Phase 2 can now implement the batch vertical slice without inventing business
rules in SQL. It must:

1. Define physical source contracts and generated historical private data.
2. Create raw evidence and validated Iceberg models while preserving source
   identifiers, units, timestamps, and revisions.
3. Translate the accepted logical dimensions and fact into exact physical
   columns, types, keys, history behavior, and dbt models.
4. Convert all three scenarios into executable dbt seeds and expected-result
   tests.
5. Implement idempotent date-range backfills and event-time reconciliation.
6. Publish metric outputs with completeness and provisional/final status.
7. Add freshness, lineage, quality, and reconciliation evidence.
8. Keep asset, charging, price, carbon, invoice, and cash measurements in their
   own future processes unless another accepted workshop changes that boundary.

Open implementation choices include numeric precision, surrogate-key mechanics,
physical naming, Iceberg partitioning, dbt incremental strategy, and exact
slowly-changing-dimension implementation. These choices must preserve the
accepted logical contract and the additional source contracts above.
