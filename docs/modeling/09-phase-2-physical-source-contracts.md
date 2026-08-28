# Phase 2 physical source contracts

## Goal

Turn the accepted business and source-authority rules into exact promises about
the records that Phase 2 will ingest. These contracts are agreed before the
synthetic generator, Airflow jobs, Iceberg tables, or dbt models are built.

## What a physical source contract means

In plain English, a physical source contract says:

> This is what one incoming record represents, these fields and units must be
> present, these timestamps and identifiers have specific meanings, and this is
> how duplicates, corrections, and invalid records are handled.

For every source, the contract will define:

- one-record meaning and grain;
- durable source identifier and revision identifier;
- required fields, data types, and units;
- event, effective, publication, ingestion, and correction times as applicable;
- duplicate, late-arrival, correction, and precedence behavior;
- validation and quarantine rules;
- raw-evidence preservation; and
- the validated output that downstream models may use.

## Decisions for us to make together

1. **Revenue-meter reading identity and grain:** What exactly does one source
   record represent, and which identifiers distinguish a boundary reading from
   its revisions?
2. **Revenue-meter revision authority:** Which reading states may become
   official, and how are duplicates, late versions, corrections, and
   withdrawals resolved?
3. **Revenue-meter validity:** Which boundary times, numeric representation,
   units, assignments, and plausibility checks must a reading pass?
4. **Revenue-meter exceptional events:** How will Phase 2 treat resets,
   rollovers, meter replacements, and estimated or substituted readings?
5. **Revenue-meter correction impact:** Which delivery intervals and dependent
   results must be recalculated when a shared boundary changes?
6. **Revenue-meter assignment identity and grain:** What exactly does one
   assignment record represent, and which identifiers distinguish an
   assignment episode from its revisions?
7. **Revenue-meter assignment authority:** How must an assignment cover a
   delivery interval before its meter and register can provide both official
   boundary readings?
8. **Revenue-meter assignment validity and history:** How are overlaps, gaps,
   cancellations, late revisions, and retroactive corrections resolved?
9. **Revenue-meter assignment metadata:** Which register role, unit,
   calibration, and physical-plausibility attributes belong in the Phase 2
   assignment source?
10. **Revenue-meter replacement scope:** Will Phase 2 model meter replacement,
    or require one stable assignment throughout each generated test period?
11. **Delivery-point assignment identity and grain:** What exactly does one
    assignment record represent, and which identifiers distinguish an
    effective relationship from its revisions?
12. **Delivery-point relationship authority:** How are a delivery point,
    industrial site, and customer related at event time, and which cardinality
    rules must Phase 2 enforce?
13. **Delivery-point assignment validity:** How must the relationship cover a
    delivery interval, and how are gaps, overlaps, cancellations, and
    conflicting relationships handled?
14. **Delivery-point history and security:** How do late or retroactive
    corrections change historical dimensional attribution and customer access
    without rewriting prior evidence?
15. **Delivery-point reassignment scope:** Will Phase 2 generate customer or
    site reassignment, or require stable relationships throughout each test
    period?
16. **Customer-master identity and grain:** What exactly does one customer
    record represent, and which identifier remains stable across descriptive
    changes and source revisions?
17. **Customer business change versus correction:** How is a genuine
    effective-dated customer change distinguished from correcting an erroneous
    version of older source data?
18. **Customer authorization scope:** Which durable customer attribute governs
    tenant access, and what happens when customer identity is missing,
    inactive, or conflicting?
19. **Customer history and lifecycle:** How are overlapping versions, late
    revisions, deactivation, and attempted identifier reuse handled?
20. **Customer test coverage:** Which historical change should the Phase 2
    synthetic data include to prove event-time dimensional history?
21. **Site-master identity and grain:** What exactly does one industrial-site
    record represent, and which identifier remains stable across descriptive
    changes and source revisions?
22. **Site location and timezone authority:** Which location, region, country,
    and IANA timezone attributes are authoritative for reporting and
    validation?
23. **Site business change versus correction:** Which changes create a new
    effective site version, which correct an older version, and which require a
    completely new site identity?
24. **Site history and lifecycle:** How are overlapping versions, late
    revisions, activation, deactivation, and missing site context handled?
25. **Site test coverage:** Which historical site change should the Phase 2
    synthetic data include to prove event-time dimensional history?
26. **Contract-terms identity and grain:** What exactly does one contract-terms
    record represent, and which identifiers distinguish a contract, an
    effective business version, and a source revision?
27. **Contract applicability:** How is one contract version assigned to a
    delivery point and customer for a complete delivery interval, and what
    happens when coverage is missing or conflicting?
28. **Contract financial terms:** Which rate, penalty, currency, unit, decimal,
    and rounding rules are authoritative for Phase 2 calculations?
29. **Contract amendment versus correction:** How is a genuine effective-dated
    commercial amendment distinguished from correcting an erroneous source
    version, especially after delivery has occurred?
30. **Contract Phase 2 scope and test coverage:** Which simple terms and
    revision scenarios will be generated now, and which tariff, invoice, tax,
    or settlement complexity remains deferred?
31. **Commitment-schedule identity and grain:** What exactly does one schedule
    record represent, and which identifiers distinguish an interval commitment
    from its source revisions?
32. **Commitment meaning and completeness:** How are a positive commitment, an
    explicit no-commitment interval, and a missing schedule record represented
    without confusing unknown with zero?
33. **Commitment applicability and validation:** How must the delivery point,
    contract, unit, interval, approval, and event-time relationships agree
    before a commitment may govern a delivery result?
34. **Commitment revision authority:** How are normal pre-delivery changes,
    withdrawals, late arrivals, and approved retroactive changes selected for
    current and as-known views?
35. **Commitment Phase 2 scope and test coverage:** Which schedule states,
    daylight-saving behavior, maintenance exclusions, and revision scenarios
    will the synthetic data prove?
36. **Excess-order identity and grain:** What exactly does one approved-order
    record represent, and how is an order spanning several intervals expressed
    without creating allocation ambiguity?
37. **Excess-order authority and timing:** Which approval, publication, customer,
    delivery-point, contract, and interval conditions must be satisfied before
    additional delivery becomes billable?
38. **Excess-order quantity and billing meaning:** How are authorized additional
    `MWh_th`, missing orders, zero quantities, partial fulfillment, and
    unbilled excess represented?
39. **Excess-order revisions and cancellations:** How are duplicates, late
    versions, amendments, and cancellations selected without creating
    retroactive revenue after delivery?
40. **Excess-order Phase 2 scope and test coverage:** Which order states and
    billing scenarios will synthetic data prove, and which pricing or order
    workflow complexity remains deferred?
41. **Capacity-assessment identity and grain:** What exactly does one capacity
    record represent, and which identifiers distinguish an interval assessment
    from its source revisions?
42. **Capacity-assessment authority and completeness:** Which assessment states
    may make contractual availability final, and how are provisional,
    rejected, withdrawn, and missing assessments represented?
43. **Capacity-assessment quantity and plausibility:** Which `MWh_th` value,
    method, operating assumptions, nameplate ceiling, decimal, and validation
    rules must accompany the assessed capacity?
44. **Capacity-assessment revisions and relationships:** How are duplicates,
    late versions, corrections, approvals, and event-time delivery-point/site
    relationships selected for current and as-known views?
45. **Capacity-assessment Phase 2 scope and test coverage:** Which complete,
    derated, missing, corrected, and shared-capacity cases will synthetic data
    prove, and which asset-level derivation remains deferred?
46. **Elexon dataset and business meaning:** Which public Elexon dataset will
    Phase 2 ingest, what does one observation mean, and why is it relevant to
    the energy project without becoming part of the steam-delivery fact?
47. **Elexon identity and publication history:** Which fields identify one
    market observation and one published version when the source exposes
    publication time but no explicit revision number?
48. **Elexon batch extraction and recovery:** Which settlement-date and
    publication-time request modes, chunking, raw-evidence, retry, watermark,
    and idempotency rules govern backfills and later updates?
49. **Elexon time, units, and validation:** How are settlement date/period,
    UTC start time, GB daylight-saving days, fuel type, average `MW`, missing
    rows, and any derived `MWh_e` validated?
50. **Elexon licence, separation, and test coverage:** Which attribution and
    non-endorsement rules apply, where will the separate market fact live, and
    which publication, replay, missing-data, and daylight-saving cases must be
    tested?

Preserve these original questions unchanged. Record agreements only in the
Accepted decisions section and the source-contract decision log.

## Accepted decisions

### PSC-001 — Phase 2 physical source inventory

Accepted on 2026-08-27:

| Source dataset | Origin | What one source record represents | Phase 2 role |
|---|---|---|---|
| Customer master | Synthetic private data | One version of a customer identity | Supplies customer context and authorization scope |
| Industrial-site master | Synthetic private data | One version of an industrial site | Supplies site and reporting-location context |
| Delivery-point assignment | Synthetic private data | One effective assignment of a delivery point to a site and customer | Identifies where contractual steam delivery occurs and enforces one active point per site |
| Revenue-meter assignment | Synthetic private data | One effective version and assignment of a revenue meter | Identifies the authoritative meter for a delivery point and time |
| Contract terms | Synthetic private data | One effective revision of commercial terms | Supplies energy rate, SLA penalty rate, and applicable contract context |
| Commitment schedule | Synthetic private data | One revision of a delivery point's commitment for a 30-minute interval | Supplies the minimum promised thermal energy |
| Approved excess order | Synthetic private data | One revision of an order authorizing extra billable energy for specified intervals | Permits billing above the normal commitment without changing the SLA promise |
| Revenue-meter reading | Synthetic private data | One revision of a cumulative register reading at a meter boundary time | Supplies the authoritative boundaries used to derive official interval delivery |
| Delivery-point capacity assessment | Synthetic private data | One revision of assessed deliverable capacity for a delivery point and 30-minute interval | Supplies the accepted capacity used for contractual availability |
| Elexon electricity-market data | Real public API data | One API record at the selected Elexon dataset's published grain | Proves real batch ingestion and provides future electricity-market context |

Rules applying to this inventory:

- The first nine sources are explicitly synthetic because genuine private
  industrial plant, customer, contract, order, and meter records are not
  publicly available for this project.
- Elexon data remains separate from the steam-delivery fact in Phase 2. It
  describes the electricity market, not steam delivered to the customer.
- Date, interval, and data-status dimensions are derived warehouse structures,
  not additional source feeds.
- Each source receives its own detailed contract before its data is generated
  or ingested.
- Raw source records and every revision remain reproducible evidence; validated
  outputs apply the accepted business rules without destroying the raw input.

### PSC-002 — cumulative revenue-meter reading revision

Accepted on 2026-08-28:

> One source record represents one published revision of one cumulative
> register reading on one revenue-grade meter at one scheduled 30-minute UTC
> boundary.

The reading is a boundary event, not an interval total. A reading at 10:30
closes `[10:00, 10:30)` and also opens `[10:30, 11:00)`.

#### Identity and required content

- The logical reading identity is `meter_natural_id + register_natural_id +
  reading_at_utc`.
- `source_revision` is a positive, increasing integer that distinguishes the
  original from later versions of that same logical reading.
- `source_reading_revision_id` uniquely identifies one immutable published
  version, while a payload hash makes exact replay idempotent.
- Each revision carries the meter and register natural identifiers, boundary
  event time, cumulative value, native unit, revision number and type,
  publication time, source-system and schema identifiers, and raw-record
  locator.
- Corrections and reconciliations additionally carry the superseded revision,
  correction time, reason code, approver, and approval time.
- Platform ingestion time, payload hash, and raw object locator are captured in
  the ingestion envelope without replacing source-supplied values.
- Validated cumulative values use a fixed decimal representation rather than a
  binary floating-point type. The validated Iceberg precision is
  `decimal(20,6)`.
- All timestamps are timezone-aware UTC. `reading_at_utc` controls business
  attribution; publication, ingestion, correction, and approval times remain
  audit timestamps.

#### Register and unit rules

- Model energy and steam-mass registers as separate register readings.
- Only an accepted cumulative thermal-energy register is authoritative for
  official `MWh_th` delivery in Phase 2.
- Preserve the raw value and native unit. Calculate the delta in the native
  unit first, then apply a governed conversion to `MWh_th`.
- Accept controlled thermal-energy units such as `MWh_th`, `kWh_th`, and
  `GJ_th`; explicitly reject electrical `MWh_e` for steam delivery.
- A mass register in metric tonnes is supporting evidence and cannot silently
  replace the thermal-energy register.

#### Revision, validation, and exception rules

- Retain every raw revision. The greatest valid approved source revision is the
  current reading; a late older revision cannot replace it.
- An exact replay of the same revision and payload is harmless. The same
  revision identity with different content is a conflict and is quarantined.
- A withdrawal without a valid replacement leaves the boundary missing.
- Resolve the delivery point from the meter assignment that was effective at
  `reading_at_utc`; do not trust a delivery-point value copied into a reading.
- Require an exact UTC half-hour boundary, non-negative value, valid effective
  revenue-meter/register metadata, controlled unit, valid timestamp order, and
  exactly one event-time-valid delivery-point assignment.
- Validate consecutive boundaries on the same meter and register. An
  unexplained decrease or a delta above the effective meter's governed
  30-minute plausibility ceiling is quarantined.
- Phase 2 synthetic data does not generate resets, rollovers, meter
  replacements, or estimated/substituted official readings. If encountered,
  preserve and quarantine the record until an explicit reconciliation policy
  is introduced.
- A missing, withdrawn, or quarantined opening or closing boundary leaves
  delivered energy and dependent measures null and provisional, never zero.

#### Correction impact

Accepting or correcting a boundary recalculates both adjacent delivery
intervals where they exist, followed by their dependent shortfall, excess, SLA,
availability, revenue, penalty, completeness, and aggregate results. Batch and
stream processing must apply the same identity, revision-selection, validity,
and recalculation rules.

Example:

| Boundary | Revision | Accepted register | Treatment |
|---|---:|---:|---|
| 10:00 | 1 | 2,000.0 MWh_th | Opens the first interval |
| 10:30 | 1 | 2,004.7 MWh_th | Original shared boundary |
| 10:30 | 2 | 2,004.9 MWh_th | Approved correction; revision 1 remains auditable |
| 11:00 | 1 | 2,010.0 MWh_th | Closes the second interval |

After the correction, delivery for 10:00–10:30 changes from 4.7 to 4.9 MWh_th,
and delivery for 10:30–11:00 changes from 5.3 to 5.1 MWh_th. Total delivery
across the two intervals remains 10.0 MWh_th, matching accepted Scenario 3.

### PSC-003 — effective revenue-meter assignment

Accepted on 2026-08-28:

> One source record represents one published revision of one effective
> assignment episode connecting an authoritative revenue meter and register to
> one customer delivery point.

In plain English, this is the historical timetable that states which meter was
the official meter for a delivery point. The current assignment must never be
used to rewrite where an older reading belonged.

#### Identity and required content

- `meter_assignment_id` is the stable natural identifier for one assignment
  episode; `source_revision` distinguishes its immutable published versions.
- A new physical assignment receives a new `meter_assignment_id`. A correction
  to the dates or details of the same assignment receives a higher revision.
- Each revision carries the meter, register, and delivery-point natural
  identifiers; assignment role; register type and native unit; calibration
  identifier; governed maximum plausible 30-minute register change; effective
  start and end; revision type; publication time; approval state and time; and
  source-system and schema identifiers.
- Corrections and cancellations also carry the superseded revision, correction
  time, reason code, and approver.
- Ingestion time, payload hash, and raw object locator are captured in the
  ingestion envelope for idempotency and lineage.

#### Effective-time and authority rules

- Effective periods are half-open UTC ranges: `[effective_from_utc,
  effective_to_utc)`, with a null end meaning still effective.
- An assignment may authorize a delivery interval only when it covers the
  complete interval:

```text
effective_from_utc <= interval_start_utc
and
effective_to_utc is null or interval_end_utc <= effective_to_utc
```

- Fetch both the opening and closing cumulative readings from that assignment's
  same meter and register. Never independently map the two boundaries and
  subtract readings from different meters.
- Each committed delivery-point interval must have exactly one valid approved
  `authoritative_revenue` assignment.
- One meter/register cannot be authoritative for multiple delivery points at
  overlapping effective times.
- A gap leaves official delivery unknown and provisional. An overlap or
  conflicting relationship is quarantined; neither is resolved by falling back
  to SCADA or zero.

#### Revision, validation, and scope rules

- Retain every raw revision. The greatest valid approved source revision is
  current; exact replay is idempotent, and conflicting content under the same
  revision identity is quarantined.
- A retroactive correction requires an explicit reason and approval because it
  can move delivery, SLA, and revenue between customer scopes. Recalculate
  every affected interval while preserving the prior as-known view.
- Require non-null identifiers and effective start, a positive revision and
  plausibility ceiling, controlled role/type/unit codes, `from < to` when an
  end exists, and valid referenced delivery-point, meter, register, and
  calibration metadata.
- Effective boundaries must align to UTC half-hour boundaries during Phase 2.
- Phase 2 generated test periods use one stable meter assignment throughout
  the period. Meter replacements and the special handoff readings they require
  remain deferred; if encountered, preserve and quarantine them pending an
  explicit reconciliation policy.

Example:

| Assignment | Meter/register | Delivery point | Effective period |
|---|---|---|---|
| MA-001 | RM-001 / ENERGY-01 | DP-001 | 2026-08-01 00:00 UTC onward |

Delivery for 10:00–10:30 uses RM-001/ENERGY-01 at both 10:00 and 10:30. A
reading from another meter cannot be mixed into that delta even if it was
published later.

### PSC-004 — effective delivery-point assignment

Accepted on 2026-08-28:

> One source record represents one published revision of one effective
> assignment episode connecting one customer delivery point to one industrial
> site and one customer.

In plain English, this is the historical timetable that answers: where did the
delivery point belong at this event time, and which customer was authorized to
see its results?

#### Identity and required content

- `delivery_point_assignment_id` is the stable natural identifier for one
  assignment episode; `source_revision` distinguishes its immutable published
  versions.
- A new relationship episode receives a new assignment identifier. A
  correction to the dates or details of the same episode receives a higher
  revision.
- Each revision carries the delivery-point, site, and customer natural
  identifiers; delivery-point name; service type; effective start and end;
  revision type; publication time; approval state and time; and source-system
  and schema identifiers.
- Corrections and cancellations also carry the superseded revision, correction
  time, reason code, and approver.
- Ingestion time, payload hash, and raw object locator are captured in the
  ingestion envelope for idempotency and lineage.

#### Relationship and effective-time rules

- Effective periods are half-open UTC ranges: `[effective_from_utc,
  effective_to_utc)`, with a null end meaning still effective.
- The assignment must cover the complete 30-minute delivery interval.
- A customer may have several sites.
- During Phase 2, a site belongs to one customer and has exactly one active
  delivery point at any event time.
- A delivery point belongs to one site and customer at any event time.
- Back-to-back relationships ending and beginning at the same boundary are not
  an overlap.
- A gap leaves site/customer attribution unresolved and the affected delivery
  provisional. Never copy the nearest relationship forward or backward.
- Overlapping or conflicting customer/site relationships are quarantined.

#### History, security, and validation rules

- Customer and site scope are derived from the event-time assignment. A
  customer identifier copied into another payload or supplied by a browser is
  not trusted as authorization evidence.
- Unresolved or conflicting attribution is denied customer access by default.
- Retain every raw revision. The greatest valid approved source revision is
  current; exact replay is idempotent, and conflicting content under the same
  revision identity is quarantined.
- A retroactive correction requires an explicit reason and approval because it
  can change dimensional attribution and customer visibility. Recalculate
  affected facts and access scope while preserving the prior as-known view.
- Require non-null identifiers and effective start, positive revision,
  controlled service and revision types, `from < to` when an end exists,
  UTC half-hour-aligned boundaries, approved status, and valid referenced
  customer and site master records.
- Phase 2 generated test periods keep delivery-point, site, and customer
  relationships stable. Reassignment scenarios remain deferred.

Example:

| Assignment | Delivery point | Site | Customer | Effective period |
|---|---|---|---|---|
| DPA-001 | DP-001 | SITE-001 | CUST-001 | 2026-08-01 00:00 UTC onward |

Every accepted delivery interval in the test period inherits SITE-001 and
CUST-001 through DPA-001. If that relationship is missing or ambiguous, the
pipeline preserves the measurements but does not guess their customer scope.

### PSC-005 — effective customer-master version

Accepted on 2026-08-28:

> One source record represents one published revision of one effective version
> of one customer business entity.

The durable customer identity is separate from descriptive information that
can change. A legal-name change creates a new historical business version;
correcting a misspelling creates a new source revision of the affected version.

#### Identity and required content

- `customer_natural_id` permanently identifies the business entity, remains
  stable across descriptive changes, and is never reused.
- `customer_version_id` identifies one effective business version;
  `source_revision` distinguishes its immutable published revisions.
- Each version carries legal name, display name, industry-sector code, country
  code, lifecycle status, tenant-authorization scope identifier, effective
  start and end, revision type, publication time, approval state and time, and
  source-system and schema identifiers.
- Corrections and cancellations also carry the superseded revision, correction
  time, reason code, and approver.
- Ingestion time, payload hash, and raw object locator are captured in the
  ingestion envelope for idempotency and lineage.

#### Business-version and correction rules

- A genuine business change creates a new `customer_version_id` with a new
  effective period while retaining the same `customer_natural_id`.
- Correcting erroneous data about an already-declared effective version creates
  a higher `source_revision` under the same `customer_version_id`.
- Effective periods are half-open UTC ranges and valid accepted versions for a
  customer must not overlap.
- The warehouse customer dimension preserves each effective business version;
  older delivery facts continue to reference the version valid at their event
  time.
- A genuinely different legal entity receives a new customer natural
  identifier rather than inheriting an old one.

#### Authorization, lifecycle, and validation rules

- During Phase 2, each customer has one immutable, one-to-one
  `tenant_authorization_scope_id`. Neither customer nor tenant-scope identifiers
  may be reassigned to another entity.
- Customer access is derived through the accepted event-time delivery-point
  assignment and this tenant scope, never from a customer identifier supplied
  by a browser or copied into a measurement payload.
- A missing, inactive-at-event-time, overlapping, or conflicting customer
  version leaves dependent attribution invalid and customer access denied by
  default.
- Deactivation creates an effective-dated lifecycle version; it does not
  physically delete historical identity or facts.
- Retain every raw revision. The greatest valid approved revision of each
  effective version is current; exact replay is idempotent, and conflicting
  content under the same revision identity is quarantined.
- Require non-null stable/version identifiers, names, scope identifier,
  effective start, positive revision, controlled country/industry/status
  codes, `from < to` when an end exists, and valid timestamp ordering.
- Synthetic customer records contain no personal names, email addresses,
  telephone numbers, or other personal contact information.

#### Phase 2 history test

Generate one approved legal-name change. Deliveries before the effective
boundary resolve to the old customer-dimension version; deliveries from that
boundary onward resolve to the new version. Separately, a correction such as a
spelling repair revises the applicable source version without pretending that
the misspelling was a genuine business era.

### PSC-006 — effective industrial-site-master version

Accepted on 2026-08-28:

> One source record represents one published revision of one effective
> descriptive version of one synthetic industrial site.

The durable site identity represents a physical industrial location. Its name,
reporting attributes, or operational status can change without changing which
physical place it is.

#### Identity and required content

- `site_natural_id` permanently identifies one physical industrial location
  and is never reused.
- `site_version_id` identifies one effective descriptive version;
  `source_revision` distinguishes its immutable published revisions.
- Each version carries site name, locality, postal area, country code,
  controlled region code, IANA timezone name, operational status, optional
  synthetic latitude/longitude, effective start and end, revision type,
  publication time, approval state and time, and source-system and schema
  identifiers.
- Corrections and cancellations also carry the superseded revision, correction
  time, reason code, and approver.
- Ingestion time, payload hash, and raw object locator are captured in the
  ingestion envelope for idempotency and lineage.

#### Site identity, history, and timezone rules

- Effective periods are half-open UTC ranges and valid accepted versions for a
  site must not overlap.
- A genuine site rename, reporting-attribute change, or lifecycle change
  creates a new `site_version_id` while retaining the same natural identity.
- Correcting erroneous data about an existing effective version creates a
  higher `source_revision` under the same version identifier.
- A genuinely different physical location receives a new `site_natural_id`;
  an old site identity is not moved to new coordinates.
- All Phase 2 sites are explicitly synthetic GB locations with `country_code =
  GB` and an approved controlled region code.
- The governing timezone is the IANA value `Europe/London`, not fixed `GMT` or
  `BST`. It supplies the historically correct daylight-saving behavior.
- Every delivery-point assignment and delivery interval must resolve to exactly
  one event-time-valid site version.

#### Validation, visibility, and authority rules

- Require non-null site/version identifiers, name, country, region, timezone,
  status, effective start, approval state, and positive revision; require
  `from < to` when an end exists and valid coordinates when supplied.
- Missing, inactive-at-event-time, overlapping, or conflicting site context
  leaves dependent delivery attribution invalid and quarantined.
- Retain every raw revision. The greatest valid approved revision of each
  effective version is current; exact replay is idempotent, and conflicting
  content under the same revision identity is quarantined.
- Customer ownership is not authoritative in the site master. It comes from
  PSC-004's event-time delivery-point assignment, preventing two sources from
  competing over customer authorization.
- Customer-facing outputs may expose the assigned site's approved name,
  locality, region, and timezone. Exact coordinates, detailed addresses,
  internal site codes, and other customers' sites remain internal.
- Every generated site and location is labelled synthetic.

#### Phase 2 history test

Generate one approved site-name change and one locality correction. The rename
creates a new event-time dimension version; the correction produces a higher
source revision of the affected version. Both continue to use
`Europe/London`, and the as-known view can reproduce the earlier published
record.

### PSC-007 — effective contract terms

Accepted on 2026-08-28:

> One source record represents one published revision of one effective
> commercial-terms episode for one contract and customer delivery point.

Contract terms state the applicable price and penalty rules. They do not state
the quantity of steam promised in an interval; that belongs to the separate
commitment schedule.

#### Identity and required content

- `contract_natural_id` identifies the continuing agreement.
- `contract_terms_version_id` identifies one effective commercial-terms
  episode; `source_revision` distinguishes its immutable published revisions.
- Each version carries the contract, delivery-point, and customer natural
  identifiers; effective start and end; energy rate; SLA shortfall-penalty
  rate; ISO currency code; revision/amendment type; publication time; approval
  state and time; and source-system and schema identifiers.
- Corrections and retroactive amendments also carry the superseded revision,
  correction or agreement identifier, reason code, correction time, and
  approver.
- Ingestion time, payload hash, and raw object locator are captured in the
  ingestion envelope for idempotency and lineage.

#### Financial representation and formulas

- `energy_rate_gbp_per_mwh_th` and
  `sla_penalty_rate_gbp_per_mwh_th` are non-negative fixed
  `decimal(18,6)` values.
- Phase 2 accepts only ISO currency code `GBP`.
- Retain decimal precision throughout row-level and aggregate calculations;
  round only customer-presented totals to two decimal places.
- Approved excess energy inherits the effective energy rate.
- Store the SLA penalty as a positive deduction:

```text
gross_earned_revenue_gbp = billable_mwh_th * energy_rate_gbp_per_mwh_th
accrued_sla_penalty_gbp  = shortfall_mwh_th * sla_penalty_rate_gbp_per_mwh_th
net_earned_revenue_gbp   = gross_earned_revenue_gbp - accrued_sla_penalty_gbp
```

#### Applicability and completeness rules

- Effective periods are half-open UTC ranges.
- A contract version applies only when it covers the complete delivery
  interval:

```text
effective_from_utc <= interval_start_utc
and
effective_to_utc is null or interval_end_utc <= effective_to_utc
```

- Each committed delivery interval must resolve to exactly one valid approved
  terms version.
- The terms record's customer must match PSC-004's event-time customer
  assignment for the delivery point. A contract payload cannot override
  customer authorization.
- Overlapping or conflicting accepted terms versions are quarantined.
- Missing contract coverage leaves revenue and penalty unknown/provisional; it
  does not invalidate physical delivery, commitment, shortfall, or SLA
  quantities.

#### Amendment, correction, and history rules

- A genuinely renegotiated rate or penalty creates a new
  `contract_terms_version_id` with a new effective period.
- Correcting erroneous dates or values in an existing terms episode creates a
  higher `source_revision` under the same version identifier.
- Normal amendments must be approved and published before becoming effective.
- A retroactive amendment after delivery requires an explicit agreement
  identifier, effective date, reason, approver, and approval time.
- An approved retroactive amendment or correction recalculates affected
  current revenue and penalty while preserving every source revision and the
  previous as-known view.
- The greatest valid approved revision of each effective version is current;
  exact replay is idempotent, and conflicting content under the same revision
  identity is quarantined.

#### Phase 2 scope and example

Phase 2 models only variable energy revenue per billable `MWh_th` and an SLA
penalty per shortfall `MWh_th`, with one contract applying to one delivery point
at a time. Commitments remain in their own schedule source. Fixed or capacity
charges, tiers, indexation, discounts, taxes, minimum bills, currency
conversion, invoicing, settlement, and cash collection remain deferred.

| Billable delivery | Shortfall | Energy rate | Penalty rate | Gross | Penalty | Net |
|---:|---:|---:|---:|---:|---:|---:|
| 4.8 MWh_th | 0.2 MWh_th | GBP 55/MWh_th | GBP 120/MWh_th | GBP 264 | GBP 24 | GBP 240 |

### PSC-008 — revisioned 30-minute commitment schedule

Accepted on 2026-08-28:

> One source record represents one published revision of the base minimum
> thermal-energy commitment for one customer delivery point and one exact
> 30-minute UTC interval.

The record states how much thermal energy the operator promised for the
interval. It does not state deliverable capacity, price/penalty terms, or an
optional excess order.

#### Identity and required content

- The logical identity is `delivery_point_natural_id + interval_start_utc`.
- `source_commitment_revision_id` uniquely identifies one immutable published
  version; `source_revision` is a positive increasing version number for the
  logical commitment.
- Each revision carries the delivery-point, customer, and contract natural
  identifiers; interval start and end; obligation status; committed quantity
  and unit; revision type; publication time; approval state and time; and
  source-system and schema identifiers.
- Corrections, withdrawals, and retroactive changes also carry the superseded
  revision, correction time, reason code, approver, and approval reference.
- Ingestion time, payload hash, and raw object locator are captured in the
  ingestion envelope for idempotency and lineage.
- `committed_mwh_th` uses fixed `decimal(20,6)` and the only accepted canonical
  source unit for Phase 2 is `MWh_th`; electrical units are rejected.

#### Time, relationship, and applicability rules

- Intervals are half-open `[interval_start_utc, interval_end_utc)`, exactly 30
  minutes, and aligned to UTC half-hour boundaries.
- Customer-facing dates use `Europe/London`, but the UTC interval key keeps
  repeated local times distinct during the autumn clock change.
- Do not force 48 commitments into every local day: an operational day may
  contain 46, 48, or 50 real half-hour intervals.
- The interval must be completely covered by one valid PSC-004
  delivery-point/customer assignment and one applicable PSC-007 contract.
- A copied customer identifier is validation evidence only; PSC-004 remains
  authoritative for customer attribution and authorization.
- An approved excess order does not change this base commitment.

#### Positive, no-commitment, and missing states

- `obligation_status = committed` requires `committed_mwh_th > 0`.
- `obligation_status = no_commitment` requires an explicit accepted record
  with `committed_mwh_th = 0`; SLA and contractual availability are then not
  applicable for that interval.
- Approved maintenance may remove the obligation only through an explicit
  `no_commitment` schedule revision carrying an approved-maintenance reason and
  approval reference. Detailed maintenance evidence remains a later process.
- A missing, withdrawn, invalid, or quarantined record is unknown, not zero and
  not `no_commitment`.
- A withdrawal must be replaced with an explicit `no_commitment` revision when
  the business intends to remove the obligation.
- Missing schedule coverage fails commitment completeness and prevents final
  commitment-dependent metrics.

#### Revision selection and recalculation rules

- Retain every raw revision. The greatest valid approved revision is the
  current commitment; an older late revision cannot replace it.
- Exact replay of the same revision and payload is idempotent. Different
  content under one revision identity is quarantined.
- Normal changes must be approved and published before the interval begins.
- A post-delivery or retroactive change requires an explicit reason and
  approval. Once accepted, it recalculates current shortfall, SLA, billable
  energy, revenue, penalty, completeness, and aggregates while preserving the
  original versions and as-known view.
- Multiple accepted current versions for one logical commitment are invalid.
- Require non-null identity/interval fields, positive revision, controlled
  statuses, valid timestamp order, relationship and contract coverage, and
  status/quantity consistency.

#### Phase 2 scope, example, and tests

Phase 2 models one base minimum commitment per delivery point and interval.
Any broader source schedule is expanded into exact interval records before
validation. Daily obligations, rolling windows, ramps, take-or-pay rules,
capacity promises, priorities, multi-point allocation, and customer nomination
workflows remain deferred.

| Revision | Status | Commitment | Published | Treatment |
|---|---|---:|---|---|
| 1 | committed | 5.0 MWh_th | Before delivery | Original accepted schedule |
| 2 | committed | 5.5 MWh_th | After delivery | Current only after explicit retroactive approval |

With delivery of 5.2 MWh_th, revision 1 produces no shortfall in the as-known
view. After revision 2 is approved, the current view produces a 0.3 MWh_th
shortfall. An explicit `no_commitment / 0.0 MWh_th` record instead makes the
interval not applicable; no record leaves the obligation unknown.

Executable tests cover original versus approved retroactive revision, missing
versus explicit no-commitment, approved-maintenance exclusion, exact duplicate
versus conflicting payload, late older revision, dependent recalculation, and
46- and 50-interval daylight-saving days with unique UTC keys.

### PSC-009 — revisioned approved excess-order allocation

Accepted on 2026-08-28:

> One source record represents one published revision of one approved
> excess-order allocation for one customer delivery point and one exact
> 30-minute UTC interval.

The allocation authorizes how much additional delivery may become billable
above the base commitment during that interval. It does not increase the SLA
commitment.

#### Order identity, interval grain, and required content

- `excess_order_natural_id` identifies the customer's overall business order.
- `order_interval_line_id` identifies one explicit interval allocation within
  that order; `source_revision` distinguishes its immutable published versions.
- An order spanning several intervals must provide one allocation line with an
  explicit quantity for every interval. The platform never divides a pooled
  total automatically.
- Each revision carries order and line identifiers; delivery-point, customer,
  and contract natural identifiers; interval start and end; approved extra
  quantity and unit; order state; request, approval, and publication times;
  revision type and number; approver; and source-system and schema identifiers.
- Cancellations and corrections also carry the superseded revision, reason
  code, and correction time.
- Ingestion time, payload hash, and raw object locator are captured in the
  ingestion envelope for idempotency and lineage.
- `approved_extra_mwh_th` uses non-negative fixed `decimal(20,6)` and canonical
  unit `MWh_th`.

#### Approval cutoff and revision rules

- An allocation is eligible only when it is both approved and published before
  `interval_start_utc`.
- Select the greatest valid revision that satisfied that cutoff. An older late
  revision cannot replace a newer eligible version.
- A valid cancellation published before the cutoff removes authorization.
- Revisions, approvals, increases, reductions, or cancellations published
  after the interval begins remain audit evidence but cannot create, change,
  or remove Phase 2 revenue retroactively.
- Exact replay of the same revision and payload is idempotent. Conflicting
  content under one revision identity is quarantined.
- Phase 2 permits at most one current eligible allocation per delivery point
  and interval; stacked orders remain deferred.

#### Missing, quantity, relationship, and expiry rules

- No eligible approved allocation means `approved_extra_mwh_th = 0`. This is
  not provisional because excess billing is an explicit exception requiring
  affirmative authorization.
- An approved allocation must be greater than zero; a valid cancellation means
  zero authorization.
- Unused approved quantity expires with its interval and cannot move to another
  interval. Phase 2 does not charge for unused authorized extra energy.
- The interval must be exactly 30 minutes and aligned to a UTC half-hour
  boundary.
- PSC-004 must assign the delivery point to the order's customer for the full
  interval; PSC-007 must supply an effective contract and energy rate; PSC-008
  must supply a positive base commitment.
- Copied customer or contract identifiers are validation evidence and cannot
  override the authoritative relationships.
- Relationship mismatch, invalid unit, negative quantity, overlap, or multiple
  current allocations for one delivery point/interval is quarantined.
- The order carries no separate rate or currency. Approved extra energy
  inherits PSC-007's effective contract energy rate.

#### Billing rule, example, and Phase 2 scope

```text
billable_mwh_th = min(
    delivered_mwh_th,
    committed_mwh_th + approved_extra_mwh_th
)

unbilled_excess_mwh_th = max(
    delivered_mwh_th - committed_mwh_th - approved_extra_mwh_th,
    0
)
```

Shortfall and SLA continue to use only the base commitment.

| Base commitment | Approved extra | Delivered | Billable | Unbilled excess | Rate | Gross revenue |
|---:|---:|---:|---:|---:|---:|---:|
| 5.0 MWh_th | 0.4 MWh_th | 5.6 MWh_th | 5.4 MWh_th | 0.2 MWh_th | GBP 50/MWh_th | GBP 270 |

Daily pooled quantities, unused-allocation transfer, multiple stacked orders,
premium pricing, take-or-pay treatment, standalone spot sales, and post-start
amendments remain deferred. Executable tests cover no order versus eligible
order, explicit multi-interval lines, cutoff timing, pre/post-cutoff
cancellation, duplicates and conflicts, late older versions, relationship
mismatch, unused expiry, and unchanged SLA alongside increased billable energy.

### PSC-010 — revisioned delivery-point capacity assessment

Accepted on 2026-08-28:

> One source record represents one published revision of assessed deliverable
> thermal capacity for one customer delivery point and one exact 30-minute UTC
> interval.

The assessment answers how much thermal energy the delivery point could have
supplied during the interval if the customer needed it. It measures ability to
deliver, not actual delivery.

#### Identity and required content

- The logical identity is `delivery_point_natural_id + interval_start_utc`.
- `source_capacity_revision_id` uniquely identifies one immutable published
  version; `source_revision` is a positive increasing version number for the
  logical assessment.
- Each revision carries delivery-point identifier; interval start and end;
  assessment status; nameplate ceiling, operational restriction, and assessed
  deliverable capacity; assessment method and version; high-level reason code;
  revision number and type; publication, approval, and finalization times; and
  source-system and schema identifiers.
- Corrections and withdrawals also carry the superseded revision, correction
  time, reason code, and approver.
- Ingestion time, payload hash, and raw object locator are captured in the
  ingestion envelope for idempotency and lineage.
- Capacity quantities use non-negative fixed `decimal(20,6)` and canonical
  unit `MWh_th`.

#### Phase 2 assessment method and plausibility

Use one transparent synthetic method:

```text
deliverable_capacity_mwh_th = max(
    nameplate_ceiling_mwh_th - operational_restriction_mwh_th,
    0
)
```

- Nameplate capacity is a plausibility ceiling, not a substitute for an
  interval assessment.
- Deliverable capacity must not exceed the nameplate ceiling, and all three
  quantities must be internally consistent with the declared method version.
- Controlled high-level reason codes include `normal`, `derated`,
  `planned_restriction`, and `unavailable`.
- Detailed derivation from assets, outages, state of charge, operating modes,
  steam-train limits, and delivery constraints remains deferred to Phase 4.

#### Final, provisional, zero, and missing states

- Only a valid approved `final` revision contributes to official contractual
  availability.
- A `provisional` revision may support an explicitly provisional operational
  view but cannot silently become final.
- A valid final assessment of zero is authoritative evidence of zero
  deliverable capacity.
- No final assessment, or an approved withdrawal without replacement, leaves
  capacity and availability unknown—not zero and not 100%.
- An explicit no-commitment interval makes availability not applicable even
  when capacity is known.

#### Revision, relationship, and availability rules

- Retain every raw revision. The greatest valid approved `final` revision is
  the current official assessment; a later provisional revision cannot replace
  an accepted final revision.
- Exact replay of the same revision and payload is idempotent. Conflicting
  content under one revision identity is quarantined, and an older late
  revision cannot replace a newer accepted one.
- An approved correction recalculates current availability and affected
  aggregates while preserving prior revisions and the as-known view.
- The interval must be exactly 30 minutes, aligned to a UTC half-hour, and
  completely covered by PSC-004's delivery-point/site/customer assignment.
- Under SC-002, each site has one active delivery point during the Phase 2 data
  window, so the site's assessed capacity belongs entirely to that point.
- Capacity is never copied to multiple delivery points or divided implicitly.
  Relationship gaps, conflicts, or attempts to assign the same site capacity
  to multiple points are quarantined.
- Calculate portfolio availability only from final accepted assessments over
  applicable commitments, using interval-level capping and weighted totals:

```text
contractual_availability_pct =
    100 * sum(min(deliverable_capacity_mwh_th, committed_mwh_th))
        / sum(committed_mwh_th)
```

Never average interval availability percentages.

#### Example and executable tests

| Revision | Status | Ceiling | Restriction | Capacity | Treatment |
|---|---|---:|---:|---:|---|
| 1 | provisional | 7.0 | 1.5 | 5.5 MWh_th | Operational estimate only |
| 2 | final | 7.0 | 1.5 | 5.5 MWh_th | Official assessment |
| 3 | final | 7.0 | 2.5 | 4.5 MWh_th | Approved correction |

For a 5.0 MWh_th commitment, revision 2 gives 100% contractual availability.
After revision 3 is accepted, the current view gives 90%, while the earlier
as-known result remains reproducible.

Executable tests cover provisional-only versus final, final zero versus
missing, capacity above nameplate, inconsistent derivation, approved
correction, duplicate versus conflicting payload, no-commitment applicability,
relationship gaps, and attempted shared-capacity duplication.

### PSC-011 — Elexon FUELHH external batch sidecar

Accepted on 2026-08-28:

> Phase 2 will ingest Elexon's public FUELHH half-hourly Great Britain
> generation-by-fuel dataset solely as a real external batch-pipeline
> demonstration. It is not an input to the steam-delivery business result.

#### Purpose and explicit business boundary

FUELHH answers which fuel types were generating electricity across Great
Britain during each settlement interval. It does not answer whether contractual
steam was delivered, why a site missed its commitment, what capacity the site
had, or what steam revenue and penalties were earned.

Its Phase 2 purpose is to prove unauthenticated external-API extraction,
date-range backfill, raw evidence on R2, publication-aware idempotency, and a
validated Iceberg output using genuine energy-industry data.

- Keep FUELHH separate from the steam-delivery fact and mart.
- Do not use it to calculate Phase 2 steam SLA, availability, revenue, penalty,
  electricity cost, or carbon emissions.
- Do not copy a national value onto every customer/site row, which would
  duplicate the observation and imply an unsupported relationship.
- Revisit its business use only during a future thermal-battery charging
  process, alongside actual site charging `MWh_e`, price/tariff evidence, and
  an accepted carbon methodology.

#### Official source and record meaning

- Endpoint: `GET
  https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELHH`.
- The current Insights API is public and requires no API key.
- One source row represents Elexon's published average GB electricity
  generation for one fuel type during one half-hour settlement interval.
- Source fields are `dataset`, `publishTime`, `startTime`, `settlementDate`,
  `settlementPeriod`, `fuelType`, and `generation`.
- The official endpoint contract is documented at
  <https://bmrs.elexon.co.uk/api-documentation/endpoint/datasets/FUELHH>.

#### Identity and publication-history interpretation

- Logical observation key: `dataset + start_time_utc + fuel_type`.
- Published-version key: the logical key plus `publish_time_utc`.
- Elexon exposes publication time but no explicit source revision number.
  Preserve every distinct publication and use the greatest publication time as
  the project's current-view rule.
- This publication-time precedence is an explicit project interpretation, not
  a claim that Elexon guarantees revision ordering semantics.
- An as-known view selects the greatest publication time at or before its
  cutoff. Exact replay is idempotent; different content under the same
  published-version key is quarantined.
- Capture retrieval time, canonical request URL and parameters, HTTP status,
  raw-response payload hash, and R2 raw-object locator.

#### Batch extraction and recovery policy

- Airflow requests one settlement date per task for deterministic historical
  backfills and stores the untouched JSON response before parsing it.
- A separate publication-time-watermark path captures later publications and
  corrections.
- Settlement-date filters and publication-time filters are never combined in
  one request because the endpoint contract prohibits that combination.
- Daily request chunks are a project reliability policy, not a claimed Elexon
  endpoint limit. Retry HTTP `429` and transient `5xx` responses with bounded
  exponential backoff and preserve failed-request evidence.
- A rerun of the same date range must reproduce the same raw-request identity
  and merge published versions idempotently rather than append duplicates.

#### Time, units, missing-data, and validation rules

- Require `dataset = FUELHH`, UTC `startTime` and `publishTime`, controlled fuel
  type, settlement date/period, and numeric generation.
- Preserve `generation` as the signed source average power value in `MW`; do
  not assume that every valid value must be non-negative.
- If interval electrical energy is later useful, derive it explicitly as
  `generation_mwh_e = generation_mw * 0.5 hours`. Never relabel `MW` as energy
  or mix `MWh_e` with steam `MWh_th`.
- Validate settlement date and period against UTC start time and the GB
  settlement calendar, including 46-, 48-, and 50-period daylight-saving days.
- A missing fuel/interval observation remains missing, not zero.
- Preserve unknown newly introduced fuel codes in raw data and quarantine them
  from validated outputs until the controlled reference set is reviewed.

#### Licence, attribution, and tests

Published outputs must include:

> Contains BMRS data © Elexon Limited copyright and database right 2026.

Where practical, link to
<https://www.elexon.co.uk/bsc/data/balancing-mechanism-reporting-agent/copyright-licence-bmrs-data/>.
Do not imply Elexon endorsement or misrepresent the source.

Executable tests cover raw-response preservation, rerun idempotency, a newer
publication, a conflicting published-version payload, missing observations,
unknown fuel codes, `MW` versus derived `MWh_e`, 46/50-period settlement days,
and confirmation that no FUELHH field enters the steam-delivery mart.

## Status

PSC-001 through PSC-011 are accepted. All nine synthetic-source contracts and
the separate real Elexon batch-sidecar contract are agreed. The source-contract
workshop is complete. The machine-readable Draft 2020-12 schemas, deterministic
nine-source generator, hashed manifest, and executable source/scenario tests
are implemented. Bulk output is generated only into a caller-selected ignored
directory and is not committed. Raw R2 evidence and validated Iceberg loads are
the next Phase 2 implementation step.
