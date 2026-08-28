# Workshop 1 — choose the business process

## Goal

Choose the first measurable business process before naming fact and dimension tables.

## Accepted first process

**Steam-delivery performance against a customer commitment.**

Read the
[plain-English steam-delivery explainer](02-steam-delivery-business-process-explainer.md)
before making grain or source-authority decisions.

Why start here:

- It connects operations to a clear customer outcome.
- It requires telemetry/meter, site, customer, contract, time, and grid context without requiring the whole company model.
- It gives availability, cost, carbon, revenue, and AI investigation meaningful downstream uses.
- It can be demonstrated in both a live alert path and a historical reconciliation path.

## Decision we want to support

For an operator:

> Is this site on track to meet its current steam commitment, and if not, what should I investigate?

For historical analysis:

> Did the site meet the commitment, what caused any shortfall, and what were the commercial consequences?

## Candidate source events

- Revenue-grade steam-meter interval
- SCADA steam-flow and pressure observations
- Delivery commitment interval or schedule
- Plant operating-mode and availability events
- Contract/tariff effective period
- Market-price and carbon-intensity interval

## Accepted decisions

### DM-003 — authoritative delivery source

Accepted as **DM-003** on 2026-08-27:

- The revenue-grade steam meter is authoritative for official delivered steam.
- SCADA measurements provide a provisional live estimate and diagnostic
  evidence; they do not silently become the contractual measurement.
- Billing records calculate the financial consequence from physical delivery
  and contract rules; they do not redefine the delivered quantity.
- If the authoritative meter reading is missing, the result remains explicitly
  provisional.
- If a corrected meter reading arrives, retain the original and correction for
  auditability while reporting the latest accepted version.

### DM-002 — fact grain

Accepted as **DM-002** on 2026-08-27:

> One row represents steam delivered through one customer delivery point during
> one non-overlapping 30-minute interval.

- A delivery point is the meter/location where steam passes from the operator
  to the customer.
- The fixed interval supports consistent comparison and additive rollups.
- High-frequency SCADA observations remain a separate operational process for
  live monitoring and investigation.
- A corrected meter reading changes the accepted measurement, not the grain of
  the underlying business event; all source versions remain auditable.

### DM-011 — official delivery event

Accepted as **DM-011** on 2026-08-27:

> Official steam delivered during an interval is the accepted closing
> cumulative revenue-meter register minus the accepted opening cumulative
> register from the same meter.

- The delta is first calculated in the meter register's native unit and then
  normalized under the accepted DM-004 measurement policy.
- Integrated SCADA flow remains a provisional live estimate and diagnostic
  measurement, not the official contractual quantity.
- Billing adjustments remain separate commercial records and do not change the
  physical delivery event.
- A missing boundary reading, unexplained negative delta, reset, rollover, or
  physically impossible delta is not converted to zero. The interval remains
  provisional and the suspect reading is quarantined until it is corrected or
  explicitly reconciled.

### DM-005 — commitment definition

Accepted as **DM-005** on 2026-08-27:

> Each customer delivery point has a scheduled minimum thermal-energy
> commitment for every 30-minute interval.

- An interval is met when accepted delivered thermal energy is greater than or
  equal to that interval's applicable commitment.
- Interval shortfall is `max(committed quantity - delivered quantity, 0)`.
- Delivering extra steam in a later interval does not erase an earlier
  shortfall.
- Daily commitment and delivery figures are additive summaries of the
  30-minute intervals, not separate contractual obligations in the first
  release.
- A capacity promise measures ability to deliver rather than actual delivery;
  it belongs to a later availability process.

### DM-004 — canonical measurement units

Accepted as **DM-004** on 2026-08-27:

- Use megawatt-hours thermal (`MWh_th`) as the canonical unit for steam-delivery
  commitments, delivered thermal energy, and shortfalls.
- Retain steam mass in metric tonnes (`t`) as a supporting operational and
  customer measure.
- Preserve every source value and its original unit before normalization.
- Label thermal and electrical energy explicitly as `MWh_th` and `MWh_e` so
  they cannot be silently combined.
- Use the revenue meter's energy register or a validated calculation based on
  steam conditions. Do not use an ungoverned fixed mass-to-energy conversion.

### DM-008 — timezone and operational-day rule

Accepted as **DM-008** on 2026-08-27:

- The delivery interval's event time controls business reporting. Publication,
  ingestion, and correction times never move delivery into a different period.
- Store interval start and end in UTC and model each interval as `[start, end)`:
  the start is included and the end is excluded.
- Derive customer-facing dates and times using the IANA `Europe/London`
  timezone, which handles GMT and BST changes.
- Define the operational day as local midnight to the next local midnight.
- Preserve publication, ingestion, and correction timestamps separately for
  source lineage, freshness, and audit purposes.
- A local day may contain 46, 48, or 50 half-hour intervals around daylight-
  saving changes. Do not manufacture or collapse intervals to force 48.

### DM-009 — late and corrected record policy

Accepted as **DM-009** on 2026-08-27:

- The accepted business result is uniquely identified by customer delivery
  point and interval start time, consistent with DM-002.
- Repeated delivery of the same source reading does not create another accepted
  business result.
- A late reading belongs to its original event-time interval and triggers
  recalculation of that interval and affected summaries.
- Preserve every raw revision. The latest valid source revision becomes the
  current accepted measurement; an older late revision cannot replace a newer
  accepted revision.
- A missing authoritative reading leaves delivered quantity and shortfall
  unknown. Do not convert missing delivery to zero or infer a contractual miss.
- Quarantine unexplained negative deltas, resets, rollovers, physically
  impossible values, and conflicting records that claim the same revision.
- Batch reconciliation and streaming processing apply the same identity,
  revision, and validity rules and must converge on the same accepted result.

### DM-006 — availability definition

Accepted as **DM-006** on 2026-08-27:

> Availability is the capacity-weighted ability of the service to provide the
> committed thermal energy during an obligated interval.

- For an interval with a positive commitment, availability is capped at 100%
  and compares deliverable thermal-energy potential with committed thermal
  energy.
- Partial derating reduces availability even when the plant remains running;
  simple on/off uptime is therefore insufficient.
- Extra capacity in one interval cannot compensate for unavailability in
  another interval.
- Planned maintenance leaves the denominator only when the contract explicitly
  approves the exclusion and the applicable commitment is adjusted. Internal
  planning alone does not remove a customer obligation.
- An interval with no applicable commitment has availability status `not
  applicable`, not 100%.

### DM-007 — revenue definition

Accepted as **DM-007** on 2026-08-27:

> Revenue in the steam-delivery performance process means earned/accrued revenue
> attributable to the delivery interval, not an invoice or cash receipt.

- Billable delivery is the lesser of accepted delivered thermal energy and the
  applicable commitment unless an approved order authorizes excess delivery.
- Gross earned revenue equals billable `MWh_th` multiplied by the contract rate
  effective during the interval.
- Accrued SLA penalty equals accepted shortfall `MWh_th` multiplied by the
  applicable penalty rate and is stored as a separate positive monetary amount.
- Net earned revenue equals gross earned revenue minus accrued SLA penalty.
- Missing official delivery leaves earned revenue and penalty provisional, not
  zero.
- Use GBP in the first release and preserve the effective contract version and
  calculation inputs so corrected delivery can be recalculated with a complete
  audit trail.
- Invoicing and cash collection occur at different grains and become separate
  business processes later.

### DM-010 — customer-visible data boundary

Accepted as **DM-010** on 2026-08-27:

- Customer access is deny-by-default and limited to the authenticated
  customer's sites and delivery points. Never trust a customer identifier sent
  by the browser without server-side authorization.
- Customers may see their own commitments, accepted or provisional delivery,
  shortfalls, contractual availability, approved high-level cause categories,
  applicable contract rates, projected service charges and SLA credits, and
  data freshness/correction status.
- Other customers' data, raw SCADA and control telemetry, detailed alarms,
  maintenance work orders and technician notes, electricity procurement cost,
  company margin, internal pricing logic, and internal alert thresholds remain
  internal.
- Customer-facing financial terms use service-charge and SLA-credit language;
  internal revenue and margin remain separate.
- Dashboards, downloads, APIs, and AI tools enforce the same row and attribute
  boundaries. Cross-customer access is explicitly denied and tested.

## Decisions for us to make together

1. **Delivery event:** What exactly counts as steam being delivered: meter delta, integrated flow, or a reconciled billing record?
2. **Authority:** Is the revenue-grade meter authoritative, with SCADA used only for investigation?
3. **Lowest useful grain:** One delivery point per five minutes, per half-hour settlement period, or per contractual interval?
4. **Commitment shape:** Is the obligation a minimum quantity per interval, a daily quantity, a capacity promise, or some combination?
5. **Measurement:** Do we retain steam mass and thermal energy? Which unit is canonical?
6. **Time:** Which timestamp controls reporting, and do sites report in local civil time while storage remains UTC?
7. **Corrections:** How do meter resets, corrected readings, duplicates, missing intervals, and late events change prior results?
8. **Availability:** Does planned maintenance leave the denominator? Is availability time-based or capacity-weighted?
9. **Revenue:** Does “revenue” mean earned/accrued, invoiced, or collected? Where do penalties appear?
10. **Security:** Which attributes may customers see, and which remain internal to operations or finance?

## Modeling cautions

- Energy or steam delivered over non-overlapping intervals is normally additive.
- Instantaneous power, pressure, temperature, and state of charge are not additive across time.
- Market publications and meter readings can be revised; event time and publication/ingestion time must remain distinct.
- A telemetry snapshot and a commercial delivery measurement may be related but should not be silently treated as the same business event.

## Workshop output

We will produce:

- an accepted business-process statement;
- a one-sentence grain declaration;
- the first row of the bus matrix;
- candidate facts/dimensions;
- definitions for delivered steam and commitment shortfall;
- source precedence and correction rules;
- security classification;
- three reconciliation examples with expected totals.

## Status

**All ten foundational workshop questions have accepted decisions as of
2026-08-27. The original questions remain unchanged; see Accepted decisions and
the decision log for their status. The first bus-matrix row is also accepted as
DM-012, its eight logical dimensions as DM-013, its twelve logical fact measures
as DM-014, and nine metric contracts as DM-015. All three reconciliation
scenarios are accepted as DM-016 through DM-018. The logical workshop is
complete; see the Phase 1 completion report. No physical schema has been
implemented.**
