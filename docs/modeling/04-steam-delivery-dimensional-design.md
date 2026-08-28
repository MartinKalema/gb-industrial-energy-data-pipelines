# Steam-delivery dimensional design

## Purpose

This document turns the accepted steam-delivery business rules into a logical
dimensional design. It remains technology-independent: accepting a logical
dimension or fact here does not create an Iceberg table or dbt model.

## Accepted grain

DM-002 defines the grain:

> One row represents steam delivered through one customer delivery point during
> one non-overlapping 30-minute interval.

A fact contains measurements at that grain. Dimensions describe the business
context surrounding those measurements.

## Accepted dimensions

Accepted as **DM-013** on 2026-08-27:

| Logical dimension | Purpose |
|---|---|
| `dim_date` | British reporting date, weekday, calendar period, and daylight-saving day type |
| `dim_interval` | One unique 30-minute UTC interval and its `Europe/London` representation |
| `dim_customer` | Customer identity and tenant authorization scope |
| `dim_site` | Industrial site, location, region, and timezone |
| `dim_delivery_point` | Contractual location where steam passes to the customer |
| `dim_contract` | Effective contract version, tariff, and penalty rules |
| `dim_meter` | Revenue meter, register, source unit, and calibration identity |
| `dim_data_status` | Measurement acceptance, provisional/missing/quarantine state, and correction state |

## History and relationship rules

- A delivery fact points to the customer, site, delivery point, contract, and
  meter versions that were valid during its event-time interval. A later
  descriptive change must not rewrite prior business context.
- Preserve durable source natural identifiers alongside warehouse-generated
  keys so every dimension record can be traced to its source.
- `dim_interval` identifies a real UTC interval uniquely, even when a local
  clock time repeats during the GMT/BST transition.
- `dim_date` represents the `Europe/London` civil reporting date derived under
  DM-008.
- `dim_data_status` describes the current accepted result. Raw source revisions
  remain preserved under DM-009 rather than becoming duplicate dimensional
  facts.
- Commitment quantity is not a contract-dimension attribute because it varies
  by delivery point and interval. It belongs in the fact at the accepted grain.

## Deliberate exclusions

Asset, raw telemetry, work-order, electricity-charging, market-price, and carbon
context do not become dimensions of this delivery fact merely because they help
explain an outcome. They occur at different grains and will be modeled as
separate processes connected through conformed context.

## Accepted fact measures

Accepted as **DM-014** on 2026-08-27:

| Logical measure | Meaning | Additivity |
|---|---|---|
| `opening_register_mwh_th` | Accepted cumulative revenue-meter value at interval start | Non-additive; never sum |
| `closing_register_mwh_th` | Accepted cumulative revenue-meter value at interval end | Non-additive; never sum |
| `committed_mwh_th` | Applicable minimum thermal-energy commitment | Additive over non-overlapping rows |
| `delivered_mwh_th` | Closing register minus opening register | Additive over non-overlapping rows |
| `delivered_steam_t` | Supporting delivered steam mass | Additive over non-overlapping rows |
| `shortfall_mwh_th` | `max(committed_mwh_th - delivered_mwh_th, 0)` | Additive over non-overlapping rows |
| `excess_mwh_th` | `max(delivered_mwh_th - committed_mwh_th, 0)` | Additive over non-overlapping rows |
| `deliverable_capacity_mwh_th` | Thermal energy the service was capable of supplying | Additive over non-overlapping rows |
| `billable_mwh_th` | Accepted delivery eligible for payment under the effective contract | Additive over non-overlapping rows |
| `gross_earned_revenue_gbp` | Billable delivery multiplied by the effective contract rate | Additive over non-overlapping rows |
| `accrued_sla_penalty_gbp` | Accepted shortfall multiplied by the effective penalty rate | Additive over non-overlapping rows |
| `net_earned_revenue_gbp` | Gross earned revenue minus accrued SLA penalty | Additive over non-overlapping rows |

Availability percentage is not an additive stored measure. Calculate it for a
selected set of applicable intervals as:

```text
sum(min(deliverable_capacity_mwh_th, committed_mwh_th))
-----------------------------------------------------------------
sum(committed_mwh_th for intervals with a positive commitment)
```

Do not average interval availability percentages because intervals can carry
different commitment weights.

If official delivery is missing, `delivered_mwh_th`, `shortfall_mwh_th`,
`excess_mwh_th`, `billable_mwh_th`, and dependent revenue and penalty amounts
remain null, not zero. Commitment and independently observed deliverable
capacity may remain populated.

Electricity cost and carbon are deliberately absent because stored heat can be
charged in a different interval from steam delivery. Their allocation requires
separate charging, market, and thermal-inventory processes.

## Status

The eight logical dimensions, twelve fact measures, and nine metric contracts
are accepted. All three expected-result scenarios are accepted. Exact physical
columns remain pending review; no physical schema has been created. The source,
validity, completeness, and shared-capacity allocation rules that populate
`deliverable_capacity_mwh_th` are Phase 2 entry decisions; availability output
remains provisional until those source contracts are accepted.
