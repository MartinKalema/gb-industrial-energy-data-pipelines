# Steam-delivery metric contracts

## Purpose

A metric contract gives one business measure a precise name, formula, eligible
population, missing-data rule, and aggregation rule. dbt models, dashboards,
APIs, exports, and AI tools must use these contracts instead of recreating
similar-looking calculations independently.

## Common scope and status rules

- An applicable interval has `committed_mwh_th > 0`.
- Corrected readings with accepted status are eligible; provisional, missing,
  and quarantined delivery is not official.
- Official period SLA and financial results are final only when delivery-data
  completeness is 100% for applicable intervals.
- Before completeness reaches 100%, known subtotals may be shown only with an
  explicit provisional status. Missing delivery is never converted to zero.
- A period with no applicable commitment returns `not applicable` for SLA
  attainment and contractual availability rather than 100%.
- Corrections recalculate the affected interval and every dependent aggregate.
- The contractual-availability formula is accepted, but a final availability
  result also requires a complete accepted source contract for
  `deliverable_capacity_mwh_th`. Until Phase 2 defines that source's authority,
  validity, completeness, and shared-capacity allocation, availability output
  remains provisional.

## Accepted contracts

Accepted as **DM-015** on 2026-08-27:

| Metric | Contract |
|---|---|
| Delivered energy | Sum accepted `delivered_mwh_th` in the selected scope; always disclose completeness |
| Committed energy | Sum `committed_mwh_th` for applicable intervals in the selected scope |
| Shortfall energy | Sum accepted `shortfall_mwh_th`; do not infer missing intervals as shortfalls |
| SLA attainment | `100 * sum(min(delivered_mwh_th, committed_mwh_th)) / sum(committed_mwh_th)` over applicable intervals |
| Contractual availability | `100 * sum(min(deliverable_capacity_mwh_th, committed_mwh_th)) / sum(committed_mwh_th)` over applicable intervals |
| Gross earned revenue | Sum accepted `gross_earned_revenue_gbp` under the effective contract versions |
| Accrued SLA penalty | Sum accepted `accrued_sla_penalty_gbp` as a positive deduction amount |
| Net earned revenue | Sum gross earned revenue minus sum accrued SLA penalty |
| Delivery-data completeness | `100 * accepted applicable interval count / expected applicable interval count` |

## Aggregation safeguards

- Cap delivery at its own interval commitment inside the SLA numerator. Excess
  delivery in one interval cannot hide a shortfall in another.
- Cap deliverable capacity at its own interval commitment inside the
  availability numerator.
- Calculate SLA and availability from summed numerators and denominators. Never
  average row, site, customer, or daily percentages.
- Monetary totals use the contract version effective during each delivery
  interval and GBP for the first release.
- Customer-visible service-charge and SLA-credit labels follow DM-010 even when
  the internal mart uses earned-revenue and penalty terminology.

## Status

The nine primary metric contracts and all three hand-calculated reconciliation
scenarios are accepted. Their expected results are ready to become dbt fixtures
and batch/stream convergence tests.
