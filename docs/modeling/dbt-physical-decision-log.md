# Phase 2 dbt physical decision log

Accepted physical implementation choices for the first steam-delivery mart
will be recorded here. The original questions remain unchanged in the
[dbt physical-model workshop](10-phase-2-dbt-physical-model-workshop.md).

| ID | Decision | Status | Rationale | Date |
|---|---|---|---|---|
| DBT-001 | Delivery-interval fact spine | **Accepted: successfully reconciled Airflow batch coverage drives every expected delivery-point and UTC half-hour row** | A queryable control scope keeps completely missing intervals visible, supports correct completeness denominators and daylight-saving calendars, and deduplicates overlapping/replayed ranges without treating technical coverage as a business source | 2026-08-28 |
| DBT-002 | Layer schemas and model naming | **Accepted: source declarations, revision-preserving staging views, intermediate business logic, and governed dimensional marts use separate folders and `industrial_energy_*` schemas** | Makes the data journey visible, preserves dbt's standard schema naming, and prevents source-shaped relations from being mistaken for finished business results | 2026-08-28 |
| DBT-003 | Numeric precision and rounding | **Accepted: quantities use `decimal(20,6)`, rates use `decimal(18,6)`, and interval GBP uses `decimal(38,12)` with rounding only at presentation** | Preserves exact source values and prevents half-hour rounding from changing aggregate SLA and financial results | 2026-08-28 |
| DBT-004 | Warehouse surrogate keys | **Accepted: deterministic SHA-256 keys derive from stable source version or fact-grain identifiers, with `YYYYMMDD` as the readable date-key exception** | Produces the same identity across rebuilds, retries, and future batch/stream paths while corrections retain the same dimensional key and source identifiers remain traceable | 2026-08-28 |
| DBT-005 | Dimension history | **Accepted: source-driven Type 2 dimensions preserve genuine effective business versions while source corrections collapse into the accepted row for the same version or assignment episode** | Keeps event-time history accurate without inventing business eras from corrections or confusing dbt observation time with source effective/publication time | 2026-08-28 |
