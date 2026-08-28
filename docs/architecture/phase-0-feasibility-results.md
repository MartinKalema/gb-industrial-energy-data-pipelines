# Phase 0 lakehouse feasibility results

**Date:** 2026-08-27

**Result:** Passed

## Active platform identity

**Great Britain Industrial Thermal Battery Operations and Steam Delivery
Intelligence Platform**

Active R2 resources:

- `gb-industrial-thermal-energy-raw-dev`
- `gb-industrial-thermal-energy-lakehouse-dev`

R2 Data Catalog is active only on the lakehouse bucket.

## Proven behavior

| Boundary | Evidence | Result |
|---|---|---|
| Local to raw R2 | Uploaded a unique harmless object, retrieved identical bytes, then removed it | Passed |
| Trino to catalog | Created an Iceberg namespace and version-2 Parquet table | Passed |
| Spark streaming to R2 | Structured Streaming processed three events and committed an Iceberg snapshot | Passed |
| Spark recovery | A second Spark container reused the same checkpoint and appended no duplicates | Passed: exactly three rows |
| Trino to Spark snapshot | Trino read the three Spark-written records | Passed |
| Schema evolution | Added a column through Iceberg | Passed |
| Row-level changes | `MERGE` updated/inserted records and `DELETE` removed one record | Passed |
| Time travel | Queried the snapshot captured before merge/delete and recovered the historical row count | Passed |

## Pinned baseline

- Spark 3.5.3
- Apache Iceberg Spark runtime and AWS bundle 1.6.1
- Trino 478
- Cloudflare R2 Data Catalog beta

These are conservative first-slice pins. Upgrades will change one compatibility
boundary at a time after functional tests exist.

## Operational observations

- Spark alone processed the stream; Trino was started only for bounded SQL.
- Catalog-vended credentials worked for Spark writes.
- Cloudflare's documented static S3 credentials plus catalog token worked for
  Trino.
- The catalog currently reports compaction enabled at 128 MB but no stored
  maintenance credential; snapshot expiration is disabled.
- Spark/Iceberg 1.6.1 emits a warning that the OAuth token-service URI should be
  explicit in a future upgrade. The supplied token works and no OAuth exchange
  was needed for this test.
- All test containers were stopped after verification.

## Next infrastructure gates

- Add dbt-trino and test temporary, table, view, and incremental materializations.
- Add Redpanda and replace the finite file source with a Kafka-compatible topic.
- Add event-time, watermark, malformed-record quarantine, and late-data tests
  only after the telemetry event contract and dimensional grain are agreed.
