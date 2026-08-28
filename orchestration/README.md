# Airflow orchestration

Airflow will orchestrate finite workflows:

- historical extracts and backfills;
- raw-to-validated batch loads;
- `dbt build` through `dbt-trino`;
- batch/stream reconciliation;
- Iceberg compaction and metadata maintenance;
- source freshness and quality checks;
- recovery workflows for known missing intervals.

Long-running IRIS, Redpanda, and Spark consumers run as supervised services. Airflow observes their health and triggers bounded recovery actions.
