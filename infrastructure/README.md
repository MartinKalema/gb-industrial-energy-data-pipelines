# Local infrastructure

The eventual Compose setup will be split into profiles:

- `core`: PostgreSQL for Airflow and application metadata
- `query`: Trino connected to Cloudflare R2 Data Catalog
- `batch`: core plus query, Airflow, and the dbt runner
- `stream`: Redpanda, IRIS/simulator producers, and Spark local mode; no Trino
- `product`: query plus FastAPI and the web application

The first executable feasibility slice pins Spark 3.5.3, Iceberg 1.6.1, and
Trino 478. These conservative versions align with Cloudflare's published Spark
example and the Trino version tested by the selected dbt-trino release. An
upgrade is a separate measured change after the catalog test is green.

Run the cross-engine catalog smoke test from the repository root:

```bash
bash tests/smoke/run_lakehouse_smoke.sh
```

The test creates the namespace/table through Trino, processes three file events
through Spark Structured Streaming using catalog-vended credentials, restarts
the Spark job from the same checkpoint to prove it does not duplicate those
events, and reads the committed rows through Trino. Trino stops when the test
finishes. Credentials enter through the ignored `.env` and environment
substitution; they are not stored in the Compose or catalog files.
