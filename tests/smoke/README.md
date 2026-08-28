# Phase 0 smoke tests

Run these from the repository root:

```bash
bash tests/smoke/run_r2_raw_smoke.sh
bash tests/smoke/run_lakehouse_smoke.sh
bash tests/smoke/run_iceberg_feature_smoke.sh
```

They verify, in order:

1. R2 raw-object upload, byte-for-byte retrieval, and cleanup.
2. Trino table creation, Spark Structured Streaming commit, Spark checkpoint
   recovery without duplication, and Trino visibility of the Spark snapshot.
3. Iceberg schema evolution, `MERGE`, row deletion, snapshot metadata, and time
   travel through Trino.

Every script reads credentials from the ignored `.env`. Trino is bound to
`127.0.0.1` and is stopped when each engine test finishes.
