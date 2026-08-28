# Batch ingestion

This area contains finite, idempotent source jobs that Airflow will invoke.
The first job is implemented: a deterministic generator for the nine accepted
fictional private sources. It emits source-shaped JSONL evidence and a hashed
manifest; it does not emit warehouse facts or select current revisions.

```bash
python3 ingestion/batch/synthetic/generate.py generate \
  --start-date 2026-08-27 \
  --end-date 2026-08-27 \
  --seed 20260828 \
  --generation-time-utc 2026-08-28T12:00:00Z \
  --output-dir /tmp/industrial-energy-synthetic
```

The caller must choose an output directory and an explicit generation time.
Generated bulk data stays out of version control. See the
[generator guide](synthetic/README.md) and the
[machine-readable contracts](../../contracts/README.md).

The next bounded jobs are:

- upload each generated JSONL object and its raw-evidence envelope to R2;
- validate and load the nine source datasets into Iceberg;
- ingest Elexon FUELHH as a separate public external-API sidecar; and
- reconcile row counts, hashes, revisions, and event-time coverage on rerun.

FUELHH is not joined to the steam-delivery mart. NESO carbon and IRIS streaming
are later-scope sources, not hidden dependencies of the Phase 2 batch slice.
Every batch load must retain source/request metadata, use bounded windows, and
be safe to replay.
