# Batch ingestion

This area contains finite, idempotent source jobs that Airflow invokes. The
bounded Phase 2 path now generates the nine accepted fictional private sources,
lands their exact bytes and evidence envelopes in Cloudflare R2, validates and
quarantines records, loads accepted revisions into typed Iceberg tables through
Trino, and reconciles every input row.

In simple terms: we first keep what the source actually said, then decide which
rows are safe to query. Bad rows are preserved with an explanation; they are
not silently fixed or discarded.

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

## Bounded Airflow pipeline

The manual `industrial_energy_bounded_batch` DAG performs these stages:

1. Validate an inclusive date range of at most 31 days, a non-negative seed,
   and a fixed UTC generation timestamp.
2. Generate nine deterministic JSONL files and their hashed manifest.
3. Write immutable, content-addressed raw files, evidence envelopes, and the
   source manifest to R2.
4. Read the objects back, verify hashes, and apply JSON Schema plus cross-record
   validation rules.
5. Write original valid payloads to accepted staging and invalid payloads to
   quarantine, both on R2.
6. Load accepted records in bounded chunks into one typed Iceberg table per
   source dataset through local Trino.
7. Reconcile raw, accepted, quarantine, duplicate, inserted, and reused counts.

The exact run contract, R2 prefixes, lineage columns, retry rules, and recovery
procedures are in the
[bounded pipeline architecture and runbook](../../docs/architecture/bounded-airflow-r2-iceberg-pipeline.md).
Triggering instructions are in the [Airflow guide](../../orchestration/README.md).

The pipeline preserves source revisions. It does not select the current
business revision or produce dimensional facts; those transformations belong
to the later dbt boundary.

## Storage boundary

All compute runs locally, but R2 is the only object-storage runtime. There is
no MinIO or local object-store service. Local run-scoped files let adjacent
Airflow tasks exchange generated and validated artifacts; the durable raw,
quality, quarantine, and Iceberg data remains on R2. A filesystem store is used
only by tests.

## Still to build

The remaining bounded batch work includes:

- ingest Elexon FUELHH as a separate public external-API sidecar; and
- build the physical dimensional mart and tests through dbt-trino.

FUELHH is not joined to the steam-delivery mart. NESO carbon and IRIS streaming
are later-scope sources, not hidden dependencies of the Phase 2 batch slice.
Every batch load must retain source/request metadata, use bounded windows, and
be safe to replay.
