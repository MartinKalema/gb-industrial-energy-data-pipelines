# Bounded Airflow to R2, Iceberg, and dbt pipeline

## What this pipeline does, in simple English

An operator chooses a small date range and starts one Airflow run. The pipeline
creates the nine synthetic business-source files for that period, saves the
original bytes permanently in Cloudflare R2, checks every record, separates
invalid records from valid ones, and writes the valid records into typed
Apache Iceberg tables through Trino. Once those source rows reconcile, it
publishes the expected date coverage and uses dbt to build and test the
delivery, SLA, availability, and earned-revenue dimensional marts.

The important idea is that the original evidence is never silently corrected
or overwritten. If a record is bad, its original content and the reasons it
failed remain in quarantine. If the same run is tried again, identical evidence
is reused instead of duplicated. This gives us a reproducible path from an
Iceberg row back to the exact source object and JSONL line that produced it.

The first seven tasks form the Phase 2 ingestion/control boundary and preserve
source revisions. Six ordered dbt tasks form the separate analytical boundary:
they select authoritative revisions, prepare shared calculations, build the
current and as-known history facts, build the dimensions, and run final tests.

## Current implementation and verification status

The DAG and its pipeline components are implemented. The seven-task
source/control Compose path was verified against real Cloudflare R2, R2 Data
Catalog, and Trino 478 on
2026-08-28 with pipeline ID
`batch-20260826-20260826-77387cd7bfe41eb9`:

- first attempt: 313 raw, 313 accepted, zero quarantined/duplicates/conflicts,
  and 313 inserted into nine Iceberg tables;
- exact replay: every raw, envelope, manifest, accepted, quarantine, and report
  object was byte-verified and reused; Iceberg inserted zero and reused all 313
  source-revision identities; and
- the first R2 landing time was preserved separately from the earlier fixed
  synthetic generation timestamp and appeared in Iceberg lineage.

The committed row counts were 4 approved-excess rows, 97 commitments, 4
contract versions, 4 customer versions, 2 delivery-point assignments, 97
capacity assessments, 4 site versions, 2 meter assignments, and 99 meter
readings. Both execution-specific reconciliation summaries were written under
the deterministic pipeline identity.

The implementation is in:

- [`orchestration/dags/steam_delivery_data_pipeline.py`](../../orchestration/dags/steam_delivery_data_pipeline.py)
- [`ingestion/batch/pipeline/`](../../ingestion/batch/pipeline/)
- [`infrastructure/airflow/`](../../infrastructure/airflow/)
- [`infrastructure/compose.yaml`](../../infrastructure/compose.yaml)

## Bounded run contract

The Airflow DAG is `steam_delivery_data_pipeline`. It is manually triggered
and has no schedule. Each run receives four explicit parameters:

| Parameter | Contract | Why it is explicit |
|---|---|---|
| `start_date` | First operating date, inclusive, in `YYYY-MM-DD` form | Defines the first day generated |
| `end_date` | Last operating date, inclusive, on or after `start_date` | Makes the end of the request unambiguous |
| `seed` | Fixed integer `20260828` | Identifies the one continuous project timeline |
| `generation_time_utc` | Fixed RFC 3339 UTC timestamp ending in `Z` | Makes evidence timestamps and retry output reproducible |

The default maximum is 31 inclusive dates. `PIPELINE_MAX_BATCH_DAYS` can change
the limit for an intentional experiment, but the normal local contract remains
31 days. The run planner rejects a reversed range, an oversized range, a
date before the continuous synthetic timeline, any seed other than the fixed
project seed, a non-UTC generation time, unsafe catalog/schema identifiers, or
a relative working directory.

Airflow permits only one active run of this DAG. The seven source/control tasks
receive one retry after one minute and a 20-minute execution timeout. Each of
the six dbt tasks has a 120-minute subprocess limit, a 125-minute Airflow limit,
and one retry after two minutes. The outer limit leaves time for up to one
minute of local process cleanup, one minute of Trino cleanup, and scheduler
margin. The whole DAG run has a bounded practical ceiling of 180 minutes. A dbt
retry runs only if enough DAG-run time remains; the limit does not guarantee a
complete second attempt. Airflow XCom carries only small
dictionaries containing identifiers, paths, counts, hashes, and artifact
locations; source records and per-node dbt results do not pass through the
metadata database.

The pipeline identity is deterministic for the combination of generator
version, start date, end date, seed, and generation timestamp:

```text
batch-<YYYYMMDD>-<YYYYMMDD>-<first 16 characters of the versioned input SHA-256>
```

The Airflow run ID is recorded separately for orchestration traceability. It is
not part of the evidence identity, so retrying the same bounded inputs produces
the same pipeline run ID and object keys.

## Why this DAG is manual

This DAG is a controlled learning, replay, and backfill workflow. A person
chooses the operating-date range and the evidence-generation timestamp, so a
run never silently invents a new date merely because the clock advanced. The
synthetic generator is standing in for future API or database extracts; it is
not itself a production source feed.

A run is not automatically new business data:

| Run inputs | Business-row outcome |
|---|---|
| Same generator version, dates, seed, and generation time | Exact replay: same pipeline identity, R2 objects, and source rows |
| Same dates and seed, later generation time | New evidence-run identity and manifest; source JSONL rows are unchanged and Iceberg reuses them |
| New operating dates, fixed project seed | New interval source revisions are appended; stable master rows and the shared cumulative-meter boundary are exact replays |
| Same dates, different seed | Invalid for this timeline: meter payloads change under existing source identities and the loader rejects the conflict |

The pipeline enforces the fixed project seed `20260828`. Generator version
`1.1.0` is part of the pipeline identity. The complete continuous timeline
starts on the `2026-08-26` Europe/London operating date. Generating adjacent
days separately is guaranteed to produce the same identities and payloads as
one combined request.

One ordinary one-day bundle contains 313 source revision rows. A following
ordinary day contributes 295 new source revisions: 16 master/history rows and
the two shared meter-boundary rows replay exactly. The two-day union therefore
contains 608 source revisions. After current-revision selection, the mart grain
is smaller: two delivery points times 48 half-hours gives 96 delivery facts per
ordinary day.

If regular data growth is wanted later, keep this manual DAG for backfills and
add a separate daily DAG. That DAG should derive one completed
`Europe/London` operating date from Airflow's logical data interval, retain the
fixed seed, and call the same bounded workflow. It should run late enough for
the deliberately delayed synthetic corrections to have been published.

Growing history does not by itself require dbt incremental models. A full
rebuild is still correct and is useful as the initial correctness baseline at
this scale. Incremental materialization becomes worthwhile only when measured
runtime or scan cost is material; it must then reproduce the full-build result,
including recalculating both intervals adjacent to a corrected meter boundary.

## Components and data flow

```text
Airflow on this computer
  1. plan the bounded run
  2. generate nine deterministic JSONL sources + manifest
  3. write immutable raw objects, evidence envelopes, and manifest to R2
  4. read the raw objects back and verify their hashes
  5. validate records and write accepted/quarantine/report objects to R2
  6. send bounded SQL statements to local Trino
  7. Trino commits accepted rows to Iceberg tables stored on R2
  8. reconcile counts and write the summary to R2
  9. publish the successfully reconciled local-date coverage to Iceberg
 10. build and test the nine revision-preserving staging views through dbt
 11. build and test the 33 delivery-calculation views
 12. build the current delivery fact
 13. build the source-knowledge delivery-history fact
 14. build the 13 dimension and revision-audit tables
 15. run the 70 final dimensional-mart and reconciliation tests
 16. publish the tested dimensional mart to the local ClickHouse serving database

Remote managed services
  Cloudflare R2 object storage + Cloudflare R2 Data Catalog

Local serving service
  ClickHouse stores the latest tested, versioned product copy
```

The responsibilities are deliberately separate:

- Airflow controls the finite sequence, retries, task history, and parameters.
- The generator creates source-shaped fictional business evidence.
- The Python validation layer performs JSON Schema and cross-record business
  checks without depending on Airflow, R2, Trino, or Spark.
- Trino performs finite table creation, identity checks, and Iceberg writes.
- Iceberg supplies typed tables, snapshots, and table metadata over R2.
- dbt applies revision precedence, dimensional logic, and executable quality
  rules through finite Trino SQL after the coverage row exists.
- Spark is not used in this batch DAG. It remains the streaming compute engine
  for Phase 3.

All compute runs locally on this computer. R2 is the only object-storage
runtime. There is no MinIO service and no local object-store runtime. The
Airflow named volume holds only local metadata, logs, generated working files,
and the local authentication file; it is not a second data lake. A filesystem
fake exists only inside automated tests so storage rules can be tested without
writing cloud objects.

## Immutable R2 layout

`R2_PIPELINE_PREFIX` defaults to `industrial-energy`. Under that prefix, one
run writes these objects to `R2_RAW_BUCKET`:

```text
industrial-energy/
  raw/synthetic/<dataset>/
    start_date=<start>/end_date=<end>/schema_version=<version>/
      sha256=<source-file-sha256>/<dataset-file>.jsonl

  raw/_evidence/<pipeline-run-id>/<dataset>.envelope.json
  raw/_manifests/<pipeline-run-id>/source-manifest.json

  validated-staging/<pipeline-run-id>/<dataset>.accepted.jsonl
  quarantine/<pipeline-run-id>/<dataset>.quarantine.jsonl

  quality/<pipeline-run-id>/validation-report.json
  quality/<pipeline-run-id>/reconciliation/
    attempt=<orchestrator-run-id-sha256-prefix>.summary.json
```

The raw data key is content-addressed: the source file SHA-256 is part of the
key. Writes use an R2 conditional create. If the key is absent, it is created.
If it already contains identical bytes, the retry reuses it. If the key exists
with different bytes, length, or content type, the pipeline fails with an
immutable-object conflict instead of overwriting the evidence.

On a replay, the R2 adapter downloads the existing object and compares its
actual bytes and content type. It does not trust object metadata alone. The
object's first R2 `Last-Modified` time becomes the platform ingestion time and
is reused by later attempts; the synthetic generation/publication timestamps
remain separate source and manifest fields.

Accepted, quarantine, and validation objects are also written immutably under
the deterministic pipeline run ID. Reconciliation is operational evidence, so
each Airflow attempt gets an immutable key under that ID: the first attempt can
truthfully report inserts while an exact replay reports reuse. Therefore an
exact replay is safe, while changed deterministic output for the same declared
inputs is visible as a conflict.

## Validation and quarantine

Validation first checks that all nine required files are present and readable.
Each JSONL line must be a JSON object and must satisfy its Draft 2020-12 source
contract. The validator then applies rules that one JSON Schema cannot check
alone, including:

- the governed logical-key-plus-revision identity is complete;
- JSON objects do not repeat a key and hide an earlier value;
- one immutable revision identity has only one canonical payload;
- reused opaque revision IDs do not point to different logical revisions;
- interval rows last exactly 30 minutes and begin on a UTC half-hour boundary;
- effective customer, site, delivery-point, meter, and contract episodes do not
  overlap where the Phase 2 rules require one active record;
- direct customer, site, and delivery-point references exist;
- children of a quarantined parent assignment are also quarantined;
- interval records have one valid event-time assignment and effective business
  context;
- contract/customer/delivery-point values agree;
- an approved excess order has a positive current base commitment; and
- revenue readings match one authoritative meter assignment and its register
  metadata.

An accepted output contains the original source payload, not a corrected or
enriched copy. A quarantined output contains the dataset, source line number,
stable issue codes and messages, the parseable original record or raw line,
and any identity/hash that could be calculated. This preserves the failure as
evidence and makes repair explainable.

If the same immutable revision appears more than once with the same canonical
payload, one copy is accepted and the rest are counted as exact replays. If the
same revision identity has different payloads, every conflicting version is
quarantined. The validator does not fall back to an older revision when the
declared latest revision is invalid.

A missing required source file is a bundle-level failure. Airflow stops before
the Iceberg load because the nine-source business context is incomplete. Row-
level failures do not stop valid rows from loading; the final status becomes
`succeeded_with_quarantine`, and the counts and reasons remain reviewable.

## Per-row lineage in Iceberg

Trino creates one typed Iceberg table per synthetic dataset in
`r2.industrial_energy_validated` by default. Source columns come from the JSON
Schema contract. Every accepted row also receives these pipeline columns:

| Column | Meaning |
|---|---|
| `pipeline_run_id` | Deterministic bounded-run identity |
| `pipeline_evidence_envelope_id` | Evidence document that describes the source file |
| `pipeline_ingested_at_utc` | Actual first R2 landing time of the immutable raw object |
| `pipeline_raw_object_uri` | Exact `r2://` source object |
| `pipeline_raw_object_sha256` | Hash used to verify that source object |
| `pipeline_raw_record_locator` | Original JSONL line, for example `line:17` |
| `pipeline_identity_sha256` | Hash of dataset plus governed source-revision key |
| `pipeline_payload_sha256` | Hash of the canonical source payload |

Together, the object URI, object hash, line locator, revision identity, and
payload hash answer: “Which exact evidence created this row, and did its
contents change?”

## Retry and idempotency rules

Retries are safe at four boundaries:

1. The generator uses fixed inputs and writes deterministic source files and a
   hashed manifest into a run-scoped working directory.
2. R2 conditional creates reuse only identical objects and reject a different
   payload at an existing immutable key.
3. The Iceberg loader computes an immutable revision identity and payload hash
   before each write. If the table already contains that identity and hash, the
   row is counted as an exact replay. If the identity exists with a different
   hash, the row is a conflict and the existing row is never updated.
4. dbt runs the full-rebuild baseline as six ordered restart points: staging
   build/tests, intermediate build/tests, current fact, history fact,
   dimensions, and final mart tests. Each task and try gets a separate
   target/log/result path. Its Trino connections carry an exact task-attempt
   tag, so failure cleanup never selects another dbt run by shared SQL text or
   adapter name. A normal Airflow retry repeats only the failed section.

New rows are written with `MERGE ... WHEN NOT MATCHED THEN INSERT`. The loader
checks the table before the merge and confirms the identities after it. This
also detects a concurrent writer that committed different content. Source
revisions are append-only evidence here; choosing a current revision belongs in
the later dbt model.

Iceberg does not enforce a unique constraint on `pipeline_identity_sha256`.
`max_active_runs=1` serializes this DAG, and its source load, coverage, and six
dbt tasks use the provisioned one-slot `iceberg_writer` Airflow pool. Every
future manual or backfill DAG that writes these tables must use that same pool.
The insert-only merge and post-write identity/hash check make completed
chunks safe to retry after a partial failure, but they do not replace writer
serialization when two first-time writers could race to insert the same
identity. A manual dbt process started outside Airflow does not participate in
the pool and must not overlap any of the DAG's dbt tasks.

## Failures and recovery

| Failure | What the pipeline does | Safe recovery |
|---|---|---|
| Invalid dates, seed, timestamp, or unsafe configuration | Fails during planning; no business data advances | Correct the parameters and start a new run |
| Generator count/hash mismatch | Stops before R2 landing | Investigate the generator or manifest, then retry the same inputs |
| R2 is unavailable | The task fails and Airflow retries once | Restore connectivity/credentials and retry; identical prior writes are reused |
| Immutable R2 key contains different content | Stops; never overwrites evidence | Treat as a reproducibility incident and compare the stored and proposed hashes |
| R2 download hash differs | Stops before validation/load | Investigate object integrity or incorrect metadata |
| One Trino statement exceeds five minutes | Cancels the active query and fails the task | Inspect Trino/R2 health or reduce the chunk before retrying |
| Missing required dataset | Stops before Iceberg | Repair/regenerate the complete bounded bundle and rerun |
| Invalid row or cross-record rule failure | Writes that row to quarantine; valid rows continue | Review issue codes, correct the upstream revision, then publish a new source revision/run |
| Existing Iceberg table disagrees with its contract | Stops before inserting into that table | Apply an explicit compatible schema migration; do not silently coerce |
| Same Iceberg identity has different content | Records a loader conflict; reconciliation fails | Investigate the competing source revision; never update the existing evidence row in place |
| Two DAGs can write the same tables concurrently | Iceberg has no uniqueness constraint, so both first writers could insert | Put every writer in the same one-slot Trino-load pool before enabling another DAG |
| A dbt model or data test fails | That dbt checkpoint and the DAG fail; reconciled source rows, coverage, and earlier successful checkpoints remain valid | Inspect that task's dbt log, correct the cause, and retry or clear only the failed task |
| A dbt checkpoint exceeds 120 minutes, exits with an error, or is interrupted | Stops that local process group, cancels only active Trino queries with the exact task-attempt user/tag, and waits for a stable no-active-query result before releasing the writer pool | After confirmed cleanup, the normal retry of that checkpoint is safe; inspect its dbt/Trino logs for the cause |
| Trino cannot confirm dbt query cleanup within 60 seconds | Fails the checkpoint without an automatic retry, because the previous write state is uncertain | Verify that no query with the logged task-attempt tag remains active, resolve Trino connectivity or cancellation, then rerun that checkpoint manually |
| Airflow container stops after a partial task | Task history and work files remain in the named volume | Restart and retry with the same four inputs |

Do not repair a failed run by editing a raw R2 object, accepted JSONL, or an
existing source-revision row. Correct the producing code or publish a new
revision so the evidence history remains auditable.

## Observability and reconciliation

Airflow shows task state, duration, retry count, and logs for 14 tasks. The first
seven check the run inputs, generate the source files, save the originals in
R2, check rows and save failures separately, load accepted rows into Iceberg,
verify every row was handled, and record the loaded date range. The next six
prepare/test staging, prepare/test calculations, build the current fact, build
the history fact, build the dimensions, and run final mart tests. The final
task publishes only that tested result to ClickHouse for fast product reads.
Small task summaries include hashes, locations, and counts. Each dbt task
streams its own model or test progress to the Airflow log and returns only its
invocation ID, version, elapsed time, aggregate statuses, and artifact paths.

The validation report records total and per-dataset accepted, quarantined, and
exact-replay counts; quarantine-reason counts; accepted row identities/hashes;
and hashes/counts for every accepted and quarantine output file. Each dataset
load records input rows, planned new rows, inserted rows, exact replays,
conflicts, chunks, and warnings.

The reconciliation task enforces both equations:

```text
raw records = accepted records + quarantined records + duplicate replay records

accepted records = Iceberg inserted records + Iceberg exact-replay records
```

Any Iceberg identity conflict fails reconciliation. A successful summary is
stored below `quality/<pipeline-run-id>/reconciliation/` with the Airflow run
ID, raw, validation, duplicate, insert/reuse, and table counts. The attempt key
uses a safe SHA-256 prefix of the Airflow run ID. Each summary is the compact
proof that one execution accounted for every raw row without forcing a first
insert and a later exact replay to claim the same immutable object key.

Only after that proof succeeds, the final source/control task inserts one row into
`r2.industrial_energy_control.batch_run_coverage`. The row records the
inclusive `Europe/London` dates, generator and raw-manifest identity, balanced
counts, reconciliation status, and first successful attempt artifact. Its
canonical hash excludes attempt-specific Airflow and reconciliation-artifact
fields, and represents inserted plus replayed Iceberg rows as one reconciled
count. Therefore an exact replay reuses the first row, while the same
`pipeline_run_id` with changed stable coverage content fails as an immutable
payload conflict.

The first dbt task then requires a created or exactly reused coverage row for
the same pipeline identity, and each later dbt task depends on the preceding
one. Coverage is deliberately not redefined as mart certification: it remains
valid if dbt later fails. dbt replaces relations one at a time, so a failed
checkpoint can leave a mixed set of relations inside that section. Airflow
retries only the failed checkpoint; earlier successful checkpoints stay green
and do not run again. Consumers must treat the mart as ready only after
`test_complete_dimensional_mart_with_dbt` succeeds.

The dbt order matters on a clean schema. The staging and intermediate sections
prepare shared views, the current and history facts are built next, and the 13
dimensions follow because `dim_data_status` reads both facts. The final 70 tests
then certify the complete mart.

Every dbt checkpoint and try has separate target, log, and compact result
artifacts, so a failed attempt cannot be mistaken for an earlier success. The
subprocess receives only the DBT/Trino connection settings and basic
operating-system variables; R2 credentials remain in Airflow/Trino and are not
inherited by dbt.

## Scale trade-offs and revisit triggers

The 31-day maximum is intentional. This is a laptop-run portfolio pipeline, and
the current generator keeps a complete bounded bundle in local working files
while validation evaluates cross-record relationships across that bundle.
Trino writes are divided into `TRINO_INSERT_BATCH_SIZE` chunks (100 in Compose;
the loader accepts 1 to 5,000) so one SQL statement does not grow without
limit. Each statement also has a five-minute overall deadline, while Airflow
limits source/control tasks to 20 minutes, each dbt subprocess to 120 minutes,
each dbt task to 125 minutes, and the complete run to 180 minutes. The loader
still performs identity lookups and a merge per chunk, which
favours clarity and safe replay over maximum throughput.

Revisit this design when any of these becomes true:

- a normal backfill needs more than 31 days;
- a dataset no longer fits comfortably in the local work volume or validator
  memory;
- generated `VALUES`/`MERGE` statements approach Trino request or planning
  limits;
- many small Iceberg snapshots/files make reads or metadata operations slow;
- multiple writers must load the same table concurrently; or
- Airflow SQLite/standalone mode is no longer sufficient for local development.

Likely next changes would be date-partitioned Airflow mapping, file-based
staging into Iceberg instead of SQL `VALUES`, measured Iceberg partitioning and
compaction, a production Airflow metadata database/executor, and explicit
concurrency control. Until a stronger concurrency design is proven, all writers
to these tables must share the one-slot Trino-load pool. Those changes should
follow measurements; increasing the 31-day bound alone would remove the safety
guarantee without solving the underlying scale limit.

## Configuration and secrets

Copy [`.env.example`](../../.env.example) to the ignored `.env` file and fill
the R2 and catalog values locally. The Airflow and Trino containers receive
credentials through environment substitution. Run plans, XCom values, logs,
source manifests, and committed configuration must contain no secret values.

The local Airflow UI binds to `127.0.0.1` by default. Its startup wrapper
generates a persistent 256-bit URL-safe password instead of a checked-in or
`change-me` value. See the [Airflow runtime guide](../../infrastructure/airflow/README.md)
for the command that reads the local credential.

Never commit `.env`, R2 keys, Cloudflare tokens, generated JSONL, Airflow state,
task logs, checkpoints, or data volumes. The fact that this is a development
project does not make credentials part of the data contract.

## Related documents

- [Phase 2 source implementation](phase-2-source-implementation.md)
- [Source contracts](../../contracts/README.md)
- [Synthetic generator](../../ingestion/batch/synthetic/README.md)
- [Airflow orchestration](../../orchestration/README.md)
- [Local infrastructure](../../infrastructure/README.md)
- [Incremental roadmap](../roadmap.md)
