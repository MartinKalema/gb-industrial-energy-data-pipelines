# System design

## Status

Implemented local batch, dimensional-mart, ClickHouse serving, API, and web
vertical slice. The streaming components remain the Phase 3 design. Accepted
business grains and definitions live in `docs/modeling/` and are not redefined
here.

## High-level design

```text
                                  EXTERNAL SERVICES
        Elexon REST       Elexon IRIS AMQP       NESO Carbon API       R2
             |                    |                      |               ^
-------------|--------------------|----------------------|---------------|----
                             LOCAL COMPUTER                              |
             |                    |                      |               |
          Airflow           Python IRIS bridge     poll adapter          |
             |                    |                      |               |
             |                    +----------+-----------+               |
             |                               v                           |
             |                     Redpanda event log                    |
             |                               |                           |
             |                     Spark Structured Streaming -----------+
             |                                                           |
             +----------------> raw landing / Iceberg batch writes -------+
                                                                         |
                       Cloudflare R2 Data Catalog (Iceberg REST)          |
                                      |                                  |
                                    Trino <-------------------------------+
                                      |
                                dbt models/tests
                                      |
                           governed dimensional marts
                                      |
                       tested Airflow publication task
                                      |
                          ClickHouse serving versions
                                      |
                       serialized retention cleanup
                                      |
                           tenant-scoped FastAPI
                                      |
                              Next.js/TypeScript
```

## Component responsibilities

| Component | Owns | Does not own |
|---|---|---|
| Airflow | Finite API pulls, backfills, dbt jobs, compaction, quality checks, recovery workflows | Per-event work or an infinite streaming loop |
| Redpanda | Local durable event buffer, replay, consumer offsets | Analytical history or business metrics |
| Spark Structured Streaming | Event-time validation, deduplication, watermarks, stateful windows, Iceberg micro-batch commits | Semantic business definitions |
| R2 | Durable data and Iceberg metadata files | Iceberg catalog state or query compute |
| Iceberg catalog | Table identity and current metadata pointers | Data files or analytical execution |
| Trino | Interactive and dbt SQL over Iceberg | Stream ingestion |
| dbt | Transformation DAG, tests, contracts, documentation, governed metrics | Continuous event processing |
| ClickHouse | Rebuildable, versioned native tables for fast frontend reads | Canonical evidence, Iceberg ownership, or business-metric definitions |
| SQLite | Local standalone Airflow metadata and task history | Iceberg catalog state, canonical evidence, or analytical telemetry history |
| FastAPI | Authorization, product contracts, ready-version selection, governed tool calls | Arbitrary user SQL or canonical data storage |
| Next.js | Role-specific product experience | Security enforcement by itself |

The ClickHouse serving database has four tables: current delivery rows,
delivery-history rows, ready publication markers, and durable per-publication
change summaries. The product API reads the first three. The change summary is
for operations and audit evidence.

## Storage layers

Use layers by responsibility rather than treating medallion names as the model:

1. **Raw evidence:** immutable source payload, retrieval/publication timestamps, request parameters, checksum, schema version, and source identity.
2. **Validated events:** typed, normalized, deduplicated, quality-classified events with event time and ingestion time.
3. **Integrated core:** reconciled operational and commercial records at agreed source grains.
4. **Dimensional products:** facts, conformed dimensions, and shared metrics designed from user decisions.

Raw retention makes revised market publications, meter corrections, and stream-gap recovery auditable.

## Batch flow

1. Airflow selects a bounded date/publication window from a persisted high-water mark.
2. The extractor calls Elexon/NESO, validates the response envelope, and stores raw evidence.
3. A batch writer creates or merges validated Iceberg records idempotently.
4. Airflow invokes six ordered dbt tasks through the Trino adapter: prepare and
   test staging, prepare and test calculations, build the current fact, build
   the history fact, build the dimensions, and run the final mart tests.
5. If a dbt task fails, Airflow retries only that task. The final test task
   certifies the complete mart as ready after the earlier checkpoints succeed.
6. Airflow reads the complete tested product projections and compares them with
   the newest healthy ClickHouse version. It creates a hidden new
   `load_attempt_id`, clones unchanged rows inside ClickHouse, removes deleted
   or replaced keys, and inserts only new or updated rows into ClickHouse. With
   no healthy base it publishes every row instead.
7. Airflow validates the complete hidden version, records durable per-dataset
   change counts, and inserts its ready marker last.
8. A failed publication stays invisible and the API continues serving the
   previous marked-ready version. An exact retry reuses the ready version.
9. A serialized cleanup task protects the newest two ready versions when they
   exist and removes older or incomplete ClickHouse copies.
10. The high-water mark advances only after durable writes and required checks
    succeed.

## Streaming flow

Spark is the **only compute engine in the streaming pipeline**. Trino does not
consume Redpanda topics, manage streaming state, or participate in Spark's
checkpoint/commit cycle.

1. The IRIS bridge receives AMQP messages; the plant simulator emits live IoT events.
2. Producers attach an event ID, event time, observed time, source revision, schema version, and trace ID.
3. Redpanda persists the event and exposes consumer lag.
4. Spark validates schemas, quarantines malformed records, deduplicates by idempotency key, applies event-time watermarks, and checkpoints offsets/state locally.
5. Spark commits bounded micro-batches to Iceberg on R2.

That is the end of the streaming pipeline. Separately, after an Iceberg snapshot
has been committed, Trino may read it for interactive queries and finite dbt
runs. Trino is therefore a downstream query engine, not a second stream engine.

## Batch/stream reconciliation

The two paths must converge rather than create separate truths:

- REST/API backfills repair missing IRIS windows.
- API and stream records use the same source natural keys and publication/revision fields.
- Revised records remain auditable; the validated current view selects the latest accepted revision.
- A reconciliation job compares counts, time ranges, keys, and important aggregates by source period.
- Replaying the same batch or stream segment must not change correct final totals.

## Catalog and R2 choice

Provisional choice: **Cloudflare R2 Data Catalog**, a managed Iceberg REST catalog attached to the R2 lakehouse bucket. Cloudflare publishes connection examples for both Trino and Spark, so it removes a local catalog service while preserving the standard Iceberg REST boundary.

This is not locked because R2 Data Catalog is in public beta. The first infrastructure milestone must prove:

- create namespace/table from Trino;
- append from Spark and read from Trino;
- concurrent commit behavior;
- `MERGE`, delete, schema evolution, and time travel;
- dbt temporary/incremental relations;
- stream restart from checkpoint without duplicates;
- acceptable local-to-R2 latency and small-file behavior.

The safe development posture is:

- use a private R2 bucket dedicated to Iceberg tables;
- scope R2 storage and Data Catalog permissions to that bucket and workload;
- use separate read-write ingestion credentials and read-only product/query credentials;
- keep the S3 key/secret and catalog OAuth token in a local ignored `.env` or secret mount;
- never bake credentials into images or commit Trino/Spark property files containing secrets;
- rotate credentials after any suspected exposure.

If the managed catalog is unavailable for the bucket jurisdiction or fails the smoke test, use Lakekeeper plus local PostgreSQL as the REST-catalog fallback. Iceberg's JDBC catalog is the narrower second fallback. Do not replace Iceberg or R2 until the failing boundary is isolated.

Cloudflare documents that R2 Data Catalog exposes the Iceberg REST interface at <https://developers.cloudflare.com/r2-data-catalog/>, provides a Trino configuration at <https://developers.cloudflare.com/r2-data-catalog/config-examples/trino/>, and currently does not support non-default-jurisdiction R2 buckets at <https://developers.cloudflare.com/r2-data-catalog/manage-catalogs/>. R2 uses an S3-compatible endpoint and `auto` region: <https://developers.cloudflare.com/r2/api/s3/api/>.

## Security boundaries

- Separate ingestion credentials from read-only product credentials.
- Use separate ClickHouse bootstrap, Airflow publisher, and read-only FastAPI
  accounts. The bootstrap credential is never passed to a workload.
- Restrict the R2 token to the project bucket and required operations whenever the integration permits.
- Enforce customer/site scope in the API and analytical access policy; never rely on a hidden UI control.
- Keep contract rates and internal maintenance notes out of customer-facing response models.
- Permit the AI layer to call only typed, read-only tools that re-check authorization.
- Record actor, tool, parameters, authorized scope, source snapshot, latency, and outcome in an audit trail.
- Include negative tests for cross-customer access, prompt injection, unsupported claims, and tool failures.

## Reliability and failure handling

- Exponential retry with jitter for transient APIs; dead-letter/quarantine for invalid data.
- Idempotency keys for both ingestion paths.
- Event-time watermarks and explicit late-data policy.
- Spark checkpoints on durable local volumes; broker offsets committed only after successful processing semantics are defined.
- Airflow retries only idempotent tasks and exposes failed data intervals.
- ClickHouse candidate rows are visible only after exact validation and a final
  ready marker. A failed attempt cannot replace the previous ready version.
- Airflow then protects the new version and removes older ready versions and
  invisible failed attempts through the same one-slot writer pool.
- Product requests join rows to that ready marker and apply tenant scope before
  optional filters. A page pins its requests to one returned data version.
- Select exactly one owner for snapshot expiration and compaction. If R2 Data Catalog maintenance is enabled, Airflow observes it and does not run conflicting rewrites.
- Implemented checks cover ClickHouse health, API liveness, serving-copy
  integrity, publication age, identity mode, and product readiness. IRIS age,
  broker lag, Iceberg commit age, and dbt freshness monitoring remain Phase 3
  work.

## Local resource strategy

Run services through Compose profiles so the laptop does not need every optional component at once:

- `query`: Trino, started only for finite SQL or analyst work
- `batch`: Trino, ClickHouse, Airflow, and the dbt/publisher dependencies
- `stream`: Redpanda, bridge/simulator, and Spark local mode; no Trino
- `product`: ClickHouse, FastAPI, and the web app; no Trino

ClickHouse was added after direct Trino/R2 product latency was measured. It is
a local serving copy, not a second lakehouse. Kubernetes and multi-worker
Spark/Trino remain out until their operational need is measured.

## Trade-offs

| Choice | Benefit | Cost/risk |
|---|---|---|
| R2 plus managed catalog | Uses the available account and removes a local catalog container | Remote dependency and beta catalog; compatibility must be verified |
| Iceberg | Open multi-engine tables, snapshots, evolution, streaming/batch convergence | Catalog and table maintenance are additional systems |
| Trino for dbt | Clear SQL path and good portfolio explanation | Not the stream processor |
| ClickHouse for frontend serving | About 0.02-second local API calls over versioned native tables | Duplicates tested product data and adds a release step |
| Ready marker written last | Partial or invalid candidates stay invisible | Cleanup must be serialized and remove markers before rows |
| Checkpointed incremental ClickHouse publication | Sends only new and updated rows into ClickHouse while validating the complete result | Still reads the complete marts from Trino, compares every row, and copies unchanged rows inside ClickHouse |
| Spark as the only stream processor | One owner for event-time state, checkpoints, deduplication, and Iceberg stream commits | Additional JVM/container footprint |
| Redpanda single node | Kafka semantics with lighter local operations | Not representative of broker high availability |
| Synthetic plant data | Reproducible edge cases and legal clarity | Must be labelled; cannot prove real plant integration |
| Elexon GB data | Free REST plus genuine push service | Introduces GB-specific settlement concepts |

## Revisit as the system grows

- R2 Data Catalog beta suitability and authentication after the smoke test
- Continuous CDC only if the business needs changes between tested batch
  publications; revisit whether the current two-ready-version minimum needs a
  time-based client promise
- Spark sizing and tuning once event volume is measured; replacing it requires a new ADR
- Multi-tenant policy enforcement below the API layer
- Semantic layer choice after the first metrics are stable
- Managed compute/catalog services only after the local project is complete
