# Great Britain Industrial Thermal Battery Operations and Steam Delivery Intelligence Platform

This portfolio project models a fictional operator of thermal batteries at
British industrial sites. The platform will answer one central question:

> Did each site meet its steam commitment, and what were the electricity-cost, carbon, availability, and revenue consequences of when it charged?

The project completed its first dimensional-modeling workshop on 2026-08-27.
The grain, dimensions, facts, metric contracts, and expected-result scenarios
are accepted. Phase 2 now has executable source contracts, a deterministic
fictional-data generator, and an Airflow path that saves the original files in
R2, checks every row, saves failed rows separately, and loads accepted rows
into typed Iceberg tables. A real R2/Trino/Iceberg run and exact replay passed
on 2026-08-28. The first dbt dimensional mart is also implemented: it produces
96 current
delivery-point/half-hour facts and 582 source-knowledge history windows for the
verified one-day slice. The mart answers delivery, SLA, contractual
availability, and earned-revenue questions. Charging cost and carbon are
explicitly deferred rather than inferred from unrelated public electricity
data. After its seven source/control tasks, the date-range Airflow pipeline now
runs six smaller dbt tasks. These prepare and test the loaded data and delivery
calculations, build the two facts and dimensions, and then test the complete
dimensional mart. If one dbt task fails, Airflow retries that task without
repeating the earlier successful dbt tasks.

The first read-only historical delivery product is now implemented on top of
that mart. A FastAPI boundary enforces demo customer scope and a server-rendered
Next.js interface lets commercial and customer personas investigate known,
provisional, final, missing, and corrected interval results. The interface
shows missing governed values as unavailable; it never changes them to zero.
The tested mart is published to native ClickHouse tables before the product
uses it. The first publication copies the complete tested mart. Later
publications compare it with the last ready version, reuse unchanged rows inside
ClickHouse, and send only new or changed row payloads to ClickHouse. R2 and
Iceberg remain the canonical data; ClickHouse is a rebuildable serving copy that
removes lakehouse-query startup time from an interactive page request.

## Why this project

One coherent product can demonstrate:

- batch ingestion and genuine event streaming;
- an R2-backed, Iceberg lakehouse queried by Spark and Trino;
- Airflow orchestration and dbt transformations/tests;
- industrial telemetry and time-series processing;
- a Python API and TypeScript product interface;
- governed metrics, tenant-aware security, and self-service workflows;
- a grounded AI assistant with authorization and regression evaluations;
- production practices such as observability, CI, data quality, and runbooks.

## Proposed data strategy

- **Real external batch sidecar:** Elexon FUELHH from the Insights REST API,
  used to prove public-API ingestion and publication-aware replay without
  entering the steam-delivery mart.
- **Future streaming context:** Elexon IRIS AMQP can exercise the live external
  path in Phase 3; it will remain separate unless a later business process
  establishes a valid analytical relationship.
- **Future carbon context:** a governed source and attribution method will be
  chosen only when the charging-cost/carbon process is modeled.
- **Synthetic private data:** thermal-battery telemetry, steam meters, customers, contracts, commitments, maintenance, and billing. These sources are synthetic because real industrial records are proprietary.

## Local-first architecture

```text
BATCH (implemented date-range pipeline)
Airflow -> deterministic source generator -> original files in R2
        -> row checks -> accepted files / failed rows saved separately in R2
        -> local Trino -> typed Iceberg source tables on R2
        -> dbt -> current and source-knowledge dimensional marts on R2
        -> final dbt tests -> checkpointed incremental publication
        -> versioned native ClickHouse serving tables

STREAM
Elexon IRIS AMQP ----> local bridge ----\
                                         -> local Redpanda
Live plant simulator -------------------/          |
                                         Spark Structured Streaming
                                                    |
                                             Iceberg on R2

QUERY (after Spark commits an Iceberg snapshot)
Analyst / dbt -> Trino -> Iceberg on R2
Historical web -> tenant-scoped product API -> ClickHouse ready publication
```

All compute services run locally. Cloudflare R2 is the only object-storage
runtime, and its managed Iceberg REST catalog remains the remote metadata
service. There is no MinIO or local object-store runtime. Local volumes hold
only Airflow state/work files, the rebuildable ClickHouse serving copy, Spark
checkpoints, and downloaded dependencies. The public source APIs also remain
remote.

## Repository map

- `docs/discovery/` — business problem, source feasibility, scope, and requirements
- `docs/modeling/` — our collaborative dimensional-modeling workshop and decisions
- `docs/architecture/` — system design and job-capability coverage
- `docs/operations/` — health, alert, capacity-evidence, and recovery runbooks;
  see the [daily publication ADR](docs/architecture/adr-003-daily-publication-and-serving-operations.md),
  [API readiness guide](docs/operations/api-production-readiness.md), and
  [ClickHouse retention and recovery guide](docs/operations/clickhouse-serving-retention-and-recovery.md)
- `ingestion/batch/` — historical API ingestion and replay/backfill logic
- `ingestion/stream/` — IRIS bridge, telemetry producer, and stream processing
- `orchestration/` — Airflow DAGs for finite workflows and maintenance
- `transformations/` — dbt project and governed analytical models
- `apps/` — Python API and TypeScript web application
- `ai/` — read-only assistant tools and proactive evaluation suite
- `infrastructure/` — local Docker Compose and engine configuration

## Historical steam-delivery product

The implemented product answers a focused historical investigation:

- Which customer, site, delivery point, and 30-minute interval missed its steam
  commitment?
- How much accepted delivery, shortfall, excess, billable energy, and known
  financial value is currently supported by the evidence?
- Is an SLA, availability, penalty/credit, or net result final, provisional,
  missing, or not applicable?
- Did a later source revision change what the business knew about an interval?

The final dbt checkpoint,
`test_complete_dimensional_mart_with_dbt`, certifies the mart before it is used
as product-ready data. Airflow then runs
`publish_tested_dimensional_mart_to_clickhouse`. That task copies the tested
current and history datasets in full when no usable base version exists. On
later runs it compares the complete mart with the last ready version, copies
unchanged rows inside ClickHouse, replaces only new or changed rows, and removes
deleted keys from the new candidate. It validates the complete result and makes
it visible only after all checks pass. This is scheduled incremental
publication after a batch, not continuous CDC. Start the local product profile
after a ready publication exists and is younger than the configured 30-hour
limit. For an intentionally static historical demonstration only, explicitly
set `PRODUCT_MAX_PUBLICATION_AGE_SECONDS=0` before starting it:

```bash
docker compose --project-directory . -f infrastructure/compose.yaml \
  --profile product up --build
```

With the default ports:

- web product: <http://127.0.0.1:3000>
- web health: <http://127.0.0.1:3000/healthz>
- API documentation: <http://127.0.0.1:8000/docs>
- API dependency readiness: <http://127.0.0.1:8000/health/ready>

The web server calls the API; the browser does not query ClickHouse and does not
construct the demo identity header. The API accepts bounded filters and runs
parameterized, read-only, tenant-scoped queries against the last ready serving
version. The API—not the interface—is the authorization boundary. Its context
response identifies that version, and the web pins the remaining page requests
to it with `X-Product-Data-Version` so one page cannot mix two publications.

The local profile has three demonstration personas:
`commercial-manager`, `customer-cust-001`, and `customer-cust-002`. The two
customer personas are restricted to their own fictional customer/site/delivery
point scope. This selector demonstrates authorization behavior only; it is not
a production login or identity provider.

Known subtotals may be shown while a result is provisional. Official SLA,
availability, penalty/credit, and net financial values remain unavailable until
their governed completeness and finality gates pass. An API `null` is rendered
as **Unavailable**, never as `0`. See the
[product architecture](docs/architecture/historical-steam-delivery-product.md),
[ClickHouse serving architecture](docs/architecture/clickhouse-frontend-serving-layer.md),
[API guide](apps/api/README.md), and [web guide](apps/web/README.md) for the
complete contract and local run instructions.

## Current decisions

| Decision | Status |
|---|---|
| Business problem | Accepted: steam commitments and charging consequences |
| Energy domain | Accepted: industrial thermal batteries and steam delivery |
| Market geography | Accepted: Great Britain |
| Object storage | Cloudflare R2 |
| Open table format | Accepted: Apache Iceberg |
| Iceberg catalog | Active: Cloudflare R2 Data Catalog beta; Spark/Trino engine smoke tests passed |
| Batch orchestration | Apache Airflow |
| Primary finite SQL engine | Accepted: Trino; not part of streaming |
| Stream processing | Accepted: Spark Structured Streaming only |
| Event broker | Redpanda (Kafka-compatible), provisional |
| Dimensional model | Phase 1 logical model accepted; full-rebuild dbt current/history mart implemented and live-verified |
| Source contracts | PSC-001 through PSC-011 accepted; 12 Draft 2020-12 schemas implemented |
| Synthetic evidence | Nine deterministic, revisioned JSONL sources implemented and contract-tested |
| Bounded source pipeline | Verified 2026-08-28: 313 inserted on the first real run, then 313 exact replays with no conflicts |
| Current implementation phase | Phase 2 batch vertical slice — source load, dimensional mart, restartable coverage-to-dbt-to-ClickHouse orchestration, daily scheduling, freshness/readiness checks, serving retention/recovery, and the focused historical product are implemented; FUELHH remains |
| Historical product API | Implemented: read-only, tenant-scoped FastAPI over ready ClickHouse versions sourced from governed current/history marts |
| Historical web product | Implemented: server-rendered commercial/customer investigation and revision history |
| Frontend serving database | Implemented: versioned native ClickHouse copy with validated, checkpointed incremental publication after final dbt tests |

Start with [the project brief](docs/discovery/project-brief.md), then review [data-source feasibility](docs/discovery/data-source-feasibility.md) and [Workshop 1](docs/modeling/01-business-process-workshop.md).

Before configuring storage, follow the [safe R2 bootstrap](docs/architecture/r2-bootstrap.md).

The first R2/Iceberg/Spark/Trino integration milestone has passed; see the
[Phase 0 feasibility results](docs/architecture/phase-0-feasibility-results.md)
and its [repeatable smoke tests](tests/smoke/README.md).

The first logical dimensional-modeling phase is complete; see the
[Phase 1 completion report](docs/modeling/07-phase-1-completion-report.md) for
the accepted decisions, artifacts, reconciliation evidence, deferred scope, and
Phase 2 implementation handoff.

The Phase 2 source layer is executable; see the
[source implementation handoff](docs/architecture/phase-2-source-implementation.md),
[machine-readable contracts](contracts/README.md), and
[synthetic generator](ingestion/batch/synthetic/README.md).

The bounded batch pipeline is documented in the
[Airflow-to-R2-and-Iceberg architecture and runbook](docs/architecture/bounded-airflow-r2-iceberg-pipeline.md).

The first dimensional mart is documented in the
[steam-delivery dbt architecture and runbook](docs/architecture/steam-delivery-dbt-dimensional-mart.md).

## Secrets

Never commit R2 or IRIS credentials. Copy `.env.example` to `.env` only on the local machine; `.env` and common secret paths are ignored by Git.
