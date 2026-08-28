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

STREAM
Elexon IRIS AMQP ----> local bridge ----\
                                         -> local Redpanda
Live plant simulator -------------------/          |
                                         Spark Structured Streaming
                                                    |
                                             Iceberg on R2

QUERY (after Spark commits an Iceberg snapshot)
Analyst / dbt / product API -> Trino -> Iceberg on R2
```

All compute services run locally. Cloudflare R2 is the only object-storage
runtime, and its managed Iceberg REST catalog remains the remote metadata
service. There is no MinIO or local object-store runtime. Local volumes hold
only Airflow state/work files, Spark checkpoints, and downloaded dependencies.
The public source APIs also remain remote.

## Repository map

- `docs/discovery/` — business problem, source feasibility, scope, and requirements
- `docs/modeling/` — our collaborative dimensional-modeling workshop and decisions
- `docs/architecture/` — system design and job-capability coverage
- `ingestion/batch/` — historical API ingestion and replay/backfill logic
- `ingestion/stream/` — IRIS bridge, telemetry producer, and stream processing
- `orchestration/` — Airflow DAGs for finite workflows and maintenance
- `transformations/` — dbt project and governed analytical models
- `apps/` — Python API and TypeScript web application
- `ai/` — read-only assistant tools and proactive evaluation suite
- `infrastructure/` — local Docker Compose and engine configuration

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
| Current implementation phase | Phase 2 batch vertical slice — source load, dimensional mart, and restartable coverage-to-dbt orchestration implemented; presentation and FUELHH remain |

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
