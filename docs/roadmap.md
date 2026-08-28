# Incremental roadmap

Every phase ends with a runnable or reviewable artifact. Later features do not block the first vertical slice.

## Phase 0 — discovery and feasibility

- Accept or revise the business problem.
- Verify Elexon REST with a small backfill.
- Register for IRIS and capture a small sample.
- Verify R2 read/write using bucket-scoped credentials.
- R2 Data Catalog enabled on `gb-industrial-thermal-energy-lakehouse-dev`; raw roundtrip, create, stream append, checkpoint recovery, cross-engine read, merge, delete, schema evolution, and time travel are proven.
- Fall back to a local Iceberg REST catalog only if the managed-catalog spike fails.
- Record source licenses and attribution.

**Demo:** one external record and one simulated plant record can be reproduced from raw evidence.

## Phase 1 — dimensional workshop

**Status: complete on 2026-08-27.**

- Select steam delivery as the first process or replace it.
- Explain the business problem and process in plain English, including how batch
  and streaming support it.
- Identify the operator and historical decisions the process must support.
- Preserve the original workshop questions and record agreements separately in
  the decision log.
- Define the authoritative source and the event that counts as official steam
  delivery.
- Declare grain, dimensions, facts, units, timestamps, corrections, and security.
- Define commitment, availability, revenue, additivity, missing-data, and
  provisional/final rules.
- Build the first bus-matrix row and metric contracts.
- Define shared identity, revision, and convergence rules for batch and stream
  processing.
- Generate and hand-calculate reconciliation scenarios for interval excess,
  missing and late delivery, and corrected shared meter boundaries.
- Create exact expected-result fixtures.

**Completion evidence:**
[Phase 1 completion report](modeling/07-phase-1-completion-report.md),
[decision log](modeling/decision-log.md), and
[reconciliation scenarios](modeling/06-reconciliation-scenarios.md).

**Phase boundary:** the logical model is accepted; physical Iceberg/dbt tables
and executable fixtures begin in Phase 2.

**Demo:** walk through three scenarios by hand and calculate accepted totals.

## Phase 2 — batch vertical slice

**Status: in progress from 2026-08-27.**

**Source-contract milestone:** complete on 2026-08-28. The nine synthetic
business-source contracts and the separate Elexon FUELHH external-batch
sidecar contract are accepted. Twelve Draft 2020-12 schemas, the deterministic
nine-source JSONL generator, its manifest, and executable contract/scenario
tests are implemented.

**Bounded source-load milestone:** implemented and verified on 2026-08-28. The
manual Airflow DAG plans a maximum 31-day range,
generates the nine sources, writes immutable raw evidence to R2, validates and
quarantines rows, loads accepted revisions through Trino into typed Iceberg
source tables, and reconciles raw-to-Iceberg counts. The real first run inserted
313 accepted rows into nine tables with zero quarantine/conflicts; the exact
replay reused every R2 artifact and all 313 Iceberg identities without an
insert or conflict.

**Dimensional-mart milestone:** implemented and live-verified on 2026-08-28.
The full-rebuild dbt project preserves all source revisions in staging, applies
current and source-knowledge revision precedence, builds eight logical
dimensions plus five revision-audit companions, and publishes 96 current
delivery-point/half-hour facts and 582 historical knowledge windows for the
one-day verification slice. The accepted reconciliation, shared meter-boundary,
missing-versus-zero, capacity-precedence, event-time relationship, grain,
lineage, and key rules are executable tests. Incremental processing remains a
measurement-driven later optimization.

- Resolve and record the Phase 2 source-contract entry decisions for deliverable
  capacity, shared-capacity allocation, commitment/contract revisions, and
  approved excess orders.
- Airflow backfills generated historical plant/business data and ingests Elexon
  FUELHH as a separate external-pipeline demonstration; FUELHH is not joined to
  the steam-delivery mart.
- Store raw evidence and validated Iceberg tables on R2.
- Use dbt-trino to build and test the first dimensional mart.
- Add freshness, lineage, reconciliation, and a basic historical dashboard/API.

**Demo:** rerun a date range idempotently and explain one historical delivery shortfall.

**Working documents:**
[Phase 2 source-contract workshop](modeling/08-phase-2-source-contract-workshop.md)
and [source-contract decision log](modeling/source-contract-decision-log.md),
followed by the
[physical source contracts](modeling/09-phase-2-physical-source-contracts.md).
The executable handoff is in the
[Phase 2 source implementation](architecture/phase-2-source-implementation.md).
The implemented batch boundary is described in the
[bounded Airflow-to-R2-and-Iceberg runbook](architecture/bounded-airflow-r2-iceberg-pipeline.md).
The implemented analytical boundary is described in the
[steam-delivery dbt dimensional-mart runbook](architecture/steam-delivery-dbt-dimensional-mart.md).

## Phase 3 — streaming vertical slice

- Run Redpanda locally.
- Stream live simulator events and bridge Elexon IRIS AMQP messages.
- Use Spark Structured Streaming for validation, deduplication, watermarks, and Iceberg commits.
- Add lag, checkpoint, quarantine, and gap-repair monitoring.

**Demo:** inject a late/out-of-order event and show correct reconciliation with batch truth.

## Phase 4 — reliability and commercial depth

- Add asset hierarchy, operating modes, outages, and work orders.
- Extend Phase 1 contractual service availability into asset-level physical and
  dispatchable availability with planned/unplanned downtime detail.
- Deepen contracts and tariffs beyond the Phase 1 interval rules, and add
  electricity cost, charging attribution, and commercial adjustments.

**Demo:** trace a plant outage to availability, shortfall, penalty, and revenue impact.

## Phase 5 — full-stack product and security

- Build role-specific operator, commercial, and customer experiences.
- Enforce customer/site scope in FastAPI and analytical policies.
- Add audit trails, negative authorization tests, health checks, and runbook.

**Demo:** the same request returns appropriately scoped results for three roles; cross-customer access fails.

## Phase 6 — governed self-service

- Let analysts define bounded metrics or alert rules through typed forms/specifications.
- Preview and validate changes.
- Publish low-risk personal rules directly; require review for shared, financial, or customer-visible definitions.

**Demo:** a domain expert safely adds an alert without arbitrary production SQL.

## Phase 7 — guarded AI investigation

- Expose only typed, read-only metric/incident tools.
- Cite data periods and Iceberg snapshots.
- Evaluate golden questions, grounding, authorization, prompt injection, tool errors, latency, and cost.

**Demo:** explain a delivery miss correctly, decline an unsupported claim, and block a cross-customer prompt.

## Optional extensions

- ClickHouse only if measured operational-query latency warrants a serving layer.
- MCP exposure of the same governed read-only tools.
- Day-ahead recommendation model after metric definitions are trustworthy.
- Production identity provider and catalog authorization.
