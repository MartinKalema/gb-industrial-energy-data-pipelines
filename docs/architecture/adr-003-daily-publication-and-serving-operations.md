# ADR-003: Daily publication and serving operations

**Status:** Accepted

**Date:** 2026-09-02

**Deciders:** Martin and project collaborator

## Context

The bounded steam-delivery pipeline was originally manual. That remains useful
for controlled replays and backfills, but it does not provide a regular data
release or a clear freshness promise. The ClickHouse serving copy also retained
every successful and failed publication attempt, and API readiness could not
distinguish recent data from an old but technically queryable version.

R2 and Iceberg must remain canonical. Any operating control added here must not
silently delete canonical evidence, weaken the existing validation gates, or
make an incomplete ClickHouse copy visible.

## Decision

Keep `steam_delivery_data_pipeline` as the manual replay and backfill workflow.
Add a separate `daily_steam_delivery_data_pipeline` that:

- runs at 12:00 `Europe/London`;
- derives the previous completed London operating date from Airflow's resolved
  data interval;
- triggers and waits for the existing bounded pipeline;
- requires the complete child workflow, including ClickHouse retention, to
  finish by 16:00 London; and
- uses `catchup=False`, with explicit gaps repaired through the manual DAG.

Add these serving controls:

- API readiness checks repository access, ready-marker row counts, identity
  mode, and publication age;
- the Compose product profile treats a publication older than 30 hours as
  stale, while the daily DAG owns the exact operating-date deadline;
- a bounded command produces alert-ready and local capacity evidence;
- each successful publication is followed by serialized ClickHouse cleanup;
- cleanup never deletes the newest two ready versions, removes old markers
  before their rows, and removes unmarked failed attempts; and
- recovery rebuilds the disposable ClickHouse copy through the existing tested
  pipeline rather than treating ClickHouse as a backup.

Raw R2 evidence, quarantine evidence, Iceberg data, and Iceberg snapshots are
outside the ClickHouse cleanup boundary.

Production identity, alert delivery, canonical-data retention, backup location,
RPO, and RTO are not invented locally. Demo identity is reported as a warning,
and disabling demo mode leaves readiness failed until a real identity provider
is configured.

## Options considered

### Option A: Put a schedule directly on the manual DAG

| Dimension | Assessment |
|---|---|
| Complexity | Low |
| Replay safety | Weaker because scheduled and manually chosen parameters share one entry point |
| Operator clarity | Lower |
| Backfill behavior | Easy to trigger accidentally with clock-derived dates |

Rejected because the manual DAG deliberately requires an operator to choose a
date range and generation timestamp.

### Option B: Add a small scheduled wrapper — selected

| Dimension | Assessment |
|---|---|
| Complexity | Medium |
| Replay safety | Strong; the proven bounded pipeline remains unchanged |
| Operator clarity | Clear daily versus manual entry points |
| Clock changes | Explicitly tested with London 23-, 24-, and 25-hour intervals |

### Option C: Use an external scheduler

| Dimension | Assessment |
|---|---|
| Complexity | Medium to high for this local project |
| Independence | Strong |
| Evidence | Splits scheduling and run history across systems |
| Current need | Not justified |

Rejected for the local implementation because Airflow already owns finite
workflows and their operating evidence.

## Trade-off analysis

| Choice | Benefit | Cost or limitation |
|---|---|---|
| Noon daily schedule | Allows the current synthetic next-morning corrections to exist before extraction | Data is intentionally not near-real-time |
| 16:00 deadline | Gives the complete bounded child workflow time to finish and makes lateness visible | It measures when the child run becomes observable as successful, not the ready marker's own timestamp; the local laptop must be running and missed days need an explicit backfill |
| 30-hour API backstop | Prevents an old ready marker remaining healthy indefinitely | It is less exact than checking the expected operating date, which remains the daily DAG's job |
| Protect the newest two ready versions | Preserves current and previous good product copies once both exist; a first publication naturally has only one | It is not a time-based promise for long-lived browser sessions |
| Marker-first deletion | An interrupted cleanup cannot leave a selectable partial version | Cleanup requires synchronous mutations and the exclusive writer pool |
| Fail closed without production identity | Prevents demo headers being mistaken for real authentication | A production deployment cannot become ready until an identity provider is selected and implemented |

## Consequences

- The working architecture remains a batch lakehouse, not Lambda or Kappa.
- Airflow now shows a separate daily freshness result while preserving the
  manual replay path.
- Stale or damaged serving data produces HTTP 503 readiness evidence.
- Ready-version growth is bounded by count while canonical evidence remains
  untouched. Invisible failed candidates are removed by cleanup after the next
  successful publication.
- The operational probe can be connected to a future monitor without embedding
  vendor-specific paging code in the API.
- Docker/Airflow integration must still be rerun whenever the pinned Airflow
  provider changes.

## Action items

1. Run the live DagBag and one scheduled-wrapper verification when Docker is
   available.
2. Select the production identity provider and accept issuer, audience, tenant
   claim, key-rotation, and failure rules.
3. Select the alerting service, owners, escalation path, and retained metrics
   system.
4. Accept canonical R2/Iceberg retention, backup, RPO, and RTO rules before a
   production claim.
5. Run the bounded capacity command with representative production data and an
   agreed latency/error target.
