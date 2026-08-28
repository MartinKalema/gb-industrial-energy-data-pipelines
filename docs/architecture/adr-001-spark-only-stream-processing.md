# ADR-001: Spark is the only streaming compute engine

**Status:** Accepted

**Date:** 2026-08-27

**Deciders:** Martin and project collaborator

## Context

The system needs a genuine local streaming path for Elexon IRIS and simulated
industrial telemetry. Both Spark and Trino are compute engines, but assigning
both to live event processing would duplicate responsibilities and make the
pipeline harder to explain and operate on one computer.

The stream requires event-time processing, late and out-of-order event handling,
deduplication, stateful windows, replay, checkpoints, and reliable micro-batch
commits to Iceberg.

## Decision

Spark Structured Streaming is the only compute engine in the streaming
pipeline:

```text
producers -> Redpanda -> Spark Structured Streaming -> Iceberg on R2
```

Trino never consumes the Redpanda stream. It begins work only after Spark has
committed an Iceberg snapshot:

```text
dbt / API / analyst -> Trino -> committed Iceberg snapshots
```

Airflow may monitor and recover the Spark service, but it will not run an
infinite stream as an Airflow task.

## Options considered

### Spark only for streaming — selected

| Dimension | Assessment |
|---|---|
| Complexity | Medium |
| Local cost | Medium JVM footprint |
| Streaming capability | Strong stateful and event-time support |
| Lakehouse fit | Direct Iceberg micro-batch commits |

### Trino-based polling or micro-batches

| Dimension | Assessment |
|---|---|
| Complexity | Low for SQL, higher for custom stream semantics |
| Local cost | Already required for dbt and queries |
| Streaming capability | Not the selected long-running stateful processor |
| Lakehouse fit | Strong for querying committed tables |

### Spark and Trino both processing the stream

Rejected because it creates two owners for streaming computation without a
business requirement that justifies the extra state, failure modes, and
reconciliation work.

## Consequences

- Spark owns schema validation, event-time state, watermarks, deduplication,
  checkpoints, quarantine routing, and Iceberg streaming writes.
- Redpanda owns durable buffering and replay; it performs no analytical compute.
- Trino remains useful, but only for finite dbt models, exploration, dashboards,
  and product queries over committed Iceberg snapshots.
- The first streaming demo can be understood and debugged as one processing path.
- Spark resource usage must be bounded for the local computer.

## Action items

1. Build one Spark Structured Streaming job consuming a Redpanda topic.
2. Prove restart from checkpoint without duplicate accepted events.
3. Verify that Trino sees each newly committed Iceberg snapshot.
4. Add batch/stream reconciliation after the first dimensional grain is agreed.
