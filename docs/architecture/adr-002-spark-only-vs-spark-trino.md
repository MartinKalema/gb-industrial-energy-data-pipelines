# ADR-002: Spark-only versus split Spark and Trino compute

**Status:** Accepted

**Date:** 2026-08-27

**Deciders:** Martin and project collaborator

## Context

Spark can execute batch SQL as well as Structured Streaming, so Trino is not a
technical requirement for this lakehouse. The choice is whether one engine
should own every workload or whether continuous event processing and finite
human-facing SQL should use engines specialized for those execution modes.

The project must run on one computer, support dbt dimensional models, demonstrate
late-event handling, and eventually serve analyst, API, and dashboard queries.

## Option A: Spark for the whole data platform

```text
batch APIs --------------------------\
Redpanda -> Spark Structured Streaming -> Iceberg on R2
dbt-spark / analyst / product SQL ---> Spark Thrift Server ---^
```

| Dimension | Assessment |
|---|---|
| Number of compute technologies | One |
| Local operations | Simpler dependency set |
| Streaming | Excellent fit |
| Heavy batch transformations | Excellent fit |
| Interactive SQL | Possible, but requires a continuously available SQL endpoint such as Spark Thrift Server |
| Workload isolation | Must use separate Spark applications/pools or share Spark resources |
| Iceberg interoperability proof | Does not demonstrate a second engine reading Spark commits |

Benefits:

- fewer engines, images, configuration files, metrics, and upgrade matrices;
- shared Spark SQL/DataFrame semantics across bounded and unbounded data;
- no Spark-to-Trino type, function, or Iceberg feature compatibility testing;
- strongest choice if the priority is the smallest locally operated platform.

Costs:

- dbt and interactive clients still need a long-running Spark SQL service;
- an ad-hoc query, dbt build, and stateful stream either compete in one Spark
  deployment or require separately sized Spark applications;
- the product query path is coupled to the heavier Spark runtime even when the
  request is a small SQL lookup;
- the project no longer proves that Iceberg is genuinely engine-independent.

## Option B: Spark for streams, Trino for finite SQL

```text
Redpanda -> Spark Structured Streaming -> committed Iceberg snapshots

dbt / analyst / API -> Trino -----------> committed Iceberg snapshots
```

| Dimension | Assessment |
|---|---|
| Number of compute technologies | Two |
| Local operations | More containers, configuration, memory, and testing |
| Streaming | Spark owns it exclusively |
| Heavy batch transformations | Spark when justified; otherwise SQL through Trino |
| Interactive SQL | Trino exposes a direct SQL query service |
| Workload isolation | Clear process and lifecycle boundary |
| Iceberg interoperability proof | Demonstrates shared open tables across engines |

Benefits:

- the long-running stateful stream and short-lived analyst/dbt queries have
  different owners and failure lifecycles;
- Trino supplies a direct ANSI-SQL-oriented endpoint for dbt, BI tools, APIs, and
  ad-hoc exploration;
- Spark can restart from its checkpoint without coordinating query workloads;
- reading Spark commits through Trino proves the value of Iceberg's open-engine
  contract rather than treating object storage as Spark-only storage.

Costs:

- two JVM engines consume more laptop resources if run simultaneously;
- both engines require compatible Iceberg/catalog configuration and tests;
- SQL types, functions, write semantics, and Iceberg feature support can differ;
- observability, upgrades, and troubleshooting cover an additional service.

## Decision

Keep Option B for this project, with a strict boundary:

- Spark is the only live stream processor.
- Trino never consumes Redpanda and is not required to run the streaming profile.
- Trino is started only for finite dbt, analyst, API, or dashboard work.
- Spark may handle an unusually heavy batch job, but ordinary dimensional SQL is
  submitted by dbt to Trino.

The strongest reason is workload isolation and a clean interactive SQL endpoint,
not that a lakehouse inherently needs two engines. If minimizing local components
is more important than interactive SQL and multi-engine interoperability, choose
Option A; it remains architecturally sound.

ADR-001 remains valid because Spark alone owns streaming.

## References

- Apache Spark Structured Streaming: <https://spark.apache.org/docs/latest/streaming/getting-started.html>
- Trino overview and use cases: <https://trino.io/docs/current/overview.html>
- dbt-spark adapter: <https://github.com/dbt-labs/dbt-adapters/tree/main/dbt-spark>
- dbt-trino adapter: <https://github.com/starburstdata/dbt-trino>
