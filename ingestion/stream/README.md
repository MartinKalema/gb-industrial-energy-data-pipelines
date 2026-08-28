# Streaming ingestion

This area will contain long-running local services:

- Elexon IRIS AMQP-to-Redpanda bridge
- live stateful thermal-battery/steam-plant simulator
- schemas and idempotency-key definitions
- Spark Structured Streaming job
- quarantine, checkpoint, lag, and recovery logic

Airflow may deploy, health-check, or recover these services, but it will not schedule one task per event or hold an infinite task open.

The stream must carry at least `event_id`, `event_time`, `observed_at`, `published_at` where applicable, `source_revision`, `schema_version`, and `trace_id`.
