# Project working agreements

- Keep the project centered on the accepted business decision, not on collecting technologies.
- Before modeling any new business process, create and review a plain-English explainer using `docs/modeling/business-process-explainer-template.md`. It must cover the real-world setting, business problem, process steps, a numerical example, and the distinct roles of batch and streaming where applicable.
- Preserve every original question under a workshop's `Decisions for us to make together` section. Record agreements separately under `Accepted decisions` and in `docs/modeling/decision-log.md`; never replace or remove the original question when its status changes.
- Do not finalize fact/dimension schemas until the corresponding decisions are accepted in `docs/modeling/decision-log.md`.
- Keep Cloudflare R2 as canonical object storage and Apache Iceberg as the only table format unless an ADR changes that decision.
- Use Spark Structured Streaming as the only streaming compute engine. Use Trino only for finite dbt and interactive SQL over committed Iceberg snapshots. Any change requires an ADR.
- Airflow owns finite workflows. Long-running brokers, producers, and stream processors run as services.
- Batch and stream records must reconcile using shared natural keys, source revisions, and event/publication/ingestion timestamps.
- Never commit credentials, tokens, raw private data, generated bulk data, checkpoints, or service volumes.
- Label simulated plant/business data clearly and preserve attribution for public data.
- Enforce authorization in the API/tool boundary and test denied paths; the UI alone is never a security boundary.
- Prefer a thin end-to-end vertical slice with tests and observability before adding optional components.
