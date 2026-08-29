# ClickHouse frontend serving layer

## Outcome

The historical steam-delivery product reads a fast, versioned copy of the
tested dimensional mart from ClickHouse. Cloudflare R2 and Apache Iceberg
remain the canonical data. Trino and dbt remain the governed transformation
path. If ClickHouse is empty or lost, the serving copy can be rebuilt from the
tested mart without changing the business result.

ClickHouse does not calculate a second definition of delivery, shortfall, SLA,
availability, or revenue. The publisher copies the governed fields needed by
the API, preserves exact decimals, timestamps, nulls, and statuses, and checks
that the copy matches its source.

## Why it exists

Direct product queries through local Trino and the remote R2 catalog were
correct but too slow for an interactive page. Measurements from the local
verification environment included about 26 seconds for context, 6 seconds for
summary, and 14 seconds for the complete server-rendered page. After publishing
the same product data to ClickHouse, API calls were about 0.02 seconds and the
server-rendered page was about 0.17 seconds.

These are project measurements, not general database benchmarks. They explain
why a serving database was added after latency was measured.

## Data ownership and flow

```text
Cloudflare R2 / Iceberg                 canonical evidence and tables
          |
          v
Trino + dbt                             governed transformations and tests
          |
          | test_complete_dimensional_mart_with_dbt succeeds
          v
Airflow task
publish_tested_dimensional_mart_to_clickhouse
          |
          | copy, validate, then publish a ready marker
          v
ClickHouse native MergeTree tables      rebuildable frontend copy
          |
          | read-only, tenant-scoped queries
          v
FastAPI -> Next.js -> browser
```

Trino is needed while the finite batch pipeline builds, tests, and publishes a
mart. It is not needed by the `product` profile after a successful publication.
The streaming decision is unchanged: Spark Structured Streaming remains the
only streaming compute engine.

## What is published

The `industrial_energy_serving` database has three native ClickHouse
`MergeTree` tables:

| Table | Purpose |
|---|---|
| `delivery_interval_current` | One denormalized product row for each current delivery-point/30-minute result, plus the governed fields needed for summaries. |
| `delivery_interval_history` | Denormalized source-knowledge windows used to explain how an interval changed. |
| `data_publication` | The ready marker and evidence for one validated product-data version. |

Current and history rows include a `load_attempt_id`. A marker's
`publication_id` is the same value. The API can see a candidate only when this
relationship exists:

```text
candidate.load_attempt_id = ready_marker.publication_id
```

The native tables are deliberately optimized for API reads. They are not a new
source of truth and are not queried directly from Iceberg during a page request.

## Safe publication process

Airflow runs `publish_tested_dimensional_mart_to_clickhouse` only after
`test_complete_dimensional_mart_with_dbt` succeeds. The task shares the
one-slot `iceberg_writer` pool with the source, coverage, and dbt tasks. Together
with the DAG's one-active-run limit, this prevents another project writer from
changing the tested Iceberg mart while the publisher's separate Trino export
queries are reading it.

For each publication, the task:

1. Creates missing serving tables, then verifies every persisted table's
   columns, engine, and sorting key against the supported schema contract.
2. Reads the certified current and history projections through Trino.
3. Calculates a source fingerprint from the tested dbt result, source coverage,
   table contract, row counts, date coverage, and deterministic content hashes.
4. Reuses an existing ready publication only after re-reading its current and
   history rows and confirming that their counts and hashes still match.
5. Otherwise creates a new `load_attempt_id` and inserts candidate rows into
   the two serving tables.
6. Reads those candidate rows back and checks the column contract, null rules,
   exact decimal and timestamp representation, row counts, unique keys,
   non-empty tenant scopes, reporting-date coverage, and content hashes.
7. Inserts the `data_publication` ready marker as the final write.

This final-marker rule gives the workflow its failure behavior:

| Event | What users see |
|---|---|
| Candidate load succeeds and every validation passes | The new version becomes available. |
| Loading stops partway through | No marker exists, so the partial rows are invisible. |
| Validation fails | No marker exists, so the failed candidate is invisible. |
| An exact successful publication is retried | The existing ready version is reused. |
| A marker remains but its serving rows are damaged | The retry builds and validates a fresh publication instead of trusting the damaged version. |
| A persisted table no longer matches the supported schema | Publication stops with a migration/rebuild error before copying data. |
| Any new publication fails | The API continues serving the previous ready version. |

Candidate rows are immutable. A later cleanup task may remove unmarked attempts
and old ready versions after a retention policy is accepted; cleanup is not part
of publication correctness.

## Consistent API reads

Without a version header, `GET /api/v1/context` resolves the newest ready
publication that the actor may query. Its response includes:

- `data_version`: the ClickHouse publication ID; and
- `data_published_at_utc`: when that ready marker was written.

The web application carries that `data_version` through pagination and detail
links, then sends it as `X-Product-Data-Version` on context, summary,
interval-list, and interval-history requests. The API validates the header and
queries that exact ready version. This prevents filters, pages, and detail
history from combining rows from two publications if a newer version becomes
ready between clicks. Without the header, those endpoints use the newest ready
publication.

Every serving query applies the ready-publication condition and the actor's
tenant scope before optional customer, site, date, status, or pagination
filters. ClickHouse improves serving latency; it does not replace FastAPI as the
authorization boundary.

## Credential roles

The local runtime uses three different ClickHouse accounts:

| Account | Credential | Responsibility |
|---|---|---|
| `clickhouse_bootstrap` | `CLICKHOUSE_BOOTSTRAP_PASSWORD` | Starts the local database and reconciles the two workload accounts. It is not passed to Airflow or the API. |
| `industrial_energy_publisher` | `CLICKHOUSE_PUBLISHER_PASSWORD` | Used only by Airflow to create, read, and insert the serving tables and ready marker. |
| `historical_delivery_api` | `CLICKHOUSE_API_PASSWORD` | Used only by FastAPI. It has `SELECT` access to the serving database and ClickHouse's read-only setting. |

Put three different high-entropy values in the ignored root `.env`. Never put
the real values in Git, Compose, or documentation.

## Local startup order

For a new or empty serving database:

1. Configure R2 and the three ClickHouse passwords in the ignored `.env`.
2. Start the batch profile. Compose starts ClickHouse and Trino before Airflow:

   ```bash
   docker compose --project-directory . -f infrastructure/compose.yaml \
     --profile batch up --build airflow
   ```

3. Trigger `steam_delivery_data_pipeline` in Airflow.
4. Confirm both `test_complete_dimensional_mart_with_dbt` and
   `publish_tested_dimensional_mart_to_clickhouse` succeeded.
5. Start the product profile:

   ```bash
   docker compose --project-directory . -f infrastructure/compose.yaml \
     --profile product up --build
   ```

The product profile starts ClickHouse, the read-only API, and the web product;
it does not start Trino. If no ready marker exists, the API process can be live
but its readiness check correctly fails.

## Verification evidence

The real local publication copied 96 current rows and 558 authorized history
rows. The destination counts and deterministic hashes matched the certified
Trino projections. Repeating the publication returned `reused` rather than
creating another version. Failure tests also prove that partial loads and
validation failures do not replace the last good version.

## Trade-offs

| Choice | Benefit | Cost or limitation |
|---|---|---|
| Rebuildable ClickHouse copy | Fast product reads without changing the canonical lakehouse | One more local service and a publication boundary to operate |
| Native `MergeTree` tables | Predictable interactive reads over product-shaped columns | Data is duplicated from the canonical mart |
| Immutable version plus final marker | Failed releases remain invisible and page requests can be pinned | Old candidates and versions need a later retention policy |
| Full snapshot publication first | Easy to reason about, compare, retry, and rebuild | Later data growth may justify incremental publication |
| Tenant checks in FastAPI and every query | Keeps authorization server-side and testable | ClickHouse is not currently the policy authority by itself |

Incremental publication, old-version cleanup, production identity, and managed
ClickHouse are future decisions. They do not change the current rule: R2 and
Iceberg are canonical, dbt owns business definitions, and only a fully validated
ClickHouse publication is visible to the product.
