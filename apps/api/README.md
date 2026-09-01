# Historical steam-delivery performance API

This FastAPI service is the read-only product boundary for the governed
steam-delivery dimensional mart. It lets a commercial manager investigate the
fictional portfolio and lets each fictional customer see only its own customer,
sites, and delivery points.

The API does not accept SQL and does not read raw, quarantine, or validated
source data. In the product profile, parameterized ClickHouse queries read only
the last successfully published current and historical serving tables. Those
tables are a rebuildable copy of the tested mart; R2 and Iceberg remain
canonical.

## Run locally

ClickHouse must be running, and a ready Airflow publication must exist and be
younger than the configured age limit. The normal local path is the root
Compose `product` profile. For an intentionally static historical demonstration
only, set `PRODUCT_MAX_PUBLICATION_AGE_SECONDS=0`; do not use that as a
production default. To run only the API process outside Compose, first export
the ignored `.env` values, then map the read-only password to the API setting:

```bash
uv sync --frozen
PRODUCT_DEMO_MODE=true \
PRODUCT_REPOSITORY_BACKEND=clickhouse \
PRODUCT_MAX_PUBLICATION_AGE_SECONDS=108000 \
CLICKHOUSE_HOST=127.0.0.1 \
CLICKHOUSE_USER=historical_delivery_api \
CLICKHOUSE_PASSWORD="$CLICKHOUSE_API_PASSWORD" \
uv run uvicorn apps.api.app:app --host 127.0.0.1 --port 8000
```

Check the process and its ready-publication dependency separately:

```bash
curl --fail http://127.0.0.1:8000/health/live
curl --fail http://127.0.0.1:8000/health/ready
```

OpenAPI is available at <http://127.0.0.1:8000/docs>.

## Demo identities

Demo mode is disabled by default. Set `PRODUCT_DEMO_MODE=true` only for local
authorization testing and send one exact `X-Demo-Actor` value:

| Actor | Server-side scope |
|---|---|
| `commercial-manager` | All governed fictional customer scopes |
| `customer-cust-001` | `TENANT-CUST-001`, `CUST-001`, `SITE-001`, `DP-001` |
| `customer-cust-002` | `TENANT-CUST-002`, `CUST-002`, `SITE-002`, `DP-002` |

This selector is not production authentication. Missing and unknown actors fail
closed. Explicit collection filters outside a customer actor's claims return a
generic `403`; inaccessible interval keys return the same `404` as unknown keys.
Every ClickHouse query independently requires a ready publication and applies
the immutable tenant scope before caller-supplied filters.

A production deployment must keep demo mode off and replace this adapter with a
verified identity provider. UI controls are never an authorization boundary.

## Reporting-date and measure rules

`start_date` and `end_date` are inclusive `Europe/London` operating dates, not
UTC-midnight instants. The API filters the fact's `date_key`; therefore reporting
date `2026-08-26` correctly includes its first BST interval at
`2026-08-25T23:00:00Z`. Interval event timestamps remain timezone-aware UTC, and
responses also provide local boundaries, UTC offset, and daylight-saving state.

The maximum reporting range is 31 inclusive days. Interval pages are ordered by
UTC start, delivery point, and fact interval key. `page` is capped at 100,000 and
`limit` at 200 for this bounded release.

Exact decimal values cross the API as JSON strings. Missing governed values are
`null`; they are never converted to zero. Known delivered, shortfall, excess,
billable, and gross-revenue subtotals can remain visible with a provisional
status. Official delivery completeness, SLA, and availability percentages are
`null` until their accepted completeness gates pass. Official penalty and net
revenue are also `null` until delivery and financial results are complete.

## Endpoint contract

All `/api/v1` endpoints require `X-Demo-Actor` in the local profile.

| Endpoint | Contract |
|---|---|
| `GET /health/live` | The API process can answer HTTP. |
| `GET /health/ready` | Identity mode, repository access, ready-publication row counts, and configured publication age are safe for traffic. Returns detailed technical evidence with HTTP `200` or `503`. |
| `GET /health/metrics` | Process-local uptime, request, error, and duration counters without customer or governed-value labels. |
| `GET /api/v1/context` | Actor, authorized customer/site/delivery-point options, available reporting dates, `data_version`, and `data_published_at_utc`. |
| `GET /api/v1/delivery-performance/summary` | Governed counts, known subtotals, completeness gates, official percentages, financial state, and freshness. |
| `GET /api/v1/delivery-performance/intervals` | Stable, bounded page of current half-hour results and all relevant data/result states. |
| `GET /api/v1/delivery-performance/intervals/{interval_key}/history` | Up to 200 authorized source-knowledge windows using revision-audit descriptions. `truncated=true` explicitly reports that older revisions were omitted. Optional `as_of` is an offset-aware timestamp. |

Summary and interval-list parameters are:

- required `start_date` and `end_date` in `YYYY-MM-DD` form;
- optional `customer_id`, `site_id`, and `delivery_point_id`;
- optional investigation status: `final`, `provisional`, `missing`, `corrected`,
  `shortfall`, or `excess`; and
- `page` and `limit` on the interval list.

The context, summary, interval-list, and interval-history endpoints accept the
optional `X-Product-Data-Version` header. The web reads `data_version` from the
first context response, carries it through pagination and detail links, and
sends it on every following request so one investigation uses one immutable
ready publication. Without the header, the API uses the newest ready
publication. A missing version returns a bounded
`data_version_unavailable` error; malformed version text returns
`invalid_data_version`.

Example:

```bash
curl --get http://127.0.0.1:8000/api/v1/delivery-performance/summary \
  --header 'X-Demo-Actor: customer-cust-002' \
  --header 'X-Request-ID: walkthrough-001' \
  --data-urlencode 'start_date=2026-08-26' \
  --data-urlencode 'end_date=2026-08-26' \
  --data-urlencode 'customer_id=CUST-002'
```

Successful and error responses echo a safe `X-Request-ID`. Errors use:

```json
{
  "detail": {
    "code": "authorization_denied",
    "message": "The requested scope is not allowed"
  }
}
```

Structured request logs contain the request ID, actor, resolved tenant scope,
requested customer/site filters, route, status, and duration. They intentionally
exclude measurements, contract values, credentials, and raw evidence.
Health responses also exclude those values. See the
[API operational checks](../../docs/operations/api-production-readiness.md) for
the 30-hour Compose freshness backstop, alert exit codes, capacity-evidence
command, and production gaps.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `PRODUCT_DEMO_MODE` | `false` | Enables the explicit local demo-identity adapter. |
| `PRODUCT_REPOSITORY_BACKEND` | `trino` | Repository implementation. The Compose product profile sets `clickhouse`; `trino` remains for parity and reference. |
| `CLICKHOUSE_HOST` | `clickhouse` | Serving database host. Use `127.0.0.1` outside Compose. |
| `CLICKHOUSE_HTTP_PORT` | `8123` | ClickHouse HTTP port. |
| `CLICKHOUSE_USER` | `historical_delivery_api` | Dedicated read-only product account. |
| `CLICKHOUSE_PASSWORD` | none | Required when the backend is `clickhouse`; Compose maps it from `CLICKHOUSE_API_PASSWORD`. |
| `CLICKHOUSE_SECURE` | `false` | Use TLS for the ClickHouse HTTP connection. |
| `PRODUCT_CLICKHOUSE_DATABASE` | `industrial_energy_serving` | Validated lower-case serving database identifier. |
| `PRODUCT_CLICKHOUSE_TIMEOUT_SECONDS` | `20` | ClickHouse connection/transport timeout. |
| `PRODUCT_CLICKHOUSE_QUERY_TIMEOUT_SECONDS` | `60` | Server-enforced query execution limit. |
| `PRODUCT_MAX_QUERY_DAYS` | `31` | Maximum inclusive reporting-date range. |
| `PRODUCT_MAX_PAGE_SIZE` | `200` | Configurable page cap, never above 200. |
| `PRODUCT_MAX_PUBLICATION_AGE_SECONDS` | `0` outside Compose; `108000` in the Compose product profile | Maximum ready-publication age. `0` disables only this check; production must use an accepted non-zero freshness limit. |

The database is configurable for isolated tests, but relation names are fixed
in code. Identifier validation prevents configuration from introducing
arbitrary SQL. The retained Trino backend still uses `TRINO_*` and
`PRODUCT_TRINO_*` settings for parity checks; it is not the frontend-serving
path in Compose.

## Verify

```bash
uv run pytest -q tests/api
docker build -f apps/api/Dockerfile -t historical-delivery-api .
uv run python -m apps.api.operational_check --url http://127.0.0.1:8000/health/ready
```

The focused suite verifies identity failures, cross-tenant denial, tenant and
ready-publication predicates, parameter binding, version pinning,
revision-history authorization, reporting-date bounds, pagination bounds,
readiness, bounded queries, history truncation, privacy-safe failure logging,
exact aggregate gates, exact decimal strings, and the difference between a
missing value and a real zero.

The operational check prints JSON and exits non-zero when readiness, the
allowed error rate, or the p95 latency limit fails. A bounded concurrent mode
records repeatable local capacity evidence. It is not a substitute for a
production load test with representative traffic.

See the
[ClickHouse frontend serving architecture](../../docs/architecture/clickhouse-frontend-serving-layer.md)
for publication, retry, failure-isolation, and credential details.
