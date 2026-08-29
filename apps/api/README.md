# Historical steam-delivery performance API

This FastAPI service is the read-only product boundary for the governed
steam-delivery dimensional mart. It lets a commercial manager investigate the
fictional portfolio and lets each fictional customer see only its own customer,
sites, and delivery points.

The API does not accept SQL and does not read raw, quarantine, or validated
source data. Parameterized Trino queries read only the current and historical
facts and their dimensions in `r2.industrial_energy_marts`.

## Run locally

The lakehouse services and dimensional mart must already be available.

```bash
uv sync --frozen
PRODUCT_DEMO_MODE=true \
TRINO_HOST=127.0.0.1 \
uv run uvicorn apps.api.app:app --host 127.0.0.1 --port 8000
```

Check the process and its Trino dependency separately:

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
Every Trino fact query independently joins its customer dimension and applies
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
| `GET /health/ready` | The current fact and customer authorization dimension are queryable through Trino. An empty mart is ready. |
| `GET /api/v1/context` | Actor, authorized customer/site/delivery-point options, and available reporting dates. |
| `GET /api/v1/delivery-performance/summary` | Governed counts, known subtotals, completeness gates, official percentages, financial state, and freshness. |
| `GET /api/v1/delivery-performance/intervals` | Stable, bounded page of current half-hour results and all relevant data/result states. |
| `GET /api/v1/delivery-performance/intervals/{interval_key}/history` | Up to 200 authorized source-knowledge windows using revision-audit descriptions. `truncated=true` explicitly reports that older revisions were omitted. Optional `as_of` is an offset-aware timestamp. |

Summary and interval-list parameters are:

- required `start_date` and `end_date` in `YYYY-MM-DD` form;
- optional `customer_id`, `site_id`, and `delivery_point_id`;
- optional investigation status: `final`, `provisional`, `missing`, `corrected`,
  `shortfall`, or `excess`; and
- `page` and `limit` on the interval list.

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

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `PRODUCT_DEMO_MODE` | `false` | Enables the explicit local demo-identity adapter. |
| `TRINO_HOST` | `trino` | Trino host. Use `127.0.0.1` outside Compose. |
| `TRINO_PORT` | `8080` | Trino port. |
| `TRINO_HTTP_SCHEME` | `http` | `http` or `https`. |
| `PRODUCT_TRINO_USER` | `historical-delivery-api` | Read-only Trino product identity. |
| `PRODUCT_TRINO_TIMEOUT_SECONDS` | `20` | Per-exchange Trino HTTP timeout. |
| `PRODUCT_TRINO_QUERY_TIMEOUT_SECONDS` | `60` | Server-enforced total queued/execution limit for each Trino query. |
| `PRODUCT_TRINO_CATALOG` | `r2` | Validated lower-case Trino catalog identifier. |
| `PRODUCT_TRINO_SCHEMA` | `industrial_energy_marts` | Validated lower-case governed mart schema. |
| `PRODUCT_MAX_QUERY_DAYS` | `31` | Maximum inclusive reporting-date range. |
| `PRODUCT_MAX_PAGE_SIZE` | `200` | Configurable page cap, never above 200. |

Catalog and schema are configurable for isolated tests, but relation names are
fixed in code. Identifier validation prevents configuration from introducing
arbitrary SQL.

## Verify

```bash
uv run pytest -q tests/api
docker build -f apps/api/Dockerfile -t historical-delivery-api .
```

The focused suite verifies identity failures, cross-tenant denial, tenant SQL
predicates, parameter binding, revision-audit history authorization, reporting
date bounds, pagination bounds, empty-mart readiness, bounded/cancelled queries,
history truncation, privacy-safe failure logging, exact aggregate gates, exact
decimal strings, and the difference between a missing value and a real zero.
