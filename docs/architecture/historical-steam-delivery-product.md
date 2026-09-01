# Historical steam-delivery performance product

## Outcome

This product turns the governed steam-delivery mart into a decision tool. A
commercial manager can identify commitment misses, quantify their known
financial effect, distinguish final results from provisional evidence, and
inspect how a later source correction changed an interval. A customer persona
can inspect the same governed measures only for its own customer and sites.

The first release is focused on historical delivery performance. It is not a
throwaway dashboard: the API owns authorization and metric presentation, the
web application consumes typed responses, and both services have health,
failure, and automated-test contracts.

The product reads a versioned native ClickHouse copy for interactive speed.
That copy is disposable and rebuildable. Cloudflare R2 and Apache Iceberg
remain canonical, while Trino and dbt remain responsible for governed
transformations and tests.

## Requirements and boundaries

### Functional requirements

- Filter a bounded range of `Europe/London` operating dates by authorized
  customer and site while retaining each interval's exact UTC boundaries.
- Present committed, delivered, shortfall, excess, capacity, SLA, availability,
  penalty, and revenue measures without recalculating dbt business rules.
- Make final, provisional, not-applicable, missing, and corrected states visible.
- Page through the underlying half-hour delivery intervals.
- Inspect the source-knowledge history of one interval.
- Deny a customer persona access to another customer's rows even when a caller
  supplies the other customer or site identifier directly.

### Non-functional requirements

- All compute remains local. Trino reads and transforms the R2-backed Iceberg
  marts during the finite batch workflow; ClickHouse serves product requests.
- Product queries are read-only, parameterized, operating-date-bounded, and
  page-bounded.
- Decimal quantities and money retain their exact string representation across
  the API boundary. UTC timestamps remain timezone-aware.
- Missing governed measures remain `null`; neither the API nor UI converts them
  to zero.
- Health checks distinguish a live process from readiness to query a complete,
  recent ClickHouse publication. Readiness checks identity mode, repository
  access, ready-marker row-count integrity, and configured publication age.
- Request logs carry a request ID, actor, authorized scope, route, outcome, and
  duration without logging contract values or credentials.

## High-level design

```text
Browser
  |
  | HTTP on loopback
  v
Next.js historical-performance product
  |
  | typed, server-side HTTP calls with the selected local demo persona
  v
FastAPI product boundary
  |-- validates date/page bounds
  |-- resolves actor and allowed customer scope
  |-- resolves/pins one ready data version
  |-- adds the version and tenant scope to every query
  |-- returns governed values and result states
  v
ClickHouse native MergeTree serving tables (read-only API account)

Separate finite publication path:

Iceberg marts on R2 -> Trino/dbt tests -> Airflow publisher -> ClickHouse
```

The web application is a presentation layer, not a security boundary. Changing
or hiding a filter cannot expand access because FastAPI independently resolves
the actor and constrains every ClickHouse query.

## Data contract

The canonical inputs to the publication task are these governed relations:

- `fct_steam_delivery_interval` for the current authoritative half-hour result;
- `fct_steam_delivery_interval_history` for source-knowledge change windows;
- current dimensions for display names and filter options; and
- revision-audit dimensions when historical descriptions are needed.

The task denormalizes the API-facing fields into
`industrial_energy_serving.delivery_interval_current` and
`industrial_energy_serving.delivery_interval_history`. It does not recalculate
meter deltas, delivery, SLA, availability, or money. It preserves the fact's
interval-level capped numerators, counts, monetary values, lineage, exact nulls,
timestamps, and result-status fields.

The API reads only those two serving tables and
`industrial_energy_serving.data_publication`. It does not read raw JSON,
quarantine files, validated source tables, or unfinished ClickHouse candidates.
See the
[ClickHouse frontend serving architecture](clickhouse-frontend-serving-layer.md)
for the table and release contract.

### Aggregate safeguards

- Known quantities and monetary subtotals may be returned with an explicit
  provisional status.
- Official SLA is returned only when every expected commitment is known and
  every applicable delivery is accepted. Its numerator is the sum of the
  fact's already capped `sla_attainment_numerator_mwh_th` values.
- Official contractual availability additionally requires final capacity for
  every applicable interval and uses the fact's already capped availability
  numerator.
- A scope with no applicable commitment returns `not_applicable`, not 100%.
- Percentages are calculated from summed numerators and denominators; row
  percentages are never averaged.
- A missing value stays unknown. SQL `coalesce` may be used for a record count
  only where zero records is the defined count; it is not used to manufacture a
  business measure.

## HTTP boundary

| Endpoint | Purpose |
|---|---|
| `GET /health/live` | Confirm that the API process is running. |
| `GET /health/ready` | Confirm identity mode, repository access, ready-marker row-count integrity, and configured publication age. |
| `GET /health/metrics` | Return bounded process-local uptime, request-count, error-count, and duration evidence. |
| `GET /api/v1/context` | Return the actor, authorized customers/sites, available date boundary, data version, and publication time. |
| `GET /api/v1/delivery-performance/summary` | Return governed totals, completeness, official percentages, result states, and freshness for a bounded scope. |
| `GET /api/v1/delivery-performance/intervals` | Return a stable page of half-hour facts and their individual states. |
| `GET /api/v1/delivery-performance/intervals/{interval_key}/history` | Return authoritative source-knowledge windows for one authorized interval. |

Inclusive `start_date` and `end_date` inputs are GB operating dates, not
UTC-midnight approximations. The API filters the fact's warehouse date key so a
summer operating day correctly includes its first interval at 23:00 UTC on the
previous civil date. Customer/site identifiers, status filters, page, and limit
are also typed inputs. The maximum query window and page size are configuration,
with conservative local defaults. Invalid bounds return a client error;
unavailable ClickHouse or a missing ready publication returns a bounded service
error rather than an empty business result.

The first context request resolves the newest ready publication and returns
`data_version` plus `data_published_at_utc`. The web carries that version through
pagination and detail links, and sends it in the `X-Product-Data-Version` header
on context, summary, interval, and history requests. The API then uses that
exact immutable version, so one investigation cannot mix two publications.
Callers that omit the header use the newest ready publication.

## Local identity and authorization

The local product profile uses an explicit demo-identity adapter so the project
can exercise commercial and customer scopes without pretending that a local
persona selector is production authentication. Demo mode is opt-in through
configuration and exposes only the fictional project identities.

- The commercial-manager persona may query both fictional customers.
- Each customer persona is restricted to its own customer and sites.
- Missing, unknown, and cross-scope identities fail closed.
- Customer personas may receive their own applicable contract rates and
  projected service-charge/SLA-credit outcomes under DM-010. Other tenants,
  procurement cost, margin, internal pricing logic, and operational details
  remain excluded; commercial views may use the internal earned-revenue and
  accrued-penalty labels for the same governed amounts.

A production deployment must replace the demo adapter with verified identity
tokens and centrally governed customer/site claims. The row-scope enforcement
and negative tests remain at the API boundary.

## Reliability and observability

- Compose starts the API only after ClickHouse is healthy. The API readiness
  endpoint stays false until identity mode, repository access, row-count
  integrity, and publication age meet the configured contract.
- Airflow runs `publish_tested_dimensional_mart_to_clickhouse` only after
  `test_complete_dimensional_mart_with_dbt` succeeds.
- Candidate current and history rows carry a new `load_attempt_id`. They remain
  invisible until exact validation passes and the matching `data_publication`
  ready marker is inserted as the final write.
- A partial load, validation failure, or timeout cannot replace the last good
  publication. An exact retry reuses the already-ready fingerprint.
- Query and connection deadlines prevent a stalled ClickHouse request from
  holding a product request indefinitely.
- Stable pagination prevents an unbounded interval response.
- Structured logs and request IDs make a UI failure traceable through FastAPI
  to a ClickHouse query without exposing source payloads.
- A readiness failure is distinct from an authorized query that correctly
  returns no rows.

## Local verification

The real serving publication contained 96 current rows and 558 authorized
history rows. Repeating the same tested source fingerprint reused the ready
version. Failure tests proved that partial and invalid candidates never become
visible.

Before the serving layer, representative local Trino/R2 calls measured about
26 seconds for context and 6 seconds for summary; the complete server-rendered
page measured about 14 seconds. With ClickHouse, API calls measured about 0.02
seconds and the same page about 0.17 seconds. These measurements describe this
local project environment and are not general engine benchmarks.

## Trade-offs

| Decision | Benefit | Cost or limitation |
|---|---|---|
| Publish native ClickHouse serving tables | Local API calls are fast and do not wait for R2 metadata, Trino planning, or object downloads | The tested mart is duplicated into a rebuildable serving copy |
| Use immutable candidates and a final ready marker | A failed publication is invisible and the previous version remains usable | Cleanup must share the writer pool and remove markers before rows |
| Pin one page to `X-Product-Data-Version` | Concurrent publication cannot mix versions within one page | The API must retain a requested ready version while clients may still use it |
| Server-render the first web workflow | Keeps data access on the server and makes empty/error states deterministic | Rich live interaction will need client components later |
| Keep aggregate presentation in the typed API | Dashboard, later AI tools, and exports can share one contract | The API query contract must be regression-tested with dbt fixtures |
| Explicit local demo identities | Makes authorization behavior visible and testable without fake production claims | It is not authentication and must never be enabled in a real deployment |
| No cache in the first release | The measured serving database path is visible without hiding it behind another layer | Repeated queries still reach ClickHouse |
| Full versioned publication first | Easy to compare, retry, and rebuild | Data growth may later justify incremental publication |

## Revisit as usage grows

- Add verified OIDC identities and policy-managed scopes before deployment.
- Add a short-lived cache only after measuring repeated ClickHouse query latency and
  defining correction invalidation behavior.
- Add incremental publication only when measured data growth justifies its
  additional state and recovery logic. Revisit whether the current two-ready-
  version minimum needs a longer time-based client promise.
- Move widely reused aggregate queries into dedicated dbt presentation models
  if another consumer needs direct SQL rather than the typed product API.
- Add live operator views only after the Spark streaming slice has event-time,
  late-data, and reconciliation guarantees.
