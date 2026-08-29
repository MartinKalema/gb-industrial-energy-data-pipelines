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

- All compute remains local; Trino reads the R2-backed Iceberg marts.
- Product queries are read-only, parameterized, operating-date-bounded, and
  page-bounded.
- Decimal quantities and money retain their exact string representation across
  the API boundary. UTC timestamps remain timezone-aware.
- Missing governed measures remain `null`; neither the API nor UI converts them
  to zero.
- Health checks distinguish a live process from readiness to query Trino.
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
  |-- adds the scope to every query
  |-- returns governed values and result states
  v
Trino (read-only product user/source tag)
  v
R2 Data Catalog -> Iceberg dimensional marts on Cloudflare R2
```

The web application is a presentation layer, not a security boundary. Changing
or hiding a filter cannot expand access because FastAPI independently resolves
the actor and constrains every Trino query.

## Data contract

The API reads only these governed relations:

- `fct_steam_delivery_interval` for the current authoritative half-hour result;
- `fct_steam_delivery_interval_history` for source-knowledge change windows;
- current dimensions for display names and filter options; and
- revision-audit dimensions when historical descriptions are needed.

The API does not read raw JSON, quarantine files, validated source tables, or
reimplement meter deltas. It uses the fact's interval-level capped numerators,
counts, monetary values, lineage, and result-status fields.

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
| `GET /health/ready` | Confirm that the configured Trino mart can be queried. |
| `GET /api/v1/context` | Return the actor, authorized customers/sites, and available date boundary. |
| `GET /api/v1/delivery-performance/summary` | Return governed totals, completeness, official percentages, result states, and freshness for a bounded scope. |
| `GET /api/v1/delivery-performance/intervals` | Return a stable page of half-hour facts and their individual states. |
| `GET /api/v1/delivery-performance/intervals/{delivery_interval_key}/history` | Return authoritative source-knowledge windows for one authorized interval. |

Inclusive `start_date` and `end_date` inputs are GB operating dates, not
UTC-midnight approximations. The API filters the fact's warehouse date key so a
summer operating day correctly includes its first interval at 23:00 UTC on the
previous civil date. Customer/site identifiers, status filters, page, and limit
are also typed inputs. The maximum query window and page size are configuration,
with conservative local defaults. Invalid bounds return a client error;
unavailable Trino returns a bounded service error rather than an empty business
result.

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

- Compose starts the product profile only after Trino is healthy and starts the
  web service only after FastAPI is ready.
- A mart is declared ready only after Airflow's
  `test_complete_dimensional_mart_with_dbt` checkpoint succeeds. The current
  full-rebuild baseline does not provide a transaction across all mart tables,
  so the local product profile must not serve a dbt build that is still running
  or has failed partway through; rerun the failed checkpoint to convergence
  before serving the mart.
- Query and connection deadlines prevent a stalled Trino request from holding a
  product request indefinitely.
- Stable pagination prevents an unbounded interval response.
- Structured logs and request IDs make a UI failure traceable through FastAPI
  to a Trino query without exposing source payloads.
- A readiness failure is distinct from an authorized query that correctly
  returns no rows.

## Trade-offs

| Decision | Benefit | Cost or limitation |
|---|---|---|
| Query Trino directly from FastAPI | One governed read path over current Iceberg results; no duplicate serving store | Product latency depends on local Trino and the remote catalog |
| Server-render the first web workflow | Keeps data access on the server and makes empty/error states deterministic | Rich live interaction will need client components later |
| Keep aggregate presentation in the typed API | Dashboard, later AI tools, and exports can share one contract | The API query contract must be regression-tested with dbt fixtures |
| Explicit local demo identities | Makes authorization behavior visible and testable without fake production claims | It is not authentication and must never be enabled in a real deployment |
| No cache in the first release | Every response reflects the current committed Iceberg snapshot | Repeated queries cost more and may need measured caching later |
| Serve only a successfully tested mart build | Prevents presenting a known partial rebuild as ready | The first local release uses an operating rule rather than an atomic multi-table publication pointer |

## Revisit as usage grows

- Add verified OIDC identities and policy-managed scopes before deployment.
- Add a short-lived cache only after measuring repeated Trino query latency and
  defining correction invalidation behavior.
- Add a separate serving store only if measured product latency cannot meet an
  accepted objective; Iceberg remains the governed system of record.
- Move widely reused aggregate queries into dedicated dbt presentation models
  if another consumer needs direct SQL rather than the typed product API.
- Add an atomic serving-version pointer or snapshot-set manifest before dbt
  builds and product traffic are allowed to overlap.
- Add live operator views only after the Spark streaming slice has event-time,
  late-data, and reconciliation guarantees.
