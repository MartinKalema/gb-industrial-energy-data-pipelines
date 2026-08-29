# Project brief

## Working title

**Great Britain Industrial Thermal Battery Operations and Steam Delivery Intelligence Platform**

The company and plants in this project are fictional. Public grid data remains attributed to its real publishers.

## Accepted business problem

Accepted on 2026-08-27.

Industrial customers require reliable steam even while electricity prices and grid conditions change. Operators need to know whether each thermal-battery site met its contracted steam commitment, why a shortfall occurred, and how charging decisions affected cost, carbon, availability, penalties, and revenue.

Today those answers would plausibly be fragmented across telemetry, meter, maintenance, market, contract, and billing systems. Different products could consequently define “delivered steam,” “availability,” or “revenue” differently.

## Product outcome

Create one governed data and product platform where:

- an operator sees live delivery risk and investigates plant conditions;
- a commercial manager quantifies SLA and revenue exposure;
- a customer sees only their sites and delivery history;
- an analyst safely adds approved metrics or alert rules;
- a read-only AI assistant explains outcomes using governed tools and citations.

## Primary product decision

The first release should help a site operator decide:

> Do I need to intervene now to protect the current steam-delivery commitment?

The historical product should then explain:

> What caused a commitment miss, and what did it cost?

## Candidate users

| Persona | Decision or workflow | Product evidence |
|---|---|---|
| Control-room operator | Detect and investigate delivery risk | Near-real-time site view and alert timeline |
| Reliability engineer | Understand outages and degraded assets | Availability and maintenance analysis |
| Commercial manager | Quantify shortfalls, penalties, and revenue at risk | Commercial performance mart |
| Customer energy manager | Verify contracted delivery and SLA | Tenant-isolated customer view |
| Analyst/domain expert | Add a metric or alert without an engineering ticket | Validated self-service definition workflow |
| Platform owner | Enforce freshness, access, lineage, and cost controls | Admin and observability view |

## Functional requirements

1. Backfill historical grid/market data through a repeatable batch pipeline.
2. Receive near-real-time external grid events and live simulated plant telemetry.
3. Preserve immutable raw payloads and source publication metadata.
4. Normalize, deduplicate, watermark, and reconcile late or revised events.
5. Build tested dimensional marts and shared metric definitions.
6. Expose role-appropriate product views through a typed API.
7. Allow bounded self-service changes with validation and approval tiers.
8. Provide a read-only, grounded AI investigation flow with authorization at every tool call.
9. Record pipeline health, data freshness, lineage, app errors, and AI evaluation results.

## Non-functional requirements

- Reproducible on one developer laptop through containerized services.
- Idempotent backfills and recoverable stream processing.
- Event-time correctness for late and out-of-order data.
- No credentials in source control or images.
- Customer/site authorization enforced server-side, not only in the UI.
- Shared metric definitions across dashboards, API responses, and AI tools.
- Small enough to demo end to end before adding optional technologies.

## Constraints and assumptions

- Compute runs locally; R2 and source APIs are remote services.
- Industrial plant, contract, maintenance, and customer data must be simulated.
- Public feeds have their own availability, licensing, and revision behavior.
- The first deployment is educational, not a real plant-control or billing system.
- Dimensional grains and metric definitions remain open until the modeling workshop.

## Explicit non-goals for the first complete slice

- Sending control commands to physical equipment
- Autonomous dispatch or financial commitments
- Predictive maintenance models
- A full digital twin
- Kubernetes
- Enterprise identity federation
- A second table format
- Adding a serving store before Iceberg/Trino product latency is measured

That condition was later satisfied: direct Trino/R2 product latency was
measured, and the implemented ClickHouse serving copy keeps Iceberg canonical.

## Completion evidence

A strong portfolio demonstration will show one injected delivery shortfall traveling through both live and historical paths, arriving at the same governed metric, appearing only to authorized users, and producing a grounded AI explanation that passes regression and cross-customer leakage tests.
