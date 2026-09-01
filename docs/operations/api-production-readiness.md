# Historical delivery API operational checks

## Outcome

The local product now exposes enough technical evidence for a person, Docker,
or a monitoring system to decide whether the API should receive traffic. These
checks do not make the demo a production deployment. In particular, production
identity, alert delivery, service-level targets, and representative capacity
testing still need decisions and external services.

No health response contains credentials, customer identifiers, contractual
values, energy measurements, or exception text.

## Health endpoints

| Endpoint | Meaning | Dependency work |
|---|---|---|
| `GET /health/live` | The Python process can answer HTTP. | None. It stays small and does not query ClickHouse or identity. |
| `GET /health/ready` | The process is safe to receive product traffic. | Checks identity mode, ClickHouse access, publication row counts, and publication age. |
| `GET /health/metrics` | Process-local technical counters since the latest restart. | Reports uptime, in-flight/completed requests, 4xx/5xx counts, and average/maximum request duration. |

Readiness returns HTTP `200` with `status: ready`, or HTTP `503` with
`status: not_ready`. Its named checks explain which boundary failed:

- `identity_provider` is a warning in the local demo and a failure when demo
  mode is off because no production provider is implemented;
- `serving_repository` proves the bounded ClickHouse checks completed;
- `serving_row_counts` compares the ready marker's expected current/history
  counts with the rows stored under that publication ID; and
- `publication_freshness` compares the latest publication time with the
  configured maximum age.

The metrics are intentionally process-local. They reset when the API restarts
and have no customer, tenant, route, or publication labels. A production
metrics system must aggregate across every API process and retain history.

## Publication-age backstop

The Compose product profile sets:

```text
PRODUCT_MAX_PUBLICATION_AGE_SECONDS=108000
```

That is 30 hours. The local assumption is one tested publication each day,
normally after 12:00, with the scheduled daily wrapper responsible for meeting
the exact next-day 16:00 workflow deadline. If the previous release was written
between 12:00 and 16:00, the 30-hour backstop becomes unhealthy between 18:00
and 22:00 the following day—two to six hours after the next 16:00 workflow
deadline. It is a backstop, not the deadline alert.

Publication age is only a backstop. It proves that ClickHouse received a recent
ready marker; it does not prove that the expected operating date was loaded.
The daily Airflow workflow must own that exact date check and alert on a missed
data interval. Re-publishing old source coverage must not be used to hide a
missed daily load.

Setting the value to `0` disables only the age check. That is useful for an
isolated test or an intentionally static historical demonstration, but it is
not an acceptable production default. A production owner must set this value
from the accepted publication and data-freshness service level.

## Alert-ready command

With the product running, execute:

```bash
uv run python -m apps.api.operational_check \
  --url http://127.0.0.1:8000/health/ready \
  --requests 1 \
  --concurrency 1 \
  --timeout-seconds 5 \
  --max-p95-ms 2000 \
  --max-error-rate 0
```

The command prints bounded JSON. It exits `0` only when the endpoint returns
the expected ready JSON and the supplied error-rate and latency limits pass. It
exits `1` when the check fails and `2` for invalid command arguments. Docker's
API health check uses the same command.

An external monitor can run this command and alert on a non-zero exit. Choosing
the alerting service, notification route, escalation schedule, and retry window
requires the real operating environment.

## Record local capacity evidence

The same command can run a deliberately small concurrent check. Create an
ignored evidence directory and test the complete readiness query:

```bash
mkdir -p tmp/operational-evidence

uv run python -m apps.api.operational_check \
  --url http://127.0.0.1:8000/health/ready \
  --requests 100 \
  --concurrency 5 \
  --timeout-seconds 5 \
  --max-p95-ms 2000 \
  --max-error-rate 0 \
  --output tmp/operational-evidence/api-readiness.json
```

For a local representative product query, use the demo identity only:

```bash
uv run python -m apps.api.operational_check \
  --url 'http://127.0.0.1:8000/api/v1/delivery-performance/summary?start_date=2026-08-26&end_date=2026-08-26' \
  --demo-actor commercial-manager \
  --expected-json-status any \
  --requests 100 \
  --concurrency 5 \
  --timeout-seconds 5 \
  --max-p95-ms 2000 \
  --max-error-rate 0 \
  --output tmp/operational-evidence/api-summary.json
```

The report records request counts, concurrency, throughput, errors, HTTP status
counts, and minimum/p50/p95/p99/maximum latency. It omits query strings and
headers. The tool caps one run at 1,000 requests and 32 workers to reduce
accidental local overload.

These commands create repeatable evidence, not a production capacity claim.
On 2026-09-02, the local single-process API and single-node ClickHouse copy were
checked with the current 96-row current / 558-row history serving data. One
hundred commercial-manager summary requests at concurrency 5 completed with
zero errors, p50 15.854 ms, p95 56.097 ms, p99 117.703 ms, and measured
throughput 241.579 requests/second. The same verification proved that the
default 30-hour age limit returns HTTP 503 for the old local publication, while
the explicitly disabled age check returns HTTP 200 for a static demo.

This is only a small local baseline. Before production, run a separately
approved load test with representative data size, query mix, concurrency,
network, replicas, and failure conditions. Agree the latency and error-rate
targets before treating the report as a pass/fail gate.

## Failure response

### Publication is too old

1. Check the expected daily Airflow run and its operating date.
2. Check `test_complete_dimensional_mart_with_dbt`.
3. Check `publish_tested_dimensional_mart_to_clickhouse`.
4. Repair and retry only the failed checkpoint where the runbook allows it.
5. Confirm `/health/ready` reports the new version and an age below the limit.

Do not change the threshold merely to turn the check green. During an approved
static demonstration, disabling age is a visible configuration decision, not a
data repair.

### Serving row counts do not match

Keep the failed version out of traffic. Do not edit its rows or ready marker by
hand. Re-run the tested ClickHouse publication so it builds and validates a new
version. The previous complete publication remains the recovery source only if
it is still within the accepted freshness window.

### Repository check fails

Check ClickHouse process health, the read-only API account, network reachability,
and the serving schema. Keep credentials out of command output and tickets.

### Identity check fails

Do not enable the demo header adapter in production. The deployment remains
not ready until a verified production identity adapter exists and its token,
issuer, audience, tenant-claim, key-rotation, and failure rules are tested.

## Decisions that need the real environment

- Select and integrate the production identity provider, then test key rotation,
  revoked tokens, wrong issuers/audiences, missing tenant claims, and provider
  failure.
- Accept the exact daily data deadline and publication-age threshold.
- Select the metrics/alerting system, scrape access, retention, paging route,
  ownership, and escalation timetable.
- Define production latency, availability, and error-rate targets.
- Measure capacity with representative data and concurrency, then choose API and
  ClickHouse replica counts, CPU/memory limits, connection limits, and autoscaling
  or admission-control rules.
