# Steam-delivery dbt dimensional mart

## What this part of the pipeline does

The source pipeline records what the business systems said. The dbt project
turns that accepted evidence into answers to the business question:

> For each delivery point and each real 30-minute interval, what was promised,
> what was delivered, what could have been delivered, and what revenue or SLA
> consequence followed?

dbt does **not** generate source data. The synthetic generator creates fictional
source records for a requested date range. Airflow validates and loads those
records into Iceberg. dbt then reads the accepted Iceberg rows and calculates
the dimensional mart. Rerunning dbt with unchanged sources replaces the mart
tables with the same logical result; it does not create another set of business
events.

## Data flow

```text
R2 Iceberg source revisions     r2.industrial_energy_validated
             |
             v
revision-preserving views       r2.industrial_energy_staging
             |
             v
current selection, joins,
meter deltas, calculations,
and source-knowledge history    r2.industrial_energy_intermediate
             |
             v
dimensions and facts            r2.industrial_energy_marts

batch coverage control          r2.industrial_energy_control
             |
             +----> creates every expected delivery-point/half-hour row
```

The coverage-control row is important. It records which operating dates a
successful source run covered. dbt expands those dates using `Europe/London`
calendar rules and combines them with the delivery-point assignments. This is
why the fact can retain an expected interval when a commitment, capacity
assessment, or meter boundary is missing. Without this control, absence could
silently remove the fact row and be mistaken for zero.

## Model layers

### Sources and staging

The nine validated business tables and the batch-coverage control table are
declared as dbt sources. Each validated table has a one-to-one staging view with
an explicit column list. Staging deliberately keeps:

- every source revision;
- null values rather than replacing them with zero;
- timezone-aware timestamps and fixed-decimal quantities;
- source status, approval, and correction fields; and
- all eight pipeline lineage fields.

Staging does not choose a current revision, filter a terminal state, or apply a
business calculation.

### Intermediate reconciliation

The intermediate layer:

1. chooses the authoritative approved revision without falling back past a
   cancellation or withdrawal;
2. constructs every expected delivery-point/half-hour interval from successful
   batch coverage;
3. resolves the customer, site, contract, and revenue meter that applied for the
   complete interval;
4. subtracts exact cumulative meter boundaries from the same assigned register;
5. attaches the commitment, eligible approved excess order, and final capacity
   assessment; and
6. applies the shared delivery, SLA, availability, and revenue calculations.

Current and historical facts call the same calculation macro so their formulas
cannot drift apart.

### Dimensional marts

The eight business dimensions are:

- `dim_date`
- `dim_interval`
- `dim_customer`
- `dim_site`
- `dim_delivery_point`
- `dim_contract`
- `dim_meter`
- `dim_data_status`

`fct_steam_delivery_interval` has the accepted grain: one delivery point during
one real UTC half-hour. It is the easiest table for current reporting.

Five physical audit companions preserve the exact source revision that was
authoritative at a historical cutoff:

- `dim_customer_revision_audit`
- `dim_site_revision_audit`
- `dim_delivery_point_revision_audit`
- `dim_contract_revision_audit`
- `dim_meter_revision_audit`

These are revision-aware versions of five business dimensions, not five new
business concepts.

`fct_steam_delivery_interval_history` adds a source-knowledge window to the
fact grain. It answers questions such as, "What result would we have reported
using only revisions published and approved by 09:00?" Its half-open
`[known_from_utc, known_to_utc)` windows are based on the later of source
publication and approval time. Platform ingestion time and Iceberg time travel
answer different audit questions and are not substituted for this business
history.

Use the ordinary dimension keys to group the same business version across
corrections. Use the five revision keys and audit dimensions when a report must
reproduce the exact descriptive values known at a historical cutoff.

## Run the mart locally

From the repository root, start Trino if the batch profile is not already
running:

```bash
docker compose --project-directory . -f infrastructure/compose.yaml \
  --profile query up -d trino
```

Verify the dbt connection:

```bash
uv run dbt debug \
  --project-dir transformations \
  --profiles-dir transformations
```

Build and test all project models:

```bash
uv run dbt build \
  --project-dir transformations \
  --profiles-dir transformations \
  --no-populate-cache
```

`--no-populate-cache` avoids an R2 Data Catalog list-views limitation during
dbt's eager relation-cache population. It does not skip model creation or data
tests.

The accepted view-only intermediate design expands the source-knowledge query
to 288 stages. Local Compose therefore sets Trino's `query.max-stage-count` to
400 instead of the default 150. Trino warns that very high limits can destabilize
shared clusters, so this setting belongs only to the isolated single-node
development engine and must be reconsidered with greater data or concurrency.
The dbt profile also uses one thread so catalog metadata requests remain serial
against the R2 Data Catalog beta.

After successful source reconciliation and coverage publication, the bounded
Airflow DAG divides the dbt work into these six ordered restart points:

| Airflow task | Command boundary | Expected result |
|---|---|---|
| `prepare_and_test_loaded_data_with_dbt` | Build `models/staging` with cautious test selection | 9 models and 235 tests |
| `prepare_and_test_delivery_calculations_with_dbt` | Build `models/intermediate` with cautious test selection | 33 models and 8 tests |
| `build_current_delivery_fact_with_dbt` | Run the current delivery fact | 1 model |
| `build_delivery_history_fact_with_dbt` | Run the source-knowledge history fact | 1 model |
| `build_dimension_tables_with_dbt` | Run the dimension models after both facts | 13 models |
| `test_complete_dimensional_mart_with_dbt` | Test the mart plus the two reconciliation fixtures | 70 tests |

"Cautious test selection" means dbt runs a test in that section only when all
the models the test needs are available there. It prevents an early task from
pulling a later mart model into its work. The dimensions follow both facts
because `dim_data_status` reads their observed status combinations; placing the
dimension section first would fail when the mart schema starts empty.

Every dbt task uses the one-slot `iceberg_writer` pool, one dbt thread, and
`--no-populate-cache`. Its dbt subprocess may run for up to 120 minutes inside
a 125-minute Airflow task limit. The extra five minutes leave time for local
process cleanup, Trino cleanup, and scheduler margin. The complete DAG still
has a 180-minute limit, so an automatic retry is conditional on the whole run
having time left. Each checkpoint and try has separate log and target
artifacts. Timeout cleanup stops only that dbt process group and only the Trino
queries tagged for that task attempt before the writer pool is released.

This split changes recovery in a practical way. If, for example, the history
fact fails, Airflow retries `build_delivery_history_fact_with_dbt`; it does not
rebuild the staging views, calculations, or current fact. The dimension task
waits until the history fact succeeds. Earlier successful checkpoints stay
green and their relations stay available. The standalone all-project command
above remains useful for development and recovery diagnostics, but must not run
concurrently with an Airflow dbt task.

Coverage and mart readiness remain separate facts: a coverage row proves the
source run reconciled, while the dimensional mart is certified only after
`test_complete_dimensional_mart_with_dbt` succeeds. dbt replaces relations
individually rather than committing the whole project in one transaction, so a
failed checkpoint can temporarily leave a mixed set of relations inside that
section. Retrying the failed checkpoint converges it; consumers must not treat
a failed final test task as a certified mart refresh.

## Inspect the result

Current interval facts:

```sql
select
    delivery_point_natural_id,
    interval_start_utc,
    committed_mwh_th,
    delivered_mwh_th,
    shortfall_mwh_th,
    excess_mwh_th,
    approved_extra_mwh_th,
    billable_mwh_th,
    net_earned_revenue_gbp,
    delivery_measurement_status,
    commitment_status,
    capacity_status
from r2.industrial_energy_marts.fct_steam_delivery_interval
order by delivery_point_natural_id, interval_start_utc;
```

State known at a cutoff:

```sql
select history.*
from r2.industrial_energy_marts.fct_steam_delivery_interval_history as history
where history.delivery_point_natural_id = 'DP-001'
  and history.interval_start_utc = timestamp '2026-08-26 03:00:00 UTC'
  and history.known_from_utc <= timestamp '2026-08-27 09:00:00 UTC'
  and (
      history.known_to_utc > timestamp '2026-08-27 09:00:00 UTC'
      or history.known_to_utc is null
  );
```

The second query must return at most one row for that delivery interval and
cutoff. To reproduce historical customer, site, contract, delivery-point, or
meter attributes, join the corresponding `*_revision_key` to its revision-audit
dimension.

## Verified baseline

The live 2026-08-26 operating-date slice was verified on 2026-08-28:

- 96 current facts: two delivery points multiplied by 48 real half-hours;
- 582 source-knowledge history windows;
- one date row, 48 interval rows, and 16 observed data-status combinations;
- all nine staging views built with exact source row/type/column parity;
- 235 staging tests and 78 intermediate/mart tests passed;
- dbt parse and compile passed; and
- all 66 Python contract, generator, and pipeline tests passed.

The live shared-boundary correction preserves total delivered energy at
10.0 MWh_th while changing the two adjacent results:

| State known at cutoff | Shortfall | Excess | Billable | Gross GBP | Penalty GBP | Net GBP |
|---|---:|---:|---:|---:|---:|---:|
| Before correction | 0.3 | 0.3 | 9.7 | 485.00 | 30.00 | 455.00 |
| After correction | 0.1 | 0.1 | 9.9 | 495.00 | 10.00 | 485.00 |

## Correctness rules enforced in code

- A missing value remains missing; it is never silently changed to zero.
- An explicit no-commitment interval is zero and has no SLA or availability
  denominator.
- A corrected cumulative reading recalculates both adjacent intervals that use
  the shared boundary.
- Opening and closing readings must belong to the same effective revenue-meter
  assignment and register.
- A later provisional capacity assessment cannot displace an earlier final one.
- A withdrawal or cancellation is terminal; an older active revision does not
  regain authority.
- Only excess approved and published before the interval starts is billable.
- Thermal-energy quantities remain fixed decimals; GBP is retained at
  `decimal(38,12)` and rounded only for presentation.
- `delivered_steam_t` remains null because the source contract contains thermal
  energy, not enough governed steam-condition evidence to calculate mass.
- Deterministic SHA-256 keys identify stable business grains and exact revision
  audit grains.

## Full rebuild now, incremental later

The mart tables are intentionally rebuilt in full. The current bounded data is
small, a full rebuild is easier to verify, and a correction to one meter
boundary can change two adjacent facts. More source rows alone do not prove
that an incremental design is needed.

Incremental processing should be introduced only after runtime and scan metrics
show a need. Its change set must include directly changed intervals, both
neighbors of a corrected meter boundary, event-time relationship changes, and
all affected source-knowledge windows. Partitioning follows measured query and
rewrite patterns rather than being guessed now.

## Known boundary

The mart reads accepted Iceberg rows. Batch coverage contains aggregate
quarantine counts, but there is not yet a typed event-level quarantine index in
Iceberg. The mart can distinguish accepted, missing, provisional, withdrawn,
and corrected analytical states. It cannot truthfully say whether one missing
event was never received or was received and quarantined. That distinction must
wait for a queryable quality index keyed to the event grain.
