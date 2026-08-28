# Phase 2 source implementation

## Outcome

The accepted Phase 2 source definitions are now executable. The repository can
produce nine deterministic fictional private-source files, prove every row
against a closed JSON Schema contract, and reproduce the same bytes from the
same inputs. The public Elexon FUELHH sidecar and the common raw-evidence
envelope also have machine-readable contracts.

This is still a source-layer milestone. It does not yet create R2 objects,
Iceberg tables, current-version views, or the dimensional mart.

## What this data lets the business answer

The business problem is: for each delivery point and half-hour, did the
operator deliver the steam it promised, was it capable of doing so, and what
was the earned-revenue effect?

| Source evidence | Part of the business problem it answers |
|---|---|
| Customer, site, and delivery-point assignment | Whose delivery was it, and at which industrial site? |
| Revenue-meter assignment and cumulative readings | Which meter is authoritative, and how much thermal energy was delivered between two boundaries? |
| Contract terms | Which energy rate and shortfall-penalty rate applied at event time? |
| Commitment schedule | How much thermal energy was promised for the interval, or was there an explicit approved no-commitment state? |
| Approved excess order | How much delivery above the base promise was authorized for billing without changing SLA? |
| Capacity assessment | How much could the delivery point have supplied, independently of what it actually delivered? |
| Elexon FUELHH | Can the platform ingest and replay a real public batch API correctly? It does not answer the steam outcome. |

Together, the first nine sources provide the evidence needed for the accepted
delivery, shortfall, SLA, contractual-availability, gross-revenue, penalty, and
net-revenue calculations. They deliberately keep three different states
separate: an explicit zero, missing evidence, and not-applicable.

## Implemented flow

```text
Accepted PSC-001..PSC-011 decisions
                |
                v
       Draft 2020-12 contracts
                ^
                | validates every row
                |
deterministic generator -> nine JSONL files + hashed manifest
                |
                v
       caller-selected temporary directory

Next: raw object + evidence envelope -> R2 -> validated Iceberg source tables
```

The generator creates source-shaped revisions. It does not resolve the latest
version, calculate interval facts, or hide corrections. That separation is
intentional: the generator supplies evidence, the contracts define allowed
records, and later validation/transformation jobs apply event-time and
cross-record rules.

## Files and interfaces

- [`contracts/`](../../contracts/README.md) contains 12 JSON Schemas: nine
  fictional private sources, Elexon FUELHH, the raw-evidence envelope, and
  reusable scalar definitions.
- [`ingestion/batch/synthetic/generate.py`](../../ingestion/batch/synthetic/generate.py)
  is a standard-library-only generator.
- [`tests/contracts/`](../../tests/contracts/test_source_contracts.py) proves
  schema validity, generated-row conformance, closed fields, conditionals, and
  the separate Elexon/envelope boundaries.
- [`tests/generator/`](../../tests/generator/test_synthetic_generator.py) proves
  deterministic artifacts, manifests and hashes, DST behavior, references,
  and the accepted business scenarios.

Generate one inclusive `Europe/London` operating day:

```bash
python3 ingestion/batch/synthetic/generate.py generate \
  --start-date 2026-08-27 \
  --end-date 2026-08-27 \
  --seed 20260828 \
  --generation-time-utc 2026-08-28T12:00:00Z \
  --output-dir /tmp/industrial-energy-synthetic
```

Run all source-layer verification:

```bash
uv run pytest -q tests/contracts tests/generator
python3 ingestion/batch/synthetic/generate.py self-check
```

## Representation decisions

- Quantities, rates, and cumulative register values are canonical strings with
  six fractional digits. Validated Iceberg ingestion will convert them to
  fixed decimals. This avoids losing exact business values to binary floating
  point in the raw JSON path.
- Source event and effective times are UTC. The generator accepts inclusive
  `Europe/London` dates and expands them into 46, 48, or 50 real half-hours, so
  the autumn repeated hour still has unique UTC keys.
- Generation time is a required input. The generator never reads the wall
  clock, which makes replay byte-for-byte reproducible. It rejects a generation
  timestamp earlier than any source publication in the bundle.
- Each source record contains source-owned timestamps and values. Ingestion
  time, hash, R2 URI, and record locator belong in the separate raw-evidence
  envelope.
- The schemas are closed: an unexpected field is a validation failure rather
  than silently becoming part of the source contract.
- Every fictional private-source row requires `synthetic_data: true`. Elexon
  rows do not carry that label because they are real public observations.
- Bulk JSONL output has no repository default and remains untracked. Existing
  output is protected unless the caller explicitly chooses `--overwrite`.

## Executable scenarios

The generator includes normal intervals and the accepted edge cases:

- a delivery shortfall;
- approved billable extra energy plus unbilled excess;
- explicit approved no-commitment versus a missing commitment;
- a cumulative-meter correction that changes both adjacent interval deltas;
- an approved retroactive commitment change;
- customer, site, and contract business history plus source corrections;
- provisional, final, corrected, final-zero, and missing capacity evidence;
  and
- an intervalized excess order plus a pre-cutoff cancellation.

The shared-meter-boundary fixture reproduces the accepted Phase 1 result. The
original adjacent deliveries are 4.7 and 5.3 MWh; after the middle boundary is
corrected, they are 4.9 and 5.1 MWh. Total delivery stays 10.0 MWh, while SLA
moves from 97% to 99% and net earned revenue from GBP 455 to GBP 485.

## Validation boundary

JSON Schema validates one record at a time. It cannot by itself compare two
timestamps, subtract cumulative readings, select a revision, or prove that a
relationship covers an interval. The next validation job must therefore add:

- exact 30-minute duration and timestamp ordering;
- duplicate-versus-conflict detection and revision precedence;
- event-time customer, site, contract, and meter-assignment coverage;
- non-overlapping effective versions;
- cumulative-meter delta and physical-plausibility checks;
- capacity arithmetic and no shared-capacity duplication; and
- Elexon settlement-date/period calendar consistency.

The current tests check that the generated fixture's identifiers are complete
and mutually consistent, but that is not a substitute for the future generic
validation/quarantine job.

## Trade-offs and revisit points

- JSONL is easy to inspect, hash, stream, and retain as raw evidence, but it is
  not the analytical storage format. Iceberg/Parquet becomes the typed query
  layer after validation.
- Two fictional customer/site/delivery-point chains keep the first slice small
  while still exposing cross-tenant and relationship mistakes. Larger volume
  and shared-asset topology wait until the thin slice is green end to end.
- Capacity uses the accepted transparent
  `max(nameplate - restriction, 0)` method. Asset, outage, state-of-charge, and
  dispatch constraints are deferred to Phase 4.
- A `finalization` capacity revision type distinguishes promoting provisional
  evidence to an approved final assessment from correcting its measured
  quantities. This implementation detail is recorded in the schema and tests.
- FUELHH remains isolated. Joining it later requires a separately accepted
  business process and valid causal/temporal logic; proximity in time alone is
  not enough.

## Next implementation boundary

Build one bounded Airflow batch path that generates a requested date range,
writes immutable raw objects and envelopes to R2, validates each record,
quarantines failures, and appends the accepted rows to Iceberg. A replay with
the same inputs must reconcile to the same object hashes and logical revision
keys before dbt builds the first dimensional mart.
