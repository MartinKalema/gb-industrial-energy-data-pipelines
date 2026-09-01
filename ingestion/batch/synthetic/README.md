# Deterministic synthetic source generator

This module creates the nine fictional private-source datasets accepted in the
[Phase 2 physical source contracts](../../../docs/modeling/09-phase-2-physical-source-contracts.md).
It lets the batch pipeline exercise realistic customer, contract, schedule,
meter, and capacity history without pretending that public industrial data
exists.

Every output row and the manifest are labelled `synthetic_data: true`. The
generator uses only the Python standard library and never reads credentials or
calls a network service.

## Quick start

Run the built-in check:

```bash
python3 ingestion/batch/synthetic/generate.py self-check
```

Generate an inclusive range of `Europe/London` operating dates into an
explicit caller-owned directory:

```bash
python3 ingestion/batch/synthetic/generate.py generate \
  --start-date 2026-10-24 \
  --end-date 2026-10-26 \
  --seed 20260828 \
  --generation-time-utc 2026-12-31T12:00:00Z \
  --output-dir /tmp/industrial-energy-synthetic
```

There is deliberately no default output directory, so bulk generated records
cannot accidentally land in the repository. Existing known output files are
left untouched unless `--overwrite` is supplied. Even with `--overwrite`, the
generator replaces only its nine JSONL files and `manifest.json`; unrelated
files are not removed.

## Time and determinism

The start and end arguments are **inclusive local dates** in
`Europe/London`. They are expanded into real half-hour intervals and then
written with unambiguous UTC timestamps. Consequently:

- an ordinary local day contains 48 intervals;
- the spring clock-change day contains 46 intervals; and
- the autumn clock-change day contains 50 intervals.

The same dates, seed, explicit generation timestamp, and generator version
produce byte-identical files.
Records use stable sorting and canonical compact JSON formatting. Decimal
quantities, rates, and meter values are strings with exactly six fractional
digits; coordinates are bounded JSON numbers. The manifest's
`generated_at_utc` is the required `--generation-time-utc` value. The wall
clock is never consulted, so an identical replay cannot drift by run time. A
generation timestamp earlier than any generated publication is rejected.
Seeds must be non-negative so they can be recorded by the raw-evidence
envelope contract.

The complete fictional source timeline starts on the `2026-08-26`
`Europe/London` operating date. The customer, site, assignment, meter, and
contract histories use fixed effective dates; cumulative meter registers also
continue from one requested date to the next. A complete bundle before that
date is rejected rather than fabricating a register history that disagrees
with the evidence already loaded into Iceberg.

## What running the generator means

The generator is a stateless simulator of upstream source systems. It does not
look in R2 or Iceberg to decide what is new. Its four inputs determine the
evidence it returns:

| Inputs compared with an earlier invocation | Result |
|---|---|
| Same dates, seed, generation time, and version | Byte-identical replay |
| Same dates and seed, later generation time | Same nine source JSONL files; a different evidence-run timestamp in the manifest |
| Later operating dates with the same seed | New interval evidence; unchanged master revisions and the shared meter boundary replay exactly |
| Same operating dates with a different seed | Different meter values under the same source identities; this is a conflict, not a valid append |

Use `20260828` as the fixed project seed for this synthetic timeline. The seed
is a reproducibility control, not a value that should change on every run. The
Airflow pipeline enforces it. Other CLI seeds are only for isolated generator
experiments whose rows will not be loaded into the project catalog.

Range composition is guaranteed: generating 26 August and 27 August
separately produces the same source-revision identities and payloads as
generating 26–27 August together. This is what makes the implemented daily schedule or
overlapping manual backfill safe. The original 26 August reference bundle
remains byte-compatible with its known-good source hashes.

This file only creates fictional source-shaped records and a manifest. It does
not upload to R2, validate or quarantine rows, load Iceberg, or build dbt
models; those are later tasks in the Airflow workflow.

## Outputs

| Source contract | File | `source_schema_id` |
|---|---|---|
| Customer master | `customer_master.jsonl` | `customer_master` |
| Industrial-site master | `industrial_site_master.jsonl` | `industrial_site_master` |
| Delivery-point assignment | `delivery_point_assignment.jsonl` | `delivery_point_assignment` |
| Revenue-meter assignment | `revenue_meter_assignment.jsonl` | `revenue_meter_assignment` |
| Contract terms | `contract_terms.jsonl` | `contract_terms` |
| Commitment schedule | `commitment_schedule.jsonl` | `commitment_schedule` |
| Approved excess order | `approved_excess_order.jsonl` | `approved_excess_order` |
| Revenue-meter reading | `revenue_meter_reading.jsonl` | `revenue_meter_reading` |
| Delivery-point capacity assessment | `delivery_point_capacity_assessment.jsonl` | `delivery_point_capacity_assessment` |

`manifest.json` records the parameters, UTC coverage, interval count for every
local date, dataset row counts and SHA-256 hashes, entity identifiers, and the
exact keys of the deliberately generated business scenarios. It is written
last, after the JSONL files.

## Included scenarios

Every valid operating date contains at least 46 intervals, so the generator
places the following interval cases at fixed local period numbers on each
date. Stable event-time placement keeps overlapping requests consistent:

- normal delivery and a `0.300000 MWh_th` shortfall;
- approved extra delivery with both billable and unbilled excess;
- explicit `no_commitment` due to approved maintenance versus a genuinely
  missing commitment record;
- a meter-boundary correction that changes both adjacent interval deltas;
- an approved retroactive commitment correction;
- customer legal-name history plus a spelling correction;
- site-name history plus a locality correction;
- a commercial-terms amendment plus a corrected older terms revision;
- provisional, final, and corrected capacity revisions;
- provisional-only, authoritative final-zero, and missing capacity cases; and
- an excess order with explicit interval lines plus a pre-cutoff cancellation.

The scenario catalog in the manifest identifies the exact interval or
effective boundary for each case. Source revisions remain separate rows; the
generator does not prematurely select a current view.

## Python entry points

`build_bundle(start_date, end_date, seed, generation_time_utc)` returns the nine
ordered in-memory record lists plus the manifest context. `write_bundle(...)`
writes them to a required `pathlib.Path` and returns the completed manifest.
`run_self_check()` verifies 46/48/50-day expansion, exact decimal encoding,
scenario presence, the nine-source boundary, and byte-for-byte deterministic
replay.

Keep generated output, manifests, and test extracts out of version control.
Only this generator and its documentation belong in the repository.
