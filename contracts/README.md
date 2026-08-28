# Phase 2 source contracts

This directory contains the machine-readable promises for every Phase 2 input
record. The schemas implement the decisions accepted in
[`09-phase-2-physical-source-contracts.md`](../docs/modeling/09-phase-2-physical-source-contracts.md).
They validate isolated records before relational and business-rule validation.

## Contract index

| Schema | One record means |
|---|---|
| `customer_master.schema.json` | One published revision of one effective customer version |
| `industrial_site_master.schema.json` | One published revision of one effective industrial-site version |
| `delivery_point_assignment.schema.json` | One published revision of one effective delivery-point relationship |
| `revenue_meter_assignment.schema.json` | One published revision of one effective meter/register assignment |
| `contract_terms.schema.json` | One published revision of one effective commercial-terms episode |
| `commitment_schedule.schema.json` | One published revision of one delivery-point commitment interval |
| `approved_excess_order.schema.json` | One published revision of one excess-order interval allocation |
| `revenue_meter_reading.schema.json` | One published revision of one cumulative boundary reading |
| `delivery_point_capacity_assessment.schema.json` | One published revision of one interval capacity assessment |
| `elexon_fuelhh.schema.json` | One normalized Elexon FUELHH fuel/interval publication |
| `raw_evidence_envelope.schema.json` | Lineage for the immutable raw object containing a source record |
| `common.schema.json` | Shared scalar definitions referenced by the other schemas |

Every schema uses JSON Schema Draft 2020-12, snake_case field names, and a
closed top-level object. An unexpected field therefore fails validation instead
of being silently ignored. Each normalized record identifies its own schema as
version `1.0.0`; incompatible changes require a new schema version.

## Representation rules

- UTC timestamps are ISO 8601 strings ending in `Z`. Event and effective
  boundaries that govern delivery are exact UTC half-hour boundaries.
- Exact quantities and rates are JSON strings with exactly six fractional
  digits, for example `"5.000000"`. Strings prevent a JSON parser from first
  turning an exact business value into a binary floating-point number.
- Validated Iceberg ingestion converts quantity strings to `DECIMAL(20,6)` and
  contract-rate strings to `DECIMAL(18,6)`. Elexon `generation_mw` is a signed
  `DECIMAL(20,6)` value; steam quantities remain non-negative.
- A missing record is not represented by a fabricated zero. Null is used only
  where the schema explicitly permits it, such as an open-ended effective
  period.
- Every record governed by one of the nine fictional private-source schemas
  requires `synthetic_data: true`. The public Elexon schema does not use that
  label because those observations come from a real external API.
- Source payloads contain source-owned facts. The separate raw-evidence
  envelope supplies ingestion time, SHA-256 hash, R2 location, and API or
  generator lineage without overwriting source timestamps.
- Elexon's camelCase API fields are normalized to snake_case only after the
  untouched response has been stored on R2. Unknown fuel codes fail the
  validated schema and remain available in raw evidence for controlled-list
  review.

JSON Schema cannot compare two independently named fields or compare records.
The validation job must additionally enforce interval duration and timestamp
ordering, non-overlapping effective versions, revision uniqueness and
precedence, assignment/contract coverage, customer consistency, meter-delta
plausibility, capacity arithmetic, and the GB settlement calendar.

## Local structural check

From the repository root, this confirms that every schema is valid JSON:

```bash
for schema in contracts/*.schema.json; do
  python3 -m json.tool "$schema" >/dev/null
done
```

Draft 2020-12 validation must load `common.schema.json` from the same directory
so the relative `$ref` values resolve locally.

The executable contract suite also validates every generated source row and
negative cases for units, timestamp alignment, exact decimals, revision
lineage, finalization, the Elexon sidecar, and the raw-evidence envelope:

```bash
uv run pytest -q tests/contracts
```

FUELHH is a batch-pipeline sidecar and never enters the Phase 2 steam-delivery
fact. Published uses of it must retain the attribution and non-endorsement
rules recorded in PSC-011.
