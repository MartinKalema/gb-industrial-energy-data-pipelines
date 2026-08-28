# Phase 2 source-contract workshop

## Goal

Define where each remaining business input comes from and how revisions become
authoritative before building the batch pipeline. These are source contracts,
not physical Iceberg column definitions.

## Why this happens before coding

A formula can be precise while its input remains ambiguous. For example, Phase
1 defined contractual availability, but Phase 2 must still identify who states
how much energy a delivery point could have supplied. That decision must not be
invented later inside a dbt model.

## Decisions for us to make together

1. **Deliverable capacity:** Which source states how much thermal energy a
   delivery point could have supplied, and when is that value final?
2. **Shared capacity:** How is site capacity allocated when more than one
   delivery point could use it?
3. **Commitment and contract revisions:** Which version applies when schedules
   or contract terms arrive late or change retroactively?
4. **Approved excess orders:** Which record authorizes delivery above the normal
   commitment to become billable?

Preserve these original questions unchanged. Record agreements only in the
Accepted decisions section and the source-contract decision log.

## Accepted decisions

### SC-001 — authoritative delivery-point capacity source

Accepted on 2026-08-27:

> For the Phase 2 batch slice, the authoritative historical capacity input is a
> synthetic, revisioned site-operations assessment for each delivery point and
> 30-minute interval.

In plain English, the record answers:

> During this interval, how much thermal energy could this delivery point have
> supplied if the customer needed it?

Rules:

- Generate the private capacity assessment because genuine industrial plant
  capability records are not public; label it clearly as simulated.
- Produce one assessment at the same delivery-point and 30-minute grain as the
  steam-delivery result.
- Treat the latest valid accepted revision as authoritative for historical
  contractual availability while retaining every earlier revision.
- Use live SCADA only for a provisional capacity estimate. It does not silently
  become the final contractual assessment.
- Use nameplate capacity only as a plausibility ceiling, not as proof that the
  service was actually available.
- If no accepted assessment exists, availability remains provisional/unknown;
  do not convert it to zero or 100%.
- Preserve event, publication, ingestion, and correction timestamps and an
  explicit source revision.
- Phase 4 may replace the simple assessment with a governed derivation from
  detailed assets, outages, derating, state of charge, steam-train limits, and
  delivery constraints without changing the Phase 1 availability definition.

Example:

| Item | Value |
|---|---:|
| Nameplate potential | 7.0 MWh_th |
| Accepted operational restriction | 1.5 MWh_th |
| Accepted deliverable capacity | 5.5 MWh_th |
| Commitment | 5.0 MWh_th |
| Contractual availability | 100% |

```text
100 * min(5.5, 5.0) / 5.0 = 100%
```

### SC-002 — shared-capacity scope

Accepted on 2026-08-27:

> During the Phase 2 batch slice, each site has exactly one active customer
> delivery point during any event-time interval.

Rules:

- Assign the site's accepted capacity assessment entirely to its one active
  delivery point.
- Allow multiple customers and multiple sites in the portfolio; the restriction
  is one active delivery point within each site at a time.
- Enforce the rule with a data-quality test over effective-time site-to-delivery
  point relationships.
- If a site changes delivery points historically, the effective periods must
  not overlap.
- Never copy the site's full capacity to multiple delivery points because that
  would report more capacity than the site can actually supply.
- Defer multi-delivery-point allocation to Phase 4, where contract priority or
  an approved operator allocation schedule can be modeled explicitly.

### SC-003 — commitment and contract revision precedence

Accepted on 2026-08-27:

- Preserve both effective time (when the business says a version applies) and
  publication time (when that version became available to the platform).
- Use the latest valid approved revision whose effective period covers the
  delivery interval for the current accepted view.
- Allow normal approved schedule changes before delivery begins.
- Require an explicit correction reason and approval for a commitment change
  submitted after delivery.
- Preserve original and corrected commitment and contract versions and
  recalculate affected shortfall and financial results after an approved
  correction.
- Never allow an older late-arriving version to replace a newer accepted
  revision.
- Require an approved contract amendment with an explicit effective date before
  a rate or penalty can change historical results.
- Provide a current accepted view using the latest approved revisions and an
  as-known-at-the-time view using only information published by the requested
  cutoff.

Example:

| Version | Commitment | Published | Effective interval | Treatment |
|---|---:|---|---|---|
| 1 | 5.0 MWh_th | Monday | Monday 10:00–10:30 | Original accepted schedule |
| 2 | 5.5 MWh_th | Tuesday | Monday 10:00–10:30 | Current only after explicit retroactive approval |

The current accepted view uses 5.5 MWh after approval. An as-known-on-Monday
view continues to show 5.0 MWh.

### SC-004 — approved excess-order authority

Accepted on 2026-08-27:

> For the Phase 2 batch slice, a synthetic, revisioned customer excess-order
> record from the order-management process is the only authority that can make
> delivery above the normal commitment billable.

In plain English, the normal commitment remains the customer's service promise.
An approved excess order gives the operator permission to supply and bill up to
a stated additional quantity for specified 30-minute intervals. It does not
increase the normal commitment or create a new SLA shortfall if the optional
extra energy is not delivered.

Rules:

- Link every excess order to a customer, delivery point, and one or more
  effective 30-minute intervals.
- State the maximum additional billable energy in `MWh_th`.
- Require the order to be approved and published before the affected interval
  begins. A later record cannot retroactively turn previously unbilled excess
  into revenue during Phase 2.
- Preserve every revision and cancellation, including effective, publication,
  ingestion, and approval timestamps.
- Select the latest valid approved revision using the SC-003 precedence rules.
- Inherit the effective contract energy rate; do not introduce a separate
  excess-energy price during Phase 2.
- Without a valid approved order, delivery above the normal commitment remains
  unbilled excess.
- Keep the base commitment unchanged when calculating SLA attainment and
  shortfall.

```text
billable_mwh_th = min(
    delivered_mwh_th,
    committed_mwh_th + approved_extra_mwh_th
)
```

Example:

| Item | Value |
|---|---:|
| Normal commitment | 5.0 MWh_th |
| Approved additional quantity | 0.4 MWh_th |
| Delivered | 5.6 MWh_th |
| Billable | 5.4 MWh_th |
| Unbilled excess | 0.2 MWh_th |

At an effective contract rate of GBP 50/MWh_th, gross energy revenue is GBP
270. Without the approved order, billable energy would remain 5.0 MWh_th and
gross energy revenue would be GBP 250.

## Status

SC-001 through SC-004 are accepted, so all four original Phase 2 source-contract
questions have agreed answers. No physical source schema or generated dataset
has been created yet. The next step is to translate these accepted business
rules into explicit source contracts before generating data.
