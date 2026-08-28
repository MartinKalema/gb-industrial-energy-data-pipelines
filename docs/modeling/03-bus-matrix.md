# Dimensional bus matrix

## Why this document exists

A bus matrix maps measurable business processes to the descriptive subjects
that provide their context. It helps us reuse consistent definitions across
future facts instead of building disconnected analytical tables.

A row represents a business process. A checked column represents context that
belongs to that process. The matrix does not define physical tables by itself.

## Accepted first row

Accepted as **DM-012** on 2026-08-27:

| Business process | Date | Interval | Customer | Site | Delivery point | Contract | Revenue meter | Data status |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Steam-delivery performance | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

This row describes one accepted business result at the DM-002 grain:

> Steam delivered through one customer delivery point during one
> non-overlapping 30-minute interval.

The descriptive subjects answer:

- **When?** Date and interval
- **For whom?** Customer
- **Where?** Site and delivery point
- **Under which promise and commercial rules?** Contract
- **Measured by what?** Revenue meter
- **How trustworthy is the current result?** Data status

## Deliberate exclusions

### Asset

An asset is not attached directly to the steam-delivery row. Several thermal-
battery components may jointly serve one delivery point. Assigning the complete
delivery to each component would duplicate delivered energy; assigning it to
one component would invent unsupported attribution.

Asset telemetry, operating state, and maintenance will form separate business
processes connected through conformed site and time context. Investigation can
then relate asset evidence to a delivery outcome without changing the delivery
grain.

### Electricity charging, market price, and carbon

Electricity charging occurs at a different time and grain from steam delivery.
Market price and carbon observations also have their own publication,
geography, and interval grains. Copying those measurements directly into the
delivery process would hide storage timing and could misattribute cost or
carbon.

They will become separate future bus-matrix rows and connect through conformed
time and location context plus an explicit thermal-inventory attribution rule.

## Status

The first bus-matrix row, its eight logical dimensions, and twelve delivery fact
measures are accepted. The nine metric contracts and three expected-result
scenarios are also accepted in their linked documents. Exact physical
attributes remain subject to Phase 2 review; no physical dimensional schema has
been implemented.
