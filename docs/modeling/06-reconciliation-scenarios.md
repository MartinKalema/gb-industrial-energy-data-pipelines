# Steam-delivery reconciliation scenarios

## Purpose

These scenarios validate the accepted dimensional grain and metric contracts
with small inputs that can be calculated by hand. Their exact inputs and
outputs will later become dbt expected-result fixtures and batch/stream
reconciliation tests.

Unless a scenario says otherwise:

- the energy rate is GBP 50 per billable `MWh_th`;
- the SLA penalty rate is GBP 100 per shortfall `MWh_th`;
- all intervals belong to the same customer delivery point and effective
  contract; and
- all quantities are `MWh_th`.

## Scenario 1 — excess cannot erase a later shortfall

Accepted as **DM-016** on 2026-08-27.

### Inputs and interval results

| Interval | Commitment | Opening register | Closing register | Delivered | Deliverable capacity | Shortfall | Excess |
|---|---:|---:|---:|---:|---:|---:|---:|
| 10:00–10:30 | 5.0 | 1,000.0 | 1,005.2 | 5.2 | 5.5 | 0.0 | 0.2 |
| 10:30–11:00 | 5.0 | 1,005.2 | 1,010.0 | 4.8 | 4.8 | 0.2 | 0.0 |

Both authoritative readings are accepted, so delivery-data completeness is
100%.

### Expected aggregate results

| Metric | Expected result |
|---|---:|
| Committed energy | 10.0 MWh_th |
| Delivered energy | 10.0 MWh_th |
| Shortfall energy | 0.2 MWh_th |
| Excess energy | 0.2 MWh_th |
| Delivery-data completeness | 100% |
| SLA attainment | 98% |
| Contractual availability | 98% |
| Billable delivery | 9.8 MWh_th |
| Gross earned revenue | GBP 490 |
| Accrued SLA penalty | GBP 20 |
| Net earned revenue | GBP 470 |

SLA attainment caps delivery at each interval's commitment:

```text
100 * (min(5.2, 5.0) + min(4.8, 5.0)) / (5.0 + 5.0) = 98%
```

Availability applies the same interval-level cap to deliverable capacity:

```text
100 * (min(5.5, 5.0) + min(4.8, 5.0)) / (5.0 + 5.0) = 98%
```

Although total delivered energy equals total commitment, the first interval's
0.2 MWh excess cannot erase the second interval's 0.2 MWh shortfall.

## Scenario 2 — missing authoritative delivery arrives late

Accepted as **DM-017** on 2026-08-27.

### Initial inputs and state

| Interval | Commitment | Opening register | Closing register | Delivered | Deliverable capacity | Status |
|---|---:|---:|---:|---:|---:|---|
| 10:00–10:30 | 5.0 | 2,000.0 | 2,005.0 | 5.0 | 5.0 | Accepted |
| 10:30–11:00 | 5.0 | 2,005.0 | Missing | Unknown | 5.0 | Missing |

### Initial expected results

| Metric | Expected result |
|---|---:|
| Committed energy | 10.0 MWh_th |
| Known delivered energy | 5.0 MWh_th, provisional |
| Delivery-data completeness | 50% |
| Official shortfall | Not final |
| SLA attainment | Not calculated |
| Gross known revenue | GBP 250, provisional |
| Accrued SLA penalty | Unknown |
| Net earned revenue | Unknown |
| Contractual availability | 100%, provided both capacity observations are accepted |

Neither a 50% nor a 100% SLA is valid. Fifty percent would silently treat the
missing delivery as zero; 100% would ignore half of the commitment.

### Late reading and final state

The authoritative closing register later arrives as 2,009.5 MWh. Event time
assigns it to 10:30–11:00 even if ingestion happens on a later day.

```text
second-interval delivery = 2,009.5 - 2,005.0 = 4.5 MWh_th
second-interval shortfall = 5.0 - 4.5 = 0.5 MWh_th
```

| Metric | Expected result after reconciliation |
|---|---:|
| Committed energy | 10.0 MWh_th |
| Delivered energy | 9.5 MWh_th |
| Shortfall energy | 0.5 MWh_th |
| Delivery-data completeness | 100% |
| SLA attainment | 95% |
| Contractual availability | 100% |
| Billable delivery | 9.5 MWh_th |
| Gross earned revenue | GBP 475 |
| Accrued SLA penalty | GBP 50 |
| Net earned revenue | GBP 425 |

## Scenario 3 — corrected shared boundary changes adjacent intervals

Accepted as **DM-018** on 2026-08-27.

Each interval has a 5.0 MWh commitment and 5.0 MWh deliverable capacity.

### Original cumulative readings

| Time | Meter register |
|---|---:|
| 10:00 | 3,000.0 |
| 10:30 | 3,004.7 |
| 11:00 | 3,010.0 |

| Interval | Delivered | Shortfall | Excess | Billable |
|---|---:|---:|---:|---:|
| 10:00–10:30 | 4.7 | 0.3 | 0.0 | 4.7 |
| 10:30–11:00 | 5.3 | 0.0 | 0.3 | 5.0 |

| Metric | Original expected result |
|---|---:|
| Committed energy | 10.0 MWh_th |
| Delivered energy | 10.0 MWh_th |
| Shortfall energy | 0.3 MWh_th |
| Excess energy | 0.3 MWh_th |
| Delivery-data completeness | 100% |
| SLA attainment | 97% |
| Contractual availability | 100% |
| Billable delivery | 9.7 MWh_th |
| Gross earned revenue | GBP 485 |
| Accrued SLA penalty | GBP 30 |
| Net earned revenue | GBP 455 |

### Corrected shared boundary

The accepted 10:30 register is corrected from 3,004.7 to 3,004.9 MWh.

| Interval | Delivered | Shortfall | Excess | Billable |
|---|---:|---:|---:|---:|
| 10:00–10:30 | 4.9 | 0.1 | 0.0 | 4.9 |
| 10:30–11:00 | 5.1 | 0.0 | 0.1 | 5.0 |

| Metric | Corrected expected result |
|---|---:|
| Committed energy | 10.0 MWh_th |
| Delivered energy | 10.0 MWh_th |
| Shortfall energy | 0.1 MWh_th |
| Excess energy | 0.1 MWh_th |
| Delivery-data completeness | 100% |
| SLA attainment | 99% |
| Contractual availability | 100% |
| Billable delivery | 9.9 MWh_th |
| Gross earned revenue | GBP 495 |
| Accrued SLA penalty | GBP 10 |
| Net earned revenue | GBP 485 |

### Why both intervals change

The 10:30 register is one shared boundary: it closes the first interval and
opens the second.

```text
first interval  = middle boundary - start boundary
second interval = end boundary - middle boundary
combined total  = end boundary - start boundary
```

Increasing the middle boundary by 0.2 MWh adds 0.2 MWh to the first interval
and subtracts 0.2 MWh from the second. The combined two-interval delivery stays
at `3,010.0 - 3,000.0 = 10.0 MWh`.

The first shortfall did not physically cause the later excess. They happened to
be equal because the fixed two-interval total exactly equaled the combined
10.0 MWh commitment: the original boundary placed 4.7 MWh in the first interval
and 5.3 MWh in the second, respectively 0.3 MWh below and above their 5.0 MWh
commitments. With different start/end totals or commitments, shortfall and
excess would not necessarily be equal.

This is similar to correcting an odometer reading at a route checkpoint: the
total journey distance is unchanged, but the distance assigned to each side of
the checkpoint changes. Preserve both source revisions, recalculate both
adjacent intervals, and expose only the corrected revision as current.

## Status

All three reconciliation scenarios and their expected results are accepted.
They are ready to become dbt fixtures and batch/stream convergence tests.
