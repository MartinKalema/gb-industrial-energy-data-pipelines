# Steam delivery business process — plain-English explainer

## Why this document exists

Before designing tables, we need to understand the real-world activity that the
data represents. This document explains the first business process without
assuming knowledge of energy markets, thermal batteries, or dimensional
modeling.

## The real-world setting

An industrial factory may need steam for heating, drying, sterilizing, or
chemical processing. If the steam supply drops, production can slow down or
stop.

A thermal battery helps supply that steam:

1. It uses electricity to create and store heat.
2. It keeps that heat until the factory needs it.
3. It releases the stored heat to produce steam for the factory.

The battery therefore separates the time when electricity is purchased from
the time when steam is delivered. The operator can charge when electricity is
cheaper, cleaner, or more plentiful while continuing to provide steam when the
customer needs it.

## The business problem

The thermal-battery operator has promised each customer a certain amount of
steam. The main question is:

> Are we delivering the steam we promised, and are we doing it reliably and
> economically?

Answering that question requires information from several places:

- The customer contract says how much steam was promised.
- The steam meter says how much steam was actually delivered.
- Plant sensors show what the thermal battery was doing.
- Maintenance records show whether equipment had failed or was unavailable.
- Electricity-market data shows what charging electricity cost.
- Carbon data shows how carbon-intensive that electricity was.

Without a shared data platform, these records may be scattered across systems
or may disagree. An operator might see that steam delivery was low without
knowing whether:

- the battery did not contain enough stored heat;
- equipment had failed;
- the plant charged too late;
- a meter reading was missing or incorrect;
- charging was reduced while electricity was expensive; or
- the customer requested more steam than originally planned.

This project brings that evidence together so the company can detect a delivery
risk, determine what happened, and measure the customer and financial effects.

## The business process

A business process is a repeatable real-world activity that the company wants
to measure. Our first business process is:

> Delivering steam to a customer against an agreed commitment.

The process is:

1. The customer is promised an amount of steam for a period.
2. The thermal battery produces and delivers steam.
3. A meter records how much steam was delivered.
4. The delivered amount is compared with the promised amount.
5. If there is a shortfall, the company investigates the cause.
6. The company calculates the resulting cost, penalty, revenue, and customer
   impact.

## Worked example

Suppose a customer was promised **5.0 MWh of thermal energy** between 10:00 and
10:30, but the meter recorded only **4.8 MWh**.

| Measurement | Value |
|---|---:|
| Promised thermal energy | 5.0 MWh |
| Delivered thermal energy | 4.8 MWh |
| Delivery shortfall | 0.2 MWh |

The first conclusion is that the commitment was missed by 0.2 MWh. The company
must then investigate why. For example, a charging interruption may have left
the battery without enough stored heat. Depending on the customer contract,
the shortfall may cause an SLA penalty or reduce earned revenue.

The numbers above explain the process and remain illustrative. The reporting
grain, source precedence, units, and commercial rules were subsequently
accepted in the dimensional-model decision log.

## How streaming and batch processing support the process

The streaming pipeline answers:

> While the delivery period is still happening, does it look like we might miss
> the commitment?

Live plant sensors continually report measurements such as steam flow, battery
temperature, state of charge, and equipment status. These measurements provide
an early warning so an operator may still have time to intervene. They produce
a provisional operational view, not necessarily the final contractual result.

The batch pipeline answers:

> After the period has finished, how much steam was officially delivered, and
> what were the final consequences?

It reconciles the authoritative meter reading with the applicable commitment
and contract. Separately modeled price, maintenance, telemetry, and other facts
can then be joined through conformed context for investigation; they do not
become measurements in the steam-delivery fact.

Streaming and batch processing therefore describe the same steam-delivery
process from different perspectives:

- **Streaming provides an early warning during delivery.**
- **Batch processing provides the final reconciled result after delivery.**

## Accepted decisions and completed outputs

The authoritative delivery-source decision was accepted as **DM-003** on
2026-08-27: the revenue-grade steam meter determines official delivered steam,
while SCADA remains provisional operational evidence. The decision log contains
the complete correction and missing-reading policy.

The fact grain was accepted as **DM-002** on 2026-08-27: one business row
represents one customer delivery point during one non-overlapping 30-minute
interval. High-frequency SCADA remains separate from this contractual result.

The official delivery event was accepted as **DM-011** on 2026-08-27: delivered
steam is calculated from the difference between the accepted closing and
opening cumulative revenue-meter registers for the same meter. Invalid or
missing boundaries remain provisional rather than being converted to zero.

The commitment definition was accepted as **DM-005** on 2026-08-27: each
delivery point has a scheduled minimum thermal-energy commitment for each
30-minute interval. Extra delivery in a later interval does not erase an
earlier shortfall; daily figures are summaries rather than a second obligation
in the first release.

The measurement-unit decision was accepted as **DM-004** on 2026-08-27:
`MWh_th` is canonical for commitments, delivery, and shortfalls, while steam
mass in metric tonnes remains available as a supporting measure. Original
source values and units are preserved, and electrical energy is labeled
separately as `MWh_e`.

The time rule was accepted as **DM-008** on 2026-08-27: the UTC delivery interval
controls reporting, while customer-facing dates use the `Europe/London` civil
day. Publication, ingestion, and correction times remain separate audit
timestamps and do not move delivery into a later reporting period.

The correction policy was accepted as **DM-009** on 2026-08-27: repeated source
readings produce one accepted business result, late data returns to its original
event-time interval, and the latest valid revision is reported while all raw
versions remain auditable. Missing delivery stays unknown, and invalid or
conflicting measurements are quarantined rather than silently repaired.

The availability definition was accepted as **DM-006** on 2026-08-27:
availability measures the capacity-weighted ability to provide committed
thermal energy, not merely whether the plant was switched on. Planned
maintenance is excluded only when the contract approves it and adjusts the
commitment; intervals without a commitment are not applicable.

The revenue definition was accepted as **DM-007** on 2026-08-27: this process
reports interval earned/accrued revenue and a separate positive accrued SLA
penalty. Net earned revenue subtracts that penalty. Invoices and cash receipts
remain separate future business processes because they occur at different
times and grains.

The customer-visible boundary was accepted as **DM-010** on 2026-08-27:
customers can see only their own approved service outcomes, contract terms, and
data status. Other tenants, raw operations, detailed maintenance records,
procurement cost, margin, and internal notes remain internal. The same boundary
applies to the API, exports, user interface, and future AI tools.

All foundational decisions for this business process are accepted in the
[dimensional-model decision log](decision-log.md). The bus-matrix row, logical
dimensions and facts, metric contracts, and three hand-calculated expected
results are also complete. See the
[Phase 1 completion report](07-phase-1-completion-report.md) for the consolidated
outcome and Phase 2 handoff. No physical schema has been implemented yet.
