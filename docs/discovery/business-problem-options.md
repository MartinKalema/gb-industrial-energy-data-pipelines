# Business-problem options

We should choose the business problem before locking the dimensional model. The following options were evaluated for energy-domain relevance, free data, batch and streaming value, dimensional richness, and alignment with the target role.

| Option | Problem | Free public data | Batch | Genuine stream | Role alignment | Main drawback |
|---|---|---:|---:|---:|---:|---|
| **GB thermal-battery delivery and dispatch** | Meet steam commitments while charging under favorable market/grid conditions | Elexon, NESO | Strong | **Strong: Elexon IRIS AMQP** | **Strongest** | Requires learning GB settlement concepts |
| California industrial heat storage | Shift charging away from costly or constrained periods | CAISO OASIS | Strong | Moderate: mostly polling | Strong | More awkward API and history access |
| US grid-stress reliability | Relate industrial heat reliability to balancing-authority demand and generation | US EIA | Strong | Weak: hourly polling | Moderate | Less compelling real-time and price story |

## Accepted decision

The **GB thermal-battery delivery and dispatch** problem was accepted on 2026-08-27.

It preserves the industrial-energy and steam-delivery story from the role while giving the project something unusually valuable: a public REST backfill interface and a genuine public push service for the same market domain. The private side of the business remains an explicit, deterministic simulation rather than pretending that proprietary telemetry is public.

## Recommended first business question

> Did each site meet its steam commitment, and what were the electricity-cost, carbon, availability, and revenue consequences of when it charged?

This is deliberately narrower than “optimize an energy company” and broad enough to connect plant operations, external markets, contracts, customers, security, and later AI investigations.

## Alternatives we can still choose

Changing geography now is inexpensive. Changing it after building source contracts, timestamps, and market dimensions will be costly. Workshop 1 therefore records the geography and business question as explicit decisions.
