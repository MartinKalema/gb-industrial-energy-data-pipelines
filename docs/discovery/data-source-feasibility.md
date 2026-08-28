# Data-source feasibility

Checked on 2026-08-27. Public API behavior and terms can change, so implementation work must pin schemas and retain source metadata.

## Elexon Insights REST API — accepted for batch

- Official developer portal: <https://developer.data.elexon.co.uk/>
- Base URL: `https://data.elexon.co.uk/bmrs/api/v1`
- Public and no API key required.
- Historical and superseded publications are available in formats including JSON and CSV.
- Useful feeds include market-index prices, system prices, demand, generation by fuel, frequency, and forecasts.

Connectivity was verified from this computer with the market-index endpoint. A one-day request returned populated half-hourly price/volume records without authentication.

Example reproducibility check:

```bash
curl --fail --silent --show-error \
  'https://data.elexon.co.uk/bmrs/api/v1/balancing/pricing/market-index?from=2026-08-25T00%3A00Z&to=2026-08-26T00%3A00Z&format=json'
```

Use bounded date windows, retries with jitter, persisted request metadata, source checksums, and idempotent natural keys. Never scrape the website; use the documented API.

## Elexon IRIS — accepted for genuine external streaming

- Official platform explanation: <https://github.com/elexon-data/insights-docs/blob/main/docs/insights_data_platform.md>
- Official clients: <https://github.com/elexon-data/iris-clients>
- Free, near-real-time AMQP push service after account registration.
- Official Python, Node.js, and .NET examples are available.
- API and IRIS messages use compatible formats, so REST can repair gaps.
- Messages have a finite queue lifetime; the consumer must checkpoint and monitor lag.

IRIS credentials belong only in the ignored local `.env` or a local secret store. The bridge will acknowledge a source message only after the event has been durably accepted downstream.

## IRIS archive — accepted for replay testing

- Official archive documentation: <https://github.com/elexon-data/insights-docs/blob/main/docs/iris_archive.md>
- Publicly readable archive of IRIS JSON messages.
- Useful for deterministic replays, schema inspection, and recovering test fixtures.

## NESO Carbon Intensity API — accepted as a secondary feed

- Official API: <https://api.carbonintensity.org.uk/>
- Provides actual and forecast carbon-intensity data for Great Britain.
- Licensed under CC BY 4.0; attribution must appear in the project and product.
- This is an HTTP source, so updates are a poll-based change feed rather than native push.

## Synthetic plant and business data — required

Real industrial telemetry, revenue-grade steam meters, customer contracts, work orders, and billing records are generally proprietary. The project will generate deterministic data for:

- plant, asset, sensor, and delivery-point master data;
- live temperature, state-of-charge, power, flow, pressure, mode, and alarm events;
- steam-meter intervals and corrections;
- customers, effective-dated contracts, commitments, tariffs, and penalties;
- outages, maintenance work orders, invoices, and adjustments.

The simulator should inject a coherent scenario: a price spike, a forced outage, a late sensor message, a delivery shortfall, and a contract revision. A small expected-results fixture will make metric tests exact.

## Initial source scope

Start with only:

1. Elexon market-index price data through REST and IRIS.
2. One visibly live Elexon operational feed such as system frequency or generation mix.
3. NESO carbon intensity.
4. Simulated telemetry, steam commitments, and contracts.

Add other datasets only when a specific product decision requires them.

## Cost boundary

The selected external data feeds are free under their published access terms: Elexon REST needs no key, IRIS is free after registration, and NESO carbon data requires attribution. Cloudflare R2 storage/operations and R2 Data Catalog are infrastructure services, not data APIs, and remain subject to the R2 account's pricing and included usage. Current R2 Data Catalog pricing is documented at <https://developers.cloudflare.com/r2-data-catalog/platform/pricing/>. We will cap generated volume and leave automatic compaction off until its behavior and cost are measured.
