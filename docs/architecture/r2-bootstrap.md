# Safe R2 bootstrap

## Dedicated project account

This project uses a dedicated Cloudflare account and credentials supplied solely
for this local portfolio environment. The credentials stay in the ignored
`.env`, are never committed, and must not be reused for unrelated assets.

## Buckets

Create two private buckets in the **default jurisdiction**:

1. `gb-industrial-thermal-energy-raw-dev` — immutable source payloads and replay evidence; no Iceberg catalog.
2. `gb-industrial-thermal-energy-lakehouse-dev` — Iceberg data/metadata with R2 Data Catalog enabled.

R2 Data Catalog currently does not support non-default-jurisdiction buckets. A location hint is optional and is different from selecting a jurisdiction.

Keep public access disabled for both buckets.

## Enable the catalog

Enable R2 Data Catalog only on `gb-industrial-thermal-energy-lakehouse-dev`. Record these non-secret values in the local `.env`:

- Catalog URI
- Warehouse name
- R2 S3 endpoint

Do not enable automatic compaction or snapshot expiration until the read/write smoke test succeeds and retention/cost behavior is understood.

## Credential roles

If the project later introduces role separation, use:

- a raw-ingestion credential restricted to object read/write on `gb-industrial-thermal-energy-raw-dev`;
- a lakehouse writer/catalog credential with only the R2 storage and Data Catalog write access needed for `gb-industrial-thermal-energy-lakehouse-dev`;
- later, a separate read-only product/query credential.

Store token values, access keys, and secret keys only in the ignored `.env` or local secret mounts.

## First smoke test

1. [x] Upload and retrieve a harmless raw test object; remove it afterward.
2. [x] Create an Iceberg namespace and table through Trino.
3. [x] Commit three events through Spark Structured Streaming.
4. [x] Read the Spark-written records through Trino.
5. [x] Exercise merge, delete, schema evolution, and time travel.
6. [x] Restart the streaming query from its checkpoint and verify no duplicates.
7. [x] Record results in `phase-0-feasibility-results.md`.

## Official references

- R2 bucket creation: <https://developers.cloudflare.com/r2/buckets/create-buckets/>
- R2 API tokens and bucket scoping: <https://developers.cloudflare.com/r2/api/tokens/>
- R2 Data Catalog management: <https://developers.cloudflare.com/r2-data-catalog/manage-catalogs/>
- Trino connection example: <https://developers.cloudflare.com/r2-data-catalog/config-examples/trino/>
