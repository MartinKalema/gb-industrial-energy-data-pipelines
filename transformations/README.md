# dbt transformations

dbt will compile and submit SQL through the Trino adapter. Trino will read and write Iceberg tables on R2 through the selected catalog.

Planned model layers:

- `sources/` — declared validated Iceberg sources and freshness expectations
- `staging/` — source-specific renaming, typing, and lightweight cleanup
- `intermediate/` — reusable reconciliation and integration logic
- `marts/` — dimensional products and shared metric inputs agreed in workshops

dbt is not the streaming engine. Spark commits validated streaming micro-batches to Iceberg; dbt incremental models run on a finite cadence when a governed mart needs refresh.
