# dbt transformations

dbt will compile and submit SQL through the Trino adapter. Trino will read and write Iceberg tables on R2 through the selected catalog.

The project is initialized with dbt Core 1.12.3 and dbt-trino 1.10.3. The
standard `dbt init` support directories are present, while the generic
`models/example` directory is replaced by the layers required by this business
process. Run local commands with:

```bash
uv run dbt debug --project-dir transformations --profiles-dir transformations
```

`profiles.yml` contains no credentials. It connects to the locally exposed
Trino endpoint by default and reads container overrides from environment
variables when Airflow runs dbt.

Model layers:

- `sources/` — the nine validated business sources plus the technical batch
  coverage source. Fixed freshness thresholds are intentionally absent while
  the bounded ingestion DAG remains manually triggered.
- `staging/` — revision-preserving views with explicit columns. They retain
  source types, nullable values, source revisions, and all eight raw-evidence
  lineage fields without filtering, deduplication, ranking, or cleanup.
- `intermediate/` — reusable reconciliation and integration logic
- `marts/` — dimensional products and shared metric inputs agreed in workshops

dbt is not the streaming engine. Spark commits validated streaming micro-batches to Iceberg; dbt incremental models run on a finite cadence when a governed mart needs refresh.
