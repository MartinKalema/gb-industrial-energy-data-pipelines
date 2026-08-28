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
- `intermediate/` — current-revision selection, coverage-driven interval spines,
  event-time integration, shared calculations, and source-knowledge windows.
- `marts/` — eight logical dimensions, five revision-audit companions, the
  30-minute current delivery fact, and its source-knowledge history.

Build the complete project after the bounded Airflow source DAG succeeds:

```bash
uv run dbt build \
  --project-dir transformations \
  --profiles-dir transformations \
  --no-populate-cache
```

The R2 catalog currently needs `--no-populate-cache` to avoid eager list-view
introspection; model creation and data tests still run normally. The local
profile uses one dbt thread to avoid concurrent metadata bursts against the R2
Data Catalog beta. Airflow does not yet invoke dbt automatically.

The first mart is a deliberate full-rebuild correctness baseline. Growing data
does not by itself justify incremental models; a later measured optimization
must produce the same results and include both neighbors of a corrected meter
boundary.

dbt is not the streaming engine. Spark commits validated streaming
micro-batches to Iceberg; dbt submits finite SQL through Trino after committed
snapshots are available. See the
[dimensional-mart architecture and runbook](../docs/architecture/steam-delivery-dbt-dimensional-mart.md).
