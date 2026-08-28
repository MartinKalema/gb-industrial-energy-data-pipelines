"""Idempotent bounded loads from accepted JSONL into R2-backed Iceberg tables.

The module intentionally depends only on the Python standard library.  It uses
Trino's statement HTTP protocol, so the Airflow runtime does not need a second
database driver merely to submit finite Iceberg writes.

The loader owns one important invariant: an immutable source-revision identity
may have exactly one canonical payload hash.  Exact replays are skipped, while
the same identity with different content is reported as a conflict and is
never used to update the existing row.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol

from .models import PipelineError


SYNTHETIC_DATASETS: tuple[str, ...] = (
    "customer_master",
    "industrial_site_master",
    "delivery_point_assignment",
    "revenue_meter_assignment",
    "contract_terms",
    "commitment_schedule",
    "approved_excess_order",
    "revenue_meter_reading",
    "delivery_point_capacity_assessment",
)

# These are immutable *published revision* identities, not current-view keys.
# The table itself already scopes the dataset, while source_system_id prevents
# two upstream systems from accidentally sharing a revision identifier.
IDENTITY_FIELDS: Mapping[str, tuple[str, ...]] = {
    "customer_master": (
        "source_system_id",
        "customer_version_id",
        "source_revision",
    ),
    "industrial_site_master": (
        "source_system_id",
        "site_version_id",
        "source_revision",
    ),
    "delivery_point_assignment": (
        "source_system_id",
        "delivery_point_assignment_id",
        "source_revision",
    ),
    "revenue_meter_assignment": (
        "source_system_id",
        "meter_assignment_id",
        "source_revision",
    ),
    "contract_terms": (
        "source_system_id",
        "contract_terms_version_id",
        "source_revision",
    ),
    "commitment_schedule": (
        "source_system_id",
        "delivery_point_natural_id",
        "interval_start_utc",
        "source_revision",
    ),
    "approved_excess_order": (
        "source_system_id",
        "order_interval_line_id",
        "source_revision",
    ),
    "revenue_meter_reading": (
        "source_system_id",
        "meter_natural_id",
        "register_natural_id",
        "reading_at_utc",
        "source_revision",
    ),
    "delivery_point_capacity_assessment": (
        "source_system_id",
        "delivery_point_natural_id",
        "interval_start_utc",
        "source_revision",
    ),
}

SAFE_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SHA256_PATTERN = re.compile(r"^[a-f0-9]{64}$")
DECIMAL_TYPE_PATTERN = re.compile(r"^DECIMAL\((\d+),(\d+)\)$")
UTC_TIMESTAMP_PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)Z$"
)


class TrinoLoaderError(PipelineError):
    """Base error for a load that must not advance the Airflow workflow."""


class AcceptedRecordError(TrinoLoaderError):
    """An alleged accepted record is malformed or incompatible with its schema."""


class TrinoProtocolError(TrinoLoaderError):
    """Trino returned an invalid statement-protocol response."""


class TrinoQueryError(TrinoLoaderError):
    """Trino rejected a statement."""

    def __init__(self, message: str, *, error_name: str | None = None):
        self.error_name = error_name
        prefix = f"{error_name}: " if error_name else ""
        super().__init__(prefix + message)


class TableContractError(TrinoLoaderError):
    """An existing Iceberg table is incompatible with the source contract."""


@dataclass(frozen=True)
class QueryResult:
    """Small result returned by a Trino statement runner."""

    columns: tuple[str, ...] = ()
    rows: tuple[tuple[Any, ...], ...] = ()
    update_type: str | None = None
    update_count: int | None = None


class StatementRunner(Protocol):
    """Injection boundary used by tests and by the stdlib HTTP client."""

    def execute(self, sql: str) -> QueryResult: ...


class TrinoHttpClient:
    """Minimal synchronous client for Trino's ``/v1/statement`` protocol."""

    def __init__(
        self,
        endpoint: str,
        *,
        user: str = "airflow",
        source: str = "industrial-energy-bounded-batch",
        timeout_seconds: float = 60.0,
        query_timeout_seconds: float = 300.0,
        headers: Mapping[str, str] | None = None,
    ):
        endpoint = endpoint.rstrip("/")
        parsed = urllib.parse.urlparse(endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Trino endpoint must be an absolute HTTP(S) URL")
        if not user.strip() or not source.strip():
            raise ValueError("Trino user and source must be non-empty")
        if timeout_seconds <= 0:
            raise ValueError("Trino timeout_seconds must be positive")
        if query_timeout_seconds <= 0:
            raise ValueError("Trino query_timeout_seconds must be positive")
        self.endpoint = endpoint
        self.timeout_seconds = float(timeout_seconds)
        self.query_timeout_seconds = float(query_timeout_seconds)
        self.headers = {
            "Accept": "application/json",
            "X-Trino-User": user,
            "X-Trino-Source": source,
            **{str(key): str(value) for key, value in (headers or {}).items()},
        }
        # The server-side limit is the fallback when the initial POST times out
        # before the client has received a cancellable nextUri. Keep this
        # invariant even if a caller supplies additional protocol headers.
        self.headers["X-Trino-Session"] = (
            f"query_max_execution_time={self.query_timeout_seconds:g}s"
        )

    def execute(self, sql: str) -> QueryResult:
        if not sql.strip():
            raise ValueError("SQL statement must be non-empty")
        deadline = time.monotonic() + self.query_timeout_seconds
        statement_url = f"{self.endpoint}/v1/statement"
        response = self._request(
            statement_url,
            method="POST",
            body=sql.encode("utf-8"),
            content_type="text/plain; charset=utf-8",
            timeout_seconds=min(self.timeout_seconds, self.query_timeout_seconds),
        )

        columns: tuple[str, ...] = ()
        rows: list[tuple[Any, ...]] = []
        update_type: str | None = None
        update_count: int | None = None
        while True:
            self._raise_query_error(response)
            if response.get("columns"):
                columns = tuple(str(column["name"]) for column in response["columns"])
            for row in response.get("data", ()):
                if not isinstance(row, list):
                    raise TrinoProtocolError("Trino data row is not an array")
                rows.append(tuple(row))
            if response.get("updateType") is not None:
                update_type = str(response["updateType"])
            if response.get("updateCount") is not None:
                update_count = int(response["updateCount"])

            next_uri = response.get("nextUri")
            if not next_uri:
                break
            if not isinstance(next_uri, str):
                raise TrinoProtocolError("Trino nextUri is not a string")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._cancel(next_uri)
                raise TrinoProtocolError(
                    f"Trino query exceeded {self.query_timeout_seconds:g} seconds"
                )
            try:
                response = self._request(
                    next_uri,
                    method="GET",
                    timeout_seconds=min(self.timeout_seconds, remaining),
                )
            except TrinoProtocolError:
                self._cancel(next_uri)
                raise

        return QueryResult(
            columns=columns,
            rows=tuple(rows),
            update_type=update_type,
            update_count=update_count,
        )

    def _request(
        self,
        url: str,
        *,
        method: str,
        body: bytes | None = None,
        content_type: str | None = None,
        timeout_seconds: float | None = None,
    ) -> Mapping[str, Any]:
        headers = dict(self.headers)
        if content_type:
            headers["Content-Type"] = content_type
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(
                request,
                timeout=timeout_seconds or self.timeout_seconds,
            ) as response:
                payload = response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise TrinoProtocolError(
                f"Trino HTTP request failed with status {exc.code}: {detail[:500]}"
            ) from exc
        except urllib.error.URLError as exc:
            raise TrinoProtocolError(f"Trino HTTP request failed: {exc.reason}") from exc
        except TimeoutError as exc:
            raise TrinoProtocolError("Trino HTTP request timed out") from exc
        try:
            decoded = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TrinoProtocolError("Trino returned non-JSON statement output") from exc
        if not isinstance(decoded, dict):
            raise TrinoProtocolError("Trino statement response is not an object")
        return decoded

    def _cancel(self, next_uri: str) -> None:
        """Best-effort cancellation for a timed-out or disconnected query."""

        request = urllib.request.Request(
            next_uri,
            headers=self.headers,
            method="DELETE",
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=min(self.timeout_seconds, 5.0),
            ) as response:
                response.read()
        except Exception:
            # The original timeout/protocol failure is the actionable error.
            # Cancellation may race with a query that Trino already finished.
            return

    @staticmethod
    def _raise_query_error(response: Mapping[str, Any]) -> None:
        error = response.get("error")
        if not error:
            return
        if not isinstance(error, Mapping):
            raise TrinoQueryError("Trino returned an unstructured query error")
        raise TrinoQueryError(
            str(error.get("message", "Trino query failed")),
            error_name=str(error["errorName"]) if error.get("errorName") else None,
        )


@dataclass(frozen=True)
class IcebergLoaderConfig:
    """Non-secret settings for the validated-source Iceberg boundary."""

    trino_endpoint: str = "http://127.0.0.1:8080"
    catalog: str = "r2"
    iceberg_schema: str = "industrial_energy_validated"
    trino_user: str = "airflow"
    trino_source: str = "industrial-energy-bounded-batch"
    timeout_seconds: float = 60.0
    query_timeout_seconds: float = 300.0
    chunk_size: int = 200
    max_record_bytes: int = 1_048_576
    conflict_detail_limit: int = 50
    verify_existing_table: bool = True

    def __post_init__(self) -> None:
        for label, value in (
            ("catalog", self.catalog),
            ("iceberg_schema", self.iceberg_schema),
        ):
            if not SAFE_SQL_IDENTIFIER.fullmatch(value):
                raise ValueError(f"{label} is not a safe unquoted SQL identifier")
        parsed = urllib.parse.urlparse(self.trino_endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("trino_endpoint must be an absolute HTTP(S) URL")
        if not self.trino_user.strip() or not self.trino_source.strip():
            raise ValueError("trino_user and trino_source must be non-empty")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.query_timeout_seconds <= 0:
            raise ValueError("query_timeout_seconds must be positive")
        if self.chunk_size < 1 or self.chunk_size > 5_000:
            raise ValueError("chunk_size must be between 1 and 5000")
        if self.max_record_bytes < 1:
            raise ValueError("max_record_bytes must be positive")
        if self.conflict_detail_limit < 0:
            raise ValueError("conflict_detail_limit must be non-negative")


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    sql_type: str
    required: bool
    explicit_null_allowed: bool
    pattern: str | None = None
    allowed_values: tuple[Any, ...] = ()
    constant: Any = field(default=None, repr=False)
    has_constant: bool = False

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        if not self.has_constant:
            result.pop("constant", None)
        return result


PIPELINE_COLUMNS: tuple[ColumnSpec, ...] = (
    ColumnSpec("pipeline_run_id", "VARCHAR", True, False),
    ColumnSpec("pipeline_evidence_envelope_id", "VARCHAR", True, False),
    ColumnSpec("pipeline_ingested_at_utc", "TIMESTAMP(6) WITH TIME ZONE", True, False),
    ColumnSpec("pipeline_raw_object_uri", "VARCHAR", True, False),
    ColumnSpec("pipeline_raw_object_sha256", "VARCHAR", True, False),
    ColumnSpec("pipeline_raw_record_locator", "VARCHAR", True, False),
    ColumnSpec("pipeline_identity_sha256", "VARCHAR", True, False),
    ColumnSpec("pipeline_payload_sha256", "VARCHAR", True, False),
)


@dataclass(frozen=True)
class TablePlan:
    dataset: str
    table: str
    identity_fields: tuple[str, ...]
    source_columns: tuple[ColumnSpec, ...]
    pipeline_columns: tuple[ColumnSpec, ...]
    create_schema_sql: str
    create_table_sql: str

    @property
    def columns(self) -> tuple[ColumnSpec, ...]:
        return self.source_columns + self.pipeline_columns

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "table": self.table,
            "identity_fields": list(self.identity_fields),
            "source_columns": [column.to_dict() for column in self.source_columns],
            "pipeline_columns": [column.to_dict() for column in self.pipeline_columns],
            "create_schema_sql": self.create_schema_sql,
            "create_table_sql": self.create_table_sql,
        }


@dataclass(frozen=True)
class AcceptedRecord:
    """One validated source row plus lineage supplied by the raw-evidence task."""

    payload: Mapping[str, Any]
    pipeline_run_id: str
    evidence_envelope_id: str
    ingested_at_utc: str
    raw_object_uri: str
    raw_object_sha256: str
    raw_record_locator: str


@dataclass(frozen=True)
class IdentityConflict:
    dataset: str
    identity_sha256: str
    identity: Mapping[str, Any]
    incoming_payload_sha256: tuple[str, ...]
    existing_payload_sha256: tuple[str, ...] = ()
    reason: str = "immutable identity has different content"

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["identity"] = dict(self.identity)
        value["incoming_payload_sha256"] = list(self.incoming_payload_sha256)
        value["existing_payload_sha256"] = list(self.existing_payload_sha256)
        return value


@dataclass(frozen=True)
class LoadResult:
    """Bounded, JSON-serializable summary suitable for an Airflow XCom."""

    dataset: str
    table: str
    dry_run: bool
    database_checks_performed: bool
    input_records: int
    planned_new_records: int
    inserted_records: int
    skipped_exact_replays: int
    conflict_records: int
    conflict_identities: int
    chunks_processed: int
    conflicts: tuple[IdentityConflict, ...] = ()
    conflict_details_truncated: bool = False
    warnings: tuple[str, ...] = ()

    @property
    def succeeded_without_conflicts(self) -> bool:
        return self.conflict_records == 0

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["conflicts"] = [conflict.to_dict() for conflict in self.conflicts]
        value["warnings"] = list(self.warnings)
        value["succeeded_without_conflicts"] = self.succeeded_without_conflicts
        return value


@dataclass(frozen=True)
class _PreparedRecord:
    accepted: AcceptedRecord
    source_values: tuple[Any, ...]
    identity: Mapping[str, Any]
    identity_sha256: str
    payload_sha256: str


@dataclass
class _LoadAccumulator:
    input_records: int = 0
    planned_new_records: int = 0
    inserted_records: int = 0
    skipped_exact_replays: int = 0
    conflict_records: int = 0
    conflict_identities: int = 0
    chunks_processed: int = 0
    conflicts: list[IdentityConflict] = field(default_factory=list)
    conflict_identity_hashes: set[str] = field(default_factory=set)
    conflict_details_truncated: bool = False
    warnings: list[str] = field(default_factory=list)


class TrinoIcebergLoader:
    """Plan, create, verify, and append the nine validated source tables."""

    def __init__(
        self,
        config: IcebergLoaderConfig,
        *,
        contracts_dir: Path | None = None,
        statement_runner: StatementRunner | None = None,
    ):
        self.config = config
        repository_root = Path(__file__).resolve().parents[3]
        self.contracts_dir = (contracts_dir or repository_root / "contracts").resolve()
        self.statement_runner = statement_runner or TrinoHttpClient(
            config.trino_endpoint,
            user=config.trino_user,
            source=config.trino_source,
            timeout_seconds=config.timeout_seconds,
            query_timeout_seconds=config.query_timeout_seconds,
        )
        self._plans: dict[str, TablePlan] = {}

    def plan_table(self, dataset: str) -> TablePlan:
        """Read one JSON Schema and return typed DDL without calling Trino."""

        self._require_dataset(dataset)
        if dataset in self._plans:
            return self._plans[dataset]
        schema_path = self.contracts_dir / f"{dataset}.schema.json"
        try:
            schema = json.loads(schema_path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise TableContractError(f"source contract is missing: {schema_path}") from exc
        except json.JSONDecodeError as exc:
            raise TableContractError(f"source contract is invalid JSON: {schema_path}") from exc
        if not isinstance(schema, dict) or not isinstance(schema.get("properties"), dict):
            raise TableContractError(f"source contract has no properties: {schema_path}")

        required = set(schema.get("required", ()))
        source_columns: list[ColumnSpec] = []
        for name, definition in schema["properties"].items():
            if not SAFE_SQL_IDENTIFIER.fullmatch(name):
                raise TableContractError(f"unsafe source column name {name!r}")
            source_columns.append(
                self._column_spec(
                    name,
                    definition,
                    required=name in required,
                )
            )
        source_names = {column.name for column in source_columns}
        missing_identity = set(IDENTITY_FIELDS[dataset]) - source_names
        if missing_identity:
            raise TableContractError(
                f"{dataset} contract is missing identity fields: {sorted(missing_identity)}"
            )
        overlap = source_names & {column.name for column in PIPELINE_COLUMNS}
        if overlap:
            raise TableContractError(
                f"{dataset} source columns collide with pipeline metadata: {sorted(overlap)}"
            )

        qualified_schema = self._qualified_schema()
        qualified_table = self._qualified_table(dataset)
        all_columns = (*source_columns, *PIPELINE_COLUMNS)
        column_ddl = ",\n  ".join(
            f"{_quote_identifier(column.name)} {column.sql_type}" for column in all_columns
        )
        plan = TablePlan(
            dataset=dataset,
            table=qualified_table,
            identity_fields=IDENTITY_FIELDS[dataset],
            source_columns=tuple(source_columns),
            pipeline_columns=PIPELINE_COLUMNS,
            create_schema_sql=f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}",
            create_table_sql=(
                f"CREATE TABLE IF NOT EXISTS {qualified_table} (\n"
                f"  {column_ddl}\n"
                ")\nWITH (\n"
                "  format = 'PARQUET',\n"
                "  format_version = 2\n"
                ")"
            ),
        )
        self._plans[dataset] = plan
        return plan

    def plan_all_tables(self) -> tuple[TablePlan, ...]:
        """Return deterministic plans for all nine synthetic source tables."""

        return tuple(self.plan_table(dataset) for dataset in SYNTHETIC_DATASETS)

    def ensure_table(self, dataset: str) -> TablePlan:
        """Create one table if needed, then verify its source/metadata columns."""

        plan = self.plan_table(dataset)
        self.statement_runner.execute(plan.create_schema_sql)
        self.statement_runner.execute(plan.create_table_sql)
        if self.config.verify_existing_table:
            self._verify_table(plan)
        return plan

    def ensure_all_tables(self) -> tuple[TablePlan, ...]:
        """Create and verify every accepted synthetic-source table."""

        return tuple(self.ensure_table(dataset) for dataset in SYNTHETIC_DATASETS)

    def load_jsonl(
        self,
        dataset: str,
        accepted_jsonl: Path,
        *,
        pipeline_run_id: str,
        evidence_envelope_id: str,
        ingested_at_utc: str,
        raw_object_uri: str,
        raw_object_sha256: str,
        dry_run: bool = False,
    ) -> LoadResult:
        """Load a plain accepted JSONL file with one raw locator per input line.

        Dry-run parses and plans every row but performs no Trino request.  It can
        identify conflicts inside the proposed input, but existing-table replay
        and conflict checks necessarily remain unknown until a live run.
        """

        path = accepted_jsonl.resolve()
        if not path.is_file():
            raise AcceptedRecordError(f"accepted JSONL does not exist: {path}")

        def records() -> Iterator[AcceptedRecord]:
            with path.open("rb") as handle:
                for line_number, raw_line in enumerate(handle, start=1):
                    if len(raw_line) > self.config.max_record_bytes:
                        raise AcceptedRecordError(
                            f"{path.name}:{line_number} exceeds max_record_bytes"
                        )
                    if not raw_line.strip():
                        raise AcceptedRecordError(
                            f"{path.name}:{line_number} is an empty JSONL record"
                        )
                    payload = _decode_json_object(raw_line, path.name, line_number)
                    yield AcceptedRecord(
                        payload=payload,
                        pipeline_run_id=pipeline_run_id,
                        evidence_envelope_id=evidence_envelope_id,
                        ingested_at_utc=ingested_at_utc,
                        raw_object_uri=raw_object_uri,
                        raw_object_sha256=raw_object_sha256,
                        raw_record_locator=f"line:{line_number}",
                    )

        return self.load_records(dataset, records(), dry_run=dry_run)

    def load_records(
        self,
        dataset: str,
        records: Iterable[AcceptedRecord],
        *,
        dry_run: bool = False,
    ) -> LoadResult:
        """Load an iterable in bounded chunks without ever updating a row."""

        plan = self.plan_table(dataset)
        if not dry_run:
            self.ensure_table(dataset)
        accumulator = _LoadAccumulator()
        dry_run_seen: dict[str, str] = {}

        for raw_chunk in _chunks(records, self.config.chunk_size):
            accumulator.chunks_processed += 1
            prepared = [self._prepare_record(plan, record) for record in raw_chunk]
            accumulator.input_records += len(prepared)
            self._load_chunk(
                plan,
                prepared,
                accumulator,
                dry_run=dry_run,
                dry_run_seen=dry_run_seen,
            )

        return LoadResult(
            dataset=dataset,
            table=plan.table,
            dry_run=dry_run,
            database_checks_performed=not dry_run,
            input_records=accumulator.input_records,
            planned_new_records=accumulator.planned_new_records,
            inserted_records=accumulator.inserted_records,
            skipped_exact_replays=accumulator.skipped_exact_replays,
            conflict_records=accumulator.conflict_records,
            conflict_identities=accumulator.conflict_identities,
            chunks_processed=accumulator.chunks_processed,
            conflicts=tuple(accumulator.conflicts),
            conflict_details_truncated=accumulator.conflict_details_truncated,
            warnings=tuple(accumulator.warnings),
        )

    def _load_chunk(
        self,
        plan: TablePlan,
        records: Sequence[_PreparedRecord],
        accumulator: _LoadAccumulator,
        *,
        dry_run: bool,
        dry_run_seen: dict[str, str],
    ) -> None:
        grouped: dict[str, list[_PreparedRecord]] = {}
        for record in records:
            grouped.setdefault(record.identity_sha256, []).append(record)

        candidates: list[_PreparedRecord] = []
        candidate_occurrences: dict[str, int] = {}
        for identity_sha256, group in grouped.items():
            payload_hashes = {record.payload_sha256 for record in group}
            if len(payload_hashes) > 1:
                self._record_conflict(
                    accumulator,
                    plan.dataset,
                    group[0],
                    incoming_hashes=payload_hashes,
                    existing_hashes=(),
                    record_count=len(group),
                    reason="one incoming chunk contains different content for one identity",
                )
                continue
            candidates.append(group[0])
            candidate_occurrences[identity_sha256] = len(group)

        if dry_run:
            for candidate in candidates:
                previous_hash = dry_run_seen.get(candidate.identity_sha256)
                occurrences = candidate_occurrences[candidate.identity_sha256]
                if previous_hash is None:
                    dry_run_seen[candidate.identity_sha256] = candidate.payload_sha256
                    accumulator.planned_new_records += 1
                    accumulator.skipped_exact_replays += occurrences - 1
                elif previous_hash == candidate.payload_sha256:
                    accumulator.skipped_exact_replays += occurrences
                else:
                    self._record_conflict(
                        accumulator,
                        plan.dataset,
                        candidate,
                        incoming_hashes={candidate.payload_sha256},
                        existing_hashes={previous_hash},
                        record_count=occurrences,
                        reason="dry-run input reuses an earlier identity with different content",
                    )
            return

        existing = self._existing_payload_hashes(plan, candidates)
        new_records: list[_PreparedRecord] = []
        for candidate in candidates:
            existing_hashes = existing.get(candidate.identity_sha256, set())
            occurrences = candidate_occurrences[candidate.identity_sha256]
            if not existing_hashes:
                new_records.append(candidate)
            elif existing_hashes == {candidate.payload_sha256}:
                accumulator.skipped_exact_replays += occurrences
            else:
                self._record_conflict(
                    accumulator,
                    plan.dataset,
                    candidate,
                    incoming_hashes={candidate.payload_sha256},
                    existing_hashes=existing_hashes,
                    record_count=occurrences,
                    reason="Iceberg already contains different content for this identity",
                )

        accumulator.planned_new_records += len(new_records)
        if not new_records:
            return

        merge_result = self.statement_runner.execute(self._merge_sql(plan, new_records))
        confirmed = self._existing_payload_hashes(plan, new_records)
        confirmed_exact: list[_PreparedRecord] = []
        for record in new_records:
            hashes = confirmed.get(record.identity_sha256, set())
            if hashes == {record.payload_sha256}:
                confirmed_exact.append(record)
            elif hashes:
                self._record_conflict(
                    accumulator,
                    plan.dataset,
                    record,
                    incoming_hashes={record.payload_sha256},
                    existing_hashes=hashes,
                    record_count=candidate_occurrences[record.identity_sha256],
                    reason="a concurrent writer committed different content for this identity",
                )
            else:
                raise TableContractError(
                    f"MERGE completed but {record.identity_sha256} is absent from {plan.table}"
                )

        if merge_result.update_count is None:
            accumulator.inserted_records += len(confirmed_exact)
            accumulator.skipped_exact_replays += sum(
                candidate_occurrences[record.identity_sha256] - 1
                for record in confirmed_exact
            )
            warning = (
                "Trino omitted MERGE updateCount; inserted_records reflects rows "
                "confirmed present after the statement"
            )
            if warning not in accumulator.warnings:
                accumulator.warnings.append(warning)
            return
        if merge_result.update_count < 0 or merge_result.update_count > len(confirmed_exact):
            raise TrinoProtocolError(
                "Trino MERGE updateCount is inconsistent with the post-write identity check"
            )
        accumulator.inserted_records += merge_result.update_count
        accumulator.skipped_exact_replays += (
            sum(candidate_occurrences[record.identity_sha256] for record in confirmed_exact)
            - merge_result.update_count
        )

    def _prepare_record(self, plan: TablePlan, accepted: AcceptedRecord) -> _PreparedRecord:
        payload = accepted.payload
        if not isinstance(payload, Mapping):
            raise AcceptedRecordError(f"{plan.dataset} payload must be a JSON object")
        expected_names = {column.name for column in plan.source_columns}
        unknown = set(payload) - expected_names
        missing = {
            column.name
            for column in plan.source_columns
            if column.required and column.name not in payload
        }
        if unknown:
            raise AcceptedRecordError(
                f"{plan.dataset} payload has unknown fields: {sorted(unknown)}"
            )
        if missing:
            raise AcceptedRecordError(
                f"{plan.dataset} payload lacks required fields: {sorted(missing)}"
            )
        if payload.get("source_schema_id") != plan.dataset:
            raise AcceptedRecordError(
                f"source_schema_id must be {plan.dataset!r} for this table"
            )

        source_values: list[Any] = []
        for column in plan.source_columns:
            present = column.name in payload
            value = payload.get(column.name)
            if present and value is None and not column.explicit_null_allowed:
                raise AcceptedRecordError(f"{column.name} does not allow explicit null")
            if present and value is not None:
                self._validate_column_value(column, value)
            source_values.append(value)

        identity = {name: payload[name] for name in plan.identity_fields}
        identity_sha256 = _sha256_json(
            {"dataset": plan.dataset, "identity": identity}
        )
        payload_sha256 = _sha256_json(payload)
        self._validate_lineage(accepted)
        return _PreparedRecord(
            accepted=accepted,
            source_values=tuple(source_values),
            identity=identity,
            identity_sha256=identity_sha256,
            payload_sha256=payload_sha256,
        )

    @staticmethod
    def _validate_lineage(accepted: AcceptedRecord) -> None:
        for label, value in (
            ("pipeline_run_id", accepted.pipeline_run_id),
            ("evidence_envelope_id", accepted.evidence_envelope_id),
            ("raw_object_uri", accepted.raw_object_uri),
            ("raw_record_locator", accepted.raw_record_locator),
        ):
            if not isinstance(value, str) or not value.strip():
                raise AcceptedRecordError(f"{label} must be a non-empty string")
        if not SHA256_PATTERN.fullmatch(accepted.raw_object_sha256):
            raise AcceptedRecordError("raw_object_sha256 must be a lowercase SHA-256")
        _timestamp_literal(accepted.ingested_at_utc)

    @staticmethod
    def _validate_column_value(column: ColumnSpec, value: Any) -> None:
        sql_type = column.sql_type
        if column.has_constant and value != column.constant:
            raise AcceptedRecordError(
                f"{column.name} must equal the schema constant {column.constant!r}"
            )
        if column.allowed_values and value not in column.allowed_values:
            raise AcceptedRecordError(
                f"{column.name} is not one of the schema's controlled values"
            )
        if sql_type == "VARCHAR":
            if not isinstance(value, str):
                raise AcceptedRecordError(f"{column.name} must be a string")
        elif sql_type == "BOOLEAN":
            if not isinstance(value, bool):
                raise AcceptedRecordError(f"{column.name} must be a boolean")
        elif sql_type == "BIGINT":
            if not isinstance(value, int) or isinstance(value, bool):
                raise AcceptedRecordError(f"{column.name} must be an integer")
        elif sql_type == "DOUBLE":
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise AcceptedRecordError(f"{column.name} must be numeric")
            if not math.isfinite(float(value)):
                raise AcceptedRecordError(f"{column.name} must be finite")
        elif sql_type == "DATE":
            if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                raise AcceptedRecordError(f"{column.name} must be an ISO date string")
        elif sql_type == "TIMESTAMP(6) WITH TIME ZONE":
            if not isinstance(value, str):
                raise AcceptedRecordError(f"{column.name} must be a UTC timestamp string")
            _timestamp_literal(value)
        elif DECIMAL_TYPE_PATTERN.fullmatch(sql_type):
            if not isinstance(value, str):
                raise AcceptedRecordError(
                    f"{column.name} must remain a canonical decimal string"
                )
            try:
                Decimal(value)
            except InvalidOperation as exc:
                raise AcceptedRecordError(f"{column.name} is not a decimal") from exc
        else:  # pragma: no cover - guarded by schema type inference
            raise TableContractError(f"unsupported SQL type {sql_type}")
        if column.pattern and isinstance(value, str):
            if re.fullmatch(column.pattern, value) is None:
                raise AcceptedRecordError(f"{column.name} does not match its source pattern")

    def _existing_payload_hashes(
        self,
        plan: TablePlan,
        records: Sequence[_PreparedRecord],
    ) -> dict[str, set[str]]:
        if not records:
            return {}
        identities = sorted({record.identity_sha256 for record in records})
        identity_literals = ", ".join(_string_literal(value) for value in identities)
        result = self.statement_runner.execute(
            "SELECT pipeline_identity_sha256, pipeline_payload_sha256\n"
            f"FROM {plan.table}\n"
            f"WHERE pipeline_identity_sha256 IN ({identity_literals})"
        )
        existing: dict[str, set[str]] = {}
        for row in result.rows:
            if len(row) < 2 or not isinstance(row[0], str) or not isinstance(row[1], str):
                raise TrinoProtocolError("identity check returned malformed hash columns")
            hashes = existing.setdefault(row[0], set())
            if row[1] in hashes:
                raise TableContractError(
                    f"{plan.table} contains duplicate rows for immutable identity {row[0]}"
                )
            hashes.add(row[1])
        return existing

    def _merge_sql(
        self,
        plan: TablePlan,
        records: Sequence[_PreparedRecord],
    ) -> str:
        columns = plan.columns
        quoted_columns = ", ".join(_quote_identifier(column.name) for column in columns)
        rows = ",\n    ".join(
            "(" + ", ".join(self._record_literals(plan, record)) + ")"
            for record in records
        )
        source_values = ", ".join(
            f"source.{_quote_identifier(column.name)}" for column in columns
        )
        identity_column = _quote_identifier("pipeline_identity_sha256")
        return (
            f"MERGE INTO {plan.table} AS target\n"
            f"USING (VALUES\n    {rows}\n) AS source ({quoted_columns})\n"
            f"ON target.{identity_column} = source.{identity_column}\n"
            f"WHEN NOT MATCHED THEN INSERT ({quoted_columns})\n"
            f"VALUES ({source_values})"
        )

    def _record_literals(
        self,
        plan: TablePlan,
        record: _PreparedRecord,
    ) -> tuple[str, ...]:
        source_literals = tuple(
            _sql_literal(value, column.sql_type)
            for column, value in zip(plan.source_columns, record.source_values, strict=True)
        )
        accepted = record.accepted
        metadata_values: tuple[Any, ...] = (
            accepted.pipeline_run_id,
            accepted.evidence_envelope_id,
            accepted.ingested_at_utc,
            accepted.raw_object_uri,
            accepted.raw_object_sha256,
            accepted.raw_record_locator,
            record.identity_sha256,
            record.payload_sha256,
        )
        metadata_literals = tuple(
            _sql_literal(value, column.sql_type)
            for column, value in zip(PIPELINE_COLUMNS, metadata_values, strict=True)
        )
        return source_literals + metadata_literals

    def _verify_table(self, plan: TablePlan) -> None:
        result = self.statement_runner.execute(f"DESCRIBE {plan.table}")
        actual: dict[str, str] = {}
        for row in result.rows:
            if len(row) >= 2:
                actual[str(row[0])] = _normalize_sql_type(str(row[1]))
        missing = [column.name for column in plan.columns if column.name not in actual]
        incompatible = [
            f"{column.name}: expected {column.sql_type}, found {actual[column.name]}"
            for column in plan.columns
            if column.name in actual
            and actual[column.name] != _normalize_sql_type(column.sql_type)
        ]
        if missing or incompatible:
            details: list[str] = []
            if missing:
                details.append(f"missing columns {missing}")
            if incompatible:
                details.append(f"incompatible columns {incompatible}")
            raise TableContractError(
                f"{plan.table} does not match its contract: " + "; ".join(details)
            )

    def _record_conflict(
        self,
        accumulator: _LoadAccumulator,
        dataset: str,
        record: _PreparedRecord,
        *,
        incoming_hashes: Iterable[str],
        existing_hashes: Iterable[str],
        record_count: int,
        reason: str,
    ) -> None:
        accumulator.conflict_records += record_count
        if record.identity_sha256 in accumulator.conflict_identity_hashes:
            return
        accumulator.conflict_identity_hashes.add(record.identity_sha256)
        accumulator.conflict_identities += 1
        if len(accumulator.conflicts) < self.config.conflict_detail_limit:
            accumulator.conflicts.append(
                IdentityConflict(
                    dataset=dataset,
                    identity_sha256=record.identity_sha256,
                    identity=dict(record.identity),
                    incoming_payload_sha256=tuple(sorted(incoming_hashes)),
                    existing_payload_sha256=tuple(sorted(existing_hashes)),
                    reason=reason,
                )
            )
        else:
            accumulator.conflict_details_truncated = True

    def _column_spec(
        self,
        name: str,
        definition: Mapping[str, Any],
        *,
        required: bool,
    ) -> ColumnSpec:
        resolved, explicit_null = self._resolve_definition(definition)
        json_type = resolved.get("type")
        if json_type is None and "const" in resolved:
            json_type = _json_type_name(resolved["const"])
        if json_type is None and resolved.get("enum"):
            json_type = _json_type_name(resolved["enum"][0])
        sql_type = self._sql_type_for_definition(resolved, json_type)
        return ColumnSpec(
            name=name,
            sql_type=sql_type,
            required=required,
            explicit_null_allowed=explicit_null,
            pattern=str(resolved["pattern"]) if resolved.get("pattern") else None,
            allowed_values=tuple(resolved.get("enum", ())),
            constant=resolved.get("const"),
            has_constant="const" in resolved,
        )

    def _resolve_definition(
        self,
        definition: Mapping[str, Any],
    ) -> tuple[Mapping[str, Any], bool]:
        explicit_null = False
        if "oneOf" in definition or "anyOf" in definition:
            choices = definition.get("oneOf", definition.get("anyOf"))
            if not isinstance(choices, list):
                raise TableContractError("schema union must be an array")
            non_null = [choice for choice in choices if choice.get("type") != "null"]
            explicit_null = len(non_null) != len(choices)
            if len(non_null) != 1:
                raise TableContractError("only nullable single-type schema unions are supported")
            definition = non_null[0]
        ref = definition.get("$ref")
        if not ref:
            return definition, explicit_null
        prefix = "common.schema.json#/$defs/"
        if not isinstance(ref, str) or not ref.startswith(prefix):
            raise TableContractError(f"unsupported JSON Schema reference {ref!r}")
        common_path = self.contracts_dir / "common.schema.json"
        try:
            common = json.loads(common_path.read_text(encoding="utf-8"))
            resolved = common["$defs"][ref.removeprefix(prefix)]
        except (FileNotFoundError, json.JSONDecodeError, KeyError, TypeError) as exc:
            raise TableContractError(f"cannot resolve JSON Schema reference {ref!r}") from exc
        return resolved, explicit_null

    @staticmethod
    def _sql_type_for_definition(
        definition: Mapping[str, Any],
        json_type: Any,
    ) -> str:
        description = str(definition.get("description", "")).lower()
        pattern = str(definition.get("pattern", ""))
        if "decimal(20,6)" in description:
            return "DECIMAL(20,6)"
        if "decimal(18,6)" in description:
            return "DECIMAL(18,6)"
        if definition.get("format") == "date-time" or "T(?:" in pattern:
            return "TIMESTAMP(6) WITH TIME ZONE"
        if definition.get("format") == "date":
            return "DATE"
        mapping = {
            "string": "VARCHAR",
            "integer": "BIGINT",
            "number": "DOUBLE",
            "boolean": "BOOLEAN",
        }
        if json_type not in mapping:
            raise TableContractError(f"unsupported JSON Schema type {json_type!r}")
        return mapping[json_type]

    def _qualified_schema(self) -> str:
        return (
            f"{_quote_identifier(self.config.catalog)}."
            f"{_quote_identifier(self.config.iceberg_schema)}"
        )

    def _qualified_table(self, dataset: str) -> str:
        return f"{self._qualified_schema()}.{_quote_identifier(dataset)}"

    @staticmethod
    def _require_dataset(dataset: str) -> None:
        if dataset not in SYNTHETIC_DATASETS:
            raise ValueError(
                f"unsupported dataset {dataset!r}; expected one of {SYNTHETIC_DATASETS}"
            )


def _chunks(values: Iterable[AcceptedRecord], size: int) -> Iterator[list[AcceptedRecord]]:
    chunk: list[AcceptedRecord] = []
    for value in values:
        chunk.append(value)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _decode_json_object(raw: bytes, filename: str, line_number: int) -> Mapping[str, Any]:
    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise AcceptedRecordError(
                    f"{filename}:{line_number} repeats JSON key {key!r}"
                )
            value[key] = item
        return value

    def reject_nonfinite(value: str) -> None:
        raise AcceptedRecordError(
            f"{filename}:{line_number} contains non-finite number {value}"
        )

    try:
        decoded = json.loads(
            raw,
            object_pairs_hook=reject_duplicate_keys,
            parse_constant=reject_nonfinite,
        )
    except UnicodeDecodeError as exc:
        raise AcceptedRecordError(f"{filename}:{line_number} is not UTF-8") from exc
    except json.JSONDecodeError as exc:
        raise AcceptedRecordError(
            f"{filename}:{line_number} is invalid JSON: {exc.msg}"
        ) from exc
    if not isinstance(decoded, dict):
        raise AcceptedRecordError(f"{filename}:{line_number} must be a JSON object")
    return decoded


def _sha256_json(value: Any) -> str:
    try:
        canonical = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise AcceptedRecordError("record is not canonical JSON data") from exc
    return hashlib.sha256(canonical).hexdigest()


def _quote_identifier(value: str) -> str:
    if not SAFE_SQL_IDENTIFIER.fullmatch(value):
        raise ValueError(f"unsafe SQL identifier {value!r}")
    return f'"{value}"'


def _string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _timestamp_literal(value: str) -> str:
    if not isinstance(value, str):
        raise AcceptedRecordError("timestamp value must be a string")
    match = UTC_TIMESTAMP_PATTERN.fullmatch(value)
    if not match:
        raise AcceptedRecordError(f"timestamp must be timezone-aware UTC: {value!r}")
    return f"TIMESTAMP {_string_literal(f'{match.group(1)} {match.group(2)} UTC')}"


def _sql_literal(value: Any, sql_type: str) -> str:
    if value is None:
        return f"CAST(NULL AS {sql_type})"
    if sql_type == "VARCHAR":
        if not isinstance(value, str):
            raise AcceptedRecordError("VARCHAR value is not a string")
        return _string_literal(value)
    if sql_type == "BOOLEAN":
        if not isinstance(value, bool):
            raise AcceptedRecordError("BOOLEAN value is not a boolean")
        return "true" if value else "false"
    if sql_type == "BIGINT":
        if not isinstance(value, int) or isinstance(value, bool):
            raise AcceptedRecordError("BIGINT value is not an integer")
        return str(value)
    if sql_type == "DOUBLE":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise AcceptedRecordError("DOUBLE value is not numeric")
        numeric = float(value)
        if not math.isfinite(numeric):
            raise AcceptedRecordError("DOUBLE value is not finite")
        return f"DOUBLE {_string_literal(repr(numeric))}"
    if sql_type == "DATE":
        if not isinstance(value, str):
            raise AcceptedRecordError("DATE value is not a string")
        return f"DATE {_string_literal(value)}"
    if sql_type == "TIMESTAMP(6) WITH TIME ZONE":
        return _timestamp_literal(value)
    decimal_match = DECIMAL_TYPE_PATTERN.fullmatch(sql_type)
    if decimal_match:
        if not isinstance(value, str):
            raise AcceptedRecordError("DECIMAL value is not a canonical string")
        return f"DECIMAL {_string_literal(value)}"
    raise TableContractError(f"unsupported SQL literal type {sql_type}")


def _normalize_sql_type(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip()).upper().replace(", ", ",")


def _json_type_name(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if value is None:
        return "null"
    raise TableContractError(f"unsupported JSON constant type {type(value).__name__}")


__all__ = [
    "AcceptedRecord",
    "AcceptedRecordError",
    "ColumnSpec",
    "IDENTITY_FIELDS",
    "IcebergLoaderConfig",
    "IdentityConflict",
    "LoadResult",
    "QueryResult",
    "SYNTHETIC_DATASETS",
    "StatementRunner",
    "TableContractError",
    "TablePlan",
    "TrinoHttpClient",
    "TrinoIcebergLoader",
    "TrinoLoaderError",
    "TrinoProtocolError",
    "TrinoQueryError",
]
