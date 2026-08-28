"""Idempotent publication of successful bounded-run coverage to Iceberg.

The control row is deliberately separate from the nine business sources.  It
declares which local operating dates a successfully reconciled run covered so
dbt can build expected half-hour intervals even when business evidence is
missing for one of them.

``pipeline_run_id`` identifies stable coverage content.  Airflow attempt IDs
and reconciliation artifact locations are retained as first-observed lineage,
but are excluded from the canonical payload hash because an exact replay has a
new attempt artifact while declaring the same coverage.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields
from typing import Any

from ingestion.batch.synthetic.generate import GENERATOR_NAME

from .models import (
    PipelineError,
    RunPlan,
    SAFE_SQL_IDENTIFIER,
    parse_iso_date,
    parse_utc_timestamp,
)
from .trino_loader import StatementRunner, TrinoHttpClient


CONTROL_SCHEMA = "industrial_energy_control"
COVERAGE_TABLE = "batch_run_coverage"
OPERATING_TIMEZONE = "Europe/London"
SHA256_PATTERN = re.compile(r"^[a-f0-9]{64}$")


class CoveragePublicationError(PipelineError):
    """Base error for a coverage row that must not be published."""


class CoveragePayloadConflict(CoveragePublicationError):
    """The run identity already exists with different canonical content."""


class CoverageTableContractError(CoveragePublicationError):
    """The Iceberg control table does not match its governed contract."""


@dataclass(frozen=True)
class CoverageColumn:
    name: str
    sql_type: str


COVERAGE_COLUMNS: tuple[CoverageColumn, ...] = (
    CoverageColumn("pipeline_run_id", "VARCHAR"),
    CoverageColumn("start_date_local_inclusive", "DATE"),
    CoverageColumn("end_date_local_inclusive", "DATE"),
    CoverageColumn("operating_timezone", "VARCHAR"),
    CoverageColumn("generator_name", "VARCHAR"),
    CoverageColumn("generator_version", "VARCHAR"),
    CoverageColumn("generator_seed", "BIGINT"),
    CoverageColumn("generated_at_utc", "TIMESTAMP(6) WITH TIME ZONE"),
    CoverageColumn("raw_manifest_uri", "VARCHAR"),
    CoverageColumn("raw_manifest_sha256", "VARCHAR"),
    CoverageColumn("raw_manifest_ingested_at_utc", "TIMESTAMP(6) WITH TIME ZONE"),
    CoverageColumn("reconciliation_status", "VARCHAR"),
    CoverageColumn("raw_record_count", "BIGINT"),
    CoverageColumn("accepted_record_count", "BIGINT"),
    CoverageColumn("quarantined_record_count", "BIGINT"),
    CoverageColumn("duplicate_record_count", "BIGINT"),
    CoverageColumn("iceberg_reconciled_record_count", "BIGINT"),
    CoverageColumn("iceberg_table_count", "BIGINT"),
    CoverageColumn("first_orchestrator_run_id", "VARCHAR"),
    CoverageColumn("reconciliation_artifact_uri", "VARCHAR"),
    CoverageColumn("reconciliation_artifact_sha256", "VARCHAR"),
    CoverageColumn("coverage_published_at_utc", "TIMESTAMP(6) WITH TIME ZONE"),
    CoverageColumn("coverage_payload_sha256", "VARCHAR"),
)

# These columns define an equivalent coverage declaration across Airflow
# retries and later exact replays.  The remaining lineage columns deliberately
# preserve the first successful attempt without changing the stable identity.
CANONICAL_COVERAGE_FIELDS: tuple[str, ...] = tuple(
    column.name for column in COVERAGE_COLUMNS[:18]
)


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    try:
        content = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CoveragePublicationError(
            "coverage payload is not canonical JSON data"
        ) from exc
    return hashlib.sha256(content).hexdigest()


def _required_artifact(
    value: Mapping[str, Any] | Any,
    *,
    name: str,
) -> tuple[str, str, str]:
    if not isinstance(value, Mapping):
        raise CoveragePublicationError(f"{name} must be a mapping")
    uri = value.get("uri")
    sha256 = value.get("sha256")
    observed_at_utc = value.get("last_modified_utc")
    if not isinstance(uri, str) or not uri.strip():
        raise CoveragePublicationError(f"{name} uri must be a non-empty string")
    if not isinstance(sha256, str) or not SHA256_PATTERN.fullmatch(sha256):
        raise CoveragePublicationError(f"{name} sha256 must be a lowercase SHA-256")
    if not isinstance(observed_at_utc, str):
        raise CoveragePublicationError(f"{name} timestamp must be a UTC string")
    parse_utc_timestamp(observed_at_utc, f"{name}.last_modified_utc")
    return uri, sha256, observed_at_utc


def _count(value: Mapping[str, Any], key: str) -> int:
    raw = value.get(key)
    if not isinstance(raw, int) or isinstance(raw, bool) or raw < 0:
        raise CoveragePublicationError(f"{key} must be a non-negative integer")
    return raw


@dataclass(frozen=True)
class BatchRunCoverage:
    """One validated, canonicalizable successful-run coverage declaration."""

    pipeline_run_id: str
    start_date_local_inclusive: str
    end_date_local_inclusive: str
    operating_timezone: str
    generator_name: str
    generator_version: str
    generator_seed: int
    generated_at_utc: str
    raw_manifest_uri: str
    raw_manifest_sha256: str
    raw_manifest_ingested_at_utc: str
    reconciliation_status: str
    raw_record_count: int
    accepted_record_count: int
    quarantined_record_count: int
    duplicate_record_count: int
    iceberg_reconciled_record_count: int
    iceberg_table_count: int
    first_orchestrator_run_id: str
    reconciliation_artifact_uri: str
    reconciliation_artifact_sha256: str
    coverage_published_at_utc: str
    coverage_payload_sha256: str

    @classmethod
    def from_workflow(
        cls,
        plan: RunPlan,
        raw_result: Mapping[str, Any],
        reconciliation_result: Mapping[str, Any],
    ) -> "BatchRunCoverage":
        """Build the row only from a successfully reconciled workflow result."""

        plan.validate()
        for name, value in (
            ("raw result", raw_result),
            ("reconciliation result", reconciliation_result),
        ):
            if value.get("pipeline_run_id") != plan.pipeline_run_id:
                raise CoveragePublicationError(
                    f"{name} belongs to another pipeline run"
                )

        status = reconciliation_result.get("status")
        if status not in {"succeeded", "succeeded_with_quarantine"}:
            raise CoveragePublicationError(
                "coverage can be published only after successful reconciliation"
            )

        raw_manifest_uri, raw_manifest_sha256, raw_manifest_ingested_at = (
            _required_artifact(raw_result.get("raw_manifest"), name="raw manifest")
        )
        reconciliation_uri, reconciliation_sha256, coverage_published_at = (
            _required_artifact(
                reconciliation_result.get("reconciliation_artifact"),
                name="reconciliation artifact",
            )
        )

        raw_count = _count(reconciliation_result, "raw_record_count")
        accepted_count = _count(reconciliation_result, "accepted_record_count")
        quarantined_count = _count(
            reconciliation_result, "quarantined_record_count"
        )
        duplicate_count = _count(reconciliation_result, "duplicate_record_count")
        inserted_count = _count(reconciliation_result, "iceberg_inserted_count")
        reused_count = _count(reconciliation_result, "iceberg_reused_count")
        table_count = _count(reconciliation_result, "iceberg_table_count")
        if raw_count != accepted_count + quarantined_count + duplicate_count:
            raise CoveragePublicationError(
                "reconciliation result does not balance raw and validation counts"
            )
        iceberg_reconciled_count = inserted_count + reused_count
        if iceberg_reconciled_count != accepted_count:
            raise CoveragePublicationError(
                "reconciliation result does not balance accepted and Iceberg counts"
            )

        stable_payload: dict[str, Any] = {
            "pipeline_run_id": plan.pipeline_run_id,
            "start_date_local_inclusive": plan.start_date,
            "end_date_local_inclusive": plan.end_date,
            "operating_timezone": OPERATING_TIMEZONE,
            "generator_name": GENERATOR_NAME,
            "generator_version": plan.generator_version,
            "generator_seed": plan.seed,
            "generated_at_utc": plan.generation_time_utc,
            "raw_manifest_uri": raw_manifest_uri,
            "raw_manifest_sha256": raw_manifest_sha256,
            "raw_manifest_ingested_at_utc": raw_manifest_ingested_at,
            "reconciliation_status": str(status),
            "raw_record_count": raw_count,
            "accepted_record_count": accepted_count,
            "quarantined_record_count": quarantined_count,
            "duplicate_record_count": duplicate_count,
            "iceberg_reconciled_record_count": iceberg_reconciled_count,
            "iceberg_table_count": table_count,
        }
        record = cls(
            **stable_payload,
            first_orchestrator_run_id=plan.orchestrator_run_id,
            reconciliation_artifact_uri=reconciliation_uri,
            reconciliation_artifact_sha256=reconciliation_sha256,
            coverage_published_at_utc=coverage_published_at,
            coverage_payload_sha256=_canonical_sha256(stable_payload),
        )
        record.validate()
        return record

    def canonical_payload(self) -> dict[str, Any]:
        values = asdict(self)
        return {name: values[name] for name in CANONICAL_COVERAGE_FIELDS}

    def validate(self) -> None:
        if tuple(item.name for item in fields(self)) != tuple(
            column.name for column in COVERAGE_COLUMNS
        ):
            raise CoverageTableContractError(
                "coverage dataclass and Iceberg columns are out of sync"
            )
        start = parse_iso_date(
            self.start_date_local_inclusive, "start_date_local_inclusive"
        )
        end = parse_iso_date(self.end_date_local_inclusive, "end_date_local_inclusive")
        if end < start:
            raise CoveragePublicationError(
                "end_date_local_inclusive must be on or after "
                "start_date_local_inclusive"
            )
        if self.operating_timezone != OPERATING_TIMEZONE:
            raise CoveragePublicationError(
                f"operating_timezone must equal {OPERATING_TIMEZONE}"
            )
        for name in (
            "pipeline_run_id",
            "generator_name",
            "generator_version",
            "raw_manifest_uri",
            "first_orchestrator_run_id",
            "reconciliation_artifact_uri",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise CoveragePublicationError(f"{name} must be a non-empty string")
        for name in (
            "raw_manifest_sha256",
            "reconciliation_artifact_sha256",
            "coverage_payload_sha256",
        ):
            if not SHA256_PATTERN.fullmatch(getattr(self, name)):
                raise CoveragePublicationError(
                    f"{name} must be a lowercase SHA-256"
                )
        for name in (
            "generated_at_utc",
            "raw_manifest_ingested_at_utc",
            "coverage_published_at_utc",
        ):
            parse_utc_timestamp(getattr(self, name), name)
        for name in (
            "generator_seed",
            "raw_record_count",
            "accepted_record_count",
            "quarantined_record_count",
            "duplicate_record_count",
            "iceberg_reconciled_record_count",
            "iceberg_table_count",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise CoveragePublicationError(
                    f"{name} must be a non-negative integer"
                )
        if self.reconciliation_status not in {
            "succeeded",
            "succeeded_with_quarantine",
        }:
            raise CoveragePublicationError("reconciliation_status is not successful")
        if (
            self.raw_record_count
            != self.accepted_record_count
            + self.quarantined_record_count
            + self.duplicate_record_count
        ):
            raise CoveragePublicationError(
                "coverage validation counts do not reconcile"
            )
        if self.iceberg_reconciled_record_count != self.accepted_record_count:
            raise CoveragePublicationError("coverage Iceberg count does not reconcile")
        expected_hash = _canonical_sha256(self.canonical_payload())
        if self.coverage_payload_sha256 != expected_hash:
            raise CoveragePublicationError(
                "coverage_payload_sha256 does not match canonical coverage content"
            )

    def values(self) -> tuple[Any, ...]:
        values = asdict(self)
        return tuple(values[column.name] for column in COVERAGE_COLUMNS)


@dataclass(frozen=True)
class CoveragePublisherConfig:
    trino_endpoint: str
    catalog: str = "r2"
    schema: str = CONTROL_SCHEMA
    table: str = COVERAGE_TABLE
    trino_user: str = "airflow"
    timeout_seconds: float = 60.0
    query_timeout_seconds: float = 300.0
    verify_existing_table: bool = True

    def __post_init__(self) -> None:
        for label, value in (
            ("catalog", self.catalog),
            ("schema", self.schema),
            ("table", self.table),
        ):
            if not SAFE_SQL_IDENTIFIER.fullmatch(value):
                raise ValueError(f"{label} is not a safe SQL identifier")
        if not self.trino_endpoint.startswith(("http://", "https://")):
            raise ValueError("trino_endpoint must be HTTP or HTTPS")
        if not self.trino_user.strip():
            raise ValueError("trino_user must be non-empty")
        if self.timeout_seconds <= 0 or self.query_timeout_seconds <= 0:
            raise ValueError("Trino timeouts must be positive")


@dataclass(frozen=True)
class CoveragePublicationResult:
    """Small JSON-serializable coverage publication summary for Airflow XCom."""

    pipeline_run_id: str
    table: str
    coverage_payload_sha256: str
    disposition: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


class BatchRunCoveragePublisher:
    """Create, verify, and idempotently insert one coverage control row."""

    def __init__(
        self,
        config: CoveragePublisherConfig,
        *,
        statement_runner: StatementRunner | None = None,
    ) -> None:
        self.config = config
        self.statement_runner = statement_runner or TrinoHttpClient(
            config.trino_endpoint,
            user=config.trino_user,
            source="industrial-energy-batch-run-coverage",
            timeout_seconds=config.timeout_seconds,
            query_timeout_seconds=config.query_timeout_seconds,
        )

    @property
    def qualified_schema(self) -> str:
        catalog = _quote_identifier(self.config.catalog)
        schema = _quote_identifier(self.config.schema)
        return f"{catalog}.{schema}"

    @property
    def qualified_table(self) -> str:
        return f'{self.qualified_schema}.{_quote_identifier(self.config.table)}'

    @property
    def create_schema_sql(self) -> str:
        return f"CREATE SCHEMA IF NOT EXISTS {self.qualified_schema}"

    @property
    def create_table_sql(self) -> str:
        column_ddl = ",\n  ".join(
            f"{_quote_identifier(column.name)} {column.sql_type}"
            for column in COVERAGE_COLUMNS
        )
        return (
            f"CREATE TABLE IF NOT EXISTS {self.qualified_table} (\n"
            f"  {column_ddl}\n"
            ")\nWITH (\n"
            "  format = 'PARQUET',\n"
            "  format_version = 2\n"
            ")"
        )

    def publish(self, record: BatchRunCoverage) -> CoveragePublicationResult:
        """Insert once, reuse an exact replay, and reject changed stable content."""

        record.validate()
        self.statement_runner.execute(self.create_schema_sql)
        self.statement_runner.execute(self.create_table_sql)
        if self.config.verify_existing_table:
            self._verify_table()

        existing_hash = self._existing_hash(record.pipeline_run_id)
        if existing_hash is not None:
            self._require_same_payload(record, existing_hash)
            return self._result(record, "reused")

        merge_result = self.statement_runner.execute(self._merge_sql(record))
        committed_hash = self._existing_hash(record.pipeline_run_id)
        if committed_hash is None:
            raise CoverageTableContractError(
                "coverage MERGE completed but the pipeline run is absent"
            )
        self._require_same_payload(record, committed_hash)
        if merge_result.update_count not in {None, 0, 1}:
            raise CoverageTableContractError(
                "coverage MERGE returned an invalid update count"
            )
        disposition = "reused" if merge_result.update_count == 0 else "created"
        return self._result(record, disposition)

    def _verify_table(self) -> None:
        result = self.statement_runner.execute(f"DESCRIBE {self.qualified_table}")
        actual: dict[str, str] = {}
        for row in result.rows:
            if len(row) >= 2:
                actual[str(row[0])] = _normalize_sql_type(str(row[1]))
        missing = [
            column.name for column in COVERAGE_COLUMNS if column.name not in actual
        ]
        incompatible = [
            f"{column.name}: expected {column.sql_type}, found {actual[column.name]}"
            for column in COVERAGE_COLUMNS
            if column.name in actual
            and actual[column.name] != _normalize_sql_type(column.sql_type)
        ]
        if missing or incompatible:
            details: list[str] = []
            if missing:
                details.append(f"missing columns {missing}")
            if incompatible:
                details.append(f"incompatible columns {incompatible}")
            raise CoverageTableContractError(
                f"{self.qualified_table} does not match its contract: "
                + "; ".join(details)
            )

    def _existing_hash(self, pipeline_run_id: str) -> str | None:
        result = self.statement_runner.execute(
            f"SELECT coverage_payload_sha256\nFROM {self.qualified_table}\n"
            f"WHERE pipeline_run_id = {_string_literal(pipeline_run_id)}"
        )
        if len(result.rows) > 1:
            raise CoverageTableContractError(
                f"{self.qualified_table} contains duplicate rows for {pipeline_run_id}"
            )
        if not result.rows:
            return None
        row = result.rows[0]
        if (
            len(row) != 1
            or not isinstance(row[0], str)
            or not SHA256_PATTERN.fullmatch(row[0])
        ):
            raise CoverageTableContractError(
                "coverage identity check returned a malformed payload hash"
            )
        return row[0]

    @staticmethod
    def _require_same_payload(record: BatchRunCoverage, existing_hash: str) -> None:
        if existing_hash != record.coverage_payload_sha256:
            raise CoveragePayloadConflict(
                f"pipeline run {record.pipeline_run_id} already declares different "
                "canonical coverage content"
            )

    def _merge_sql(self, record: BatchRunCoverage) -> str:
        quoted_columns = ", ".join(
            _quote_identifier(column.name) for column in COVERAGE_COLUMNS
        )
        source_values = ", ".join(
            f"source.{_quote_identifier(column.name)}" for column in COVERAGE_COLUMNS
        )
        literals = ", ".join(
            _sql_literal(value, column.sql_type)
            for column, value in zip(COVERAGE_COLUMNS, record.values(), strict=True)
        )
        return (
            f"MERGE INTO {self.qualified_table} AS target\n"
            f"USING (VALUES\n    ({literals})\n) AS source ({quoted_columns})\n"
            "ON target.\"pipeline_run_id\" = source.\"pipeline_run_id\"\n"
            f"WHEN NOT MATCHED THEN INSERT ({quoted_columns})\n"
            f"VALUES ({source_values})"
        )

    def _result(
        self, record: BatchRunCoverage, disposition: str
    ) -> CoveragePublicationResult:
        return CoveragePublicationResult(
            pipeline_run_id=record.pipeline_run_id,
            table=self.qualified_table,
            coverage_payload_sha256=record.coverage_payload_sha256,
            disposition=disposition,
        )


def _quote_identifier(value: str) -> str:
    if not SAFE_SQL_IDENTIFIER.fullmatch(value):
        raise ValueError(f"unsafe SQL identifier {value!r}")
    return f'"{value}"'


def _string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _timestamp_literal(value: str) -> str:
    parsed = parse_utc_timestamp(value, "coverage timestamp")
    timestamp = parsed.strftime("%Y-%m-%d %H:%M:%S")
    if parsed.microsecond:
        timestamp += f".{parsed.microsecond:06d}".rstrip("0")
    return f"TIMESTAMP {_string_literal(timestamp + ' UTC')}"


def _sql_literal(value: Any, sql_type: str) -> str:
    if sql_type == "VARCHAR":
        if not isinstance(value, str):
            raise CoveragePublicationError("VARCHAR coverage value must be a string")
        return _string_literal(value)
    if sql_type == "BIGINT":
        if not isinstance(value, int) or isinstance(value, bool):
            raise CoveragePublicationError("BIGINT coverage value must be an integer")
        return str(value)
    if sql_type == "DATE":
        if not isinstance(value, str):
            raise CoveragePublicationError("DATE coverage value must be a string")
        parse_iso_date(value, "coverage date")
        return f"DATE {_string_literal(value)}"
    if sql_type == "TIMESTAMP(6) WITH TIME ZONE":
        if not isinstance(value, str):
            raise CoveragePublicationError("timestamp coverage value must be a string")
        return _timestamp_literal(value)
    raise CoverageTableContractError(f"unsupported coverage SQL type {sql_type}")


def _normalize_sql_type(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip()).upper().replace(", ", ",")


__all__ = [
    "COVERAGE_COLUMNS",
    "COVERAGE_TABLE",
    "CONTROL_SCHEMA",
    "BatchRunCoverage",
    "BatchRunCoveragePublisher",
    "CoveragePayloadConflict",
    "CoveragePublicationError",
    "CoveragePublicationResult",
    "CoveragePublisherConfig",
    "CoverageTableContractError",
]
