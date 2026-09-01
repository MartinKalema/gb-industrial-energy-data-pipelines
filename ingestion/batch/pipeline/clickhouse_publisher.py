"""Publish a certified dbt mart to the frontend's ClickHouse read model.

R2-backed Iceberg remains authoritative.  This module copies only the two
denormalized datasets required by the historical-delivery API into ordinary
ClickHouse ``MergeTree`` tables.  Candidate rows are invisible to readers
because they carry a new ``load_attempt_id`` and become live only when a
matching ``data_publication`` marker is inserted after full validation.

The module deliberately uses the standard-library HTTP clients already used
by the batch pipeline.  Airflow therefore does not need a stateful ClickHouse
driver, and tests can inject both database boundaries.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from .models import PipelineError
from .trino_loader import QueryResult, StatementRunner, TrinoHttpClient

SERVING_SCHEMA_VERSION = "historical-delivery-serving-v1"
CURRENT_TABLE = "delivery_interval_current"
HISTORY_TABLE = "delivery_interval_history"
PUBLICATION_TABLE = "data_publication"
READY_STATUS = "ready"
SHA256_PATTERN = re.compile(r"^[a-f0-9]{64}$")
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
SAFE_ATTEMPT_ID = re.compile(r"^publication-[a-f0-9]{32}$")
CURRENT_SORTING_KEY = (
    "load_attempt_id, reporting_date, interval_start_at, "
    "delivery_point_id, interval_key"
)
HISTORY_SORTING_KEY = "load_attempt_id, interval_key, known_from_at, history_key"
PUBLICATION_SORTING_KEY = "published_at_utc, publication_id"
PUBLICATION_COLUMNS = (
    ("publication_id", "String"),
    ("source_fingerprint_sha256", "FixedString(64)"),
    ("pipeline_run_id", "String"),
    ("coverage_payload_sha256", "FixedString(64)"),
    ("dbt_result_identity_sha256", "FixedString(64)"),
    ("current_row_count", "UInt64"),
    ("history_row_count", "UInt64"),
    ("current_content_sha256", "FixedString(64)"),
    ("history_content_sha256", "FixedString(64)"),
    ("minimum_reporting_date", "Nullable(Date)"),
    ("maximum_reporting_date", "Nullable(Date)"),
    ("published_at_utc", "DateTime64(6, 'UTC')"),
    ("publication_status", "LowCardinality(String)"),
)


class ServingPublicationError(PipelineError):
    """A serving candidate failed before it could be made visible."""


class ClickHouseProtocolError(ServingPublicationError):
    """ClickHouse returned an unusable HTTP response."""


@dataclass(frozen=True, slots=True)
class FieldSpec:
    """One exact source-to-serving column contract."""

    name: str
    clickhouse_type: str
    source_expression: str
    value_kind: str = "string"
    nullable: bool = False
    decimal_precision: int | None = None
    decimal_scale: int | None = None


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    """One denormalized API dataset selected from certified Trino marts."""

    table_name: str
    key_name: str
    fields: tuple[FieldSpec, ...]
    from_sql: str

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(field.name for field in self.fields)

    def source_sql(self, *, catalog: str, schema: str) -> str:
        select_list = ",\n                ".join(
            f"{field.source_expression} AS {field.name}" for field in self.fields
        )
        return f"""
            SELECT
                {select_list}
            {self.from_sql.format(catalog=_trino_identifier(catalog), schema=_trino_identifier(schema))}
            ORDER BY {self.key_name}
        """


def _field(
    name: str,
    clickhouse_type: str,
    source_expression: str,
    *,
    value_kind: str = "string",
    nullable: bool = False,
    decimal_precision: int | None = None,
    decimal_scale: int | None = None,
) -> FieldSpec:
    return FieldSpec(
        name=name,
        clickhouse_type=clickhouse_type,
        source_expression=source_expression,
        value_kind=value_kind,
        nullable=nullable,
        decimal_precision=decimal_precision,
        decimal_scale=decimal_scale,
    )


def _decimal_field(
    name: str,
    source_alias: str,
    *,
    precision: int,
    scale: int,
) -> FieldSpec:
    return _field(
        name,
        f"Nullable(Decimal({precision}, {scale}))",
        f"CAST({source_alias}.{name} AS VARCHAR)",
        value_kind="decimal",
        nullable=True,
        decimal_precision=precision,
        decimal_scale=scale,
    )


def _common_fields(source_alias: str) -> tuple[FieldSpec, ...]:
    """Fields shared by current and history API interval responses."""

    return (
        _field(
            "interval_key",
            "String",
            f"{source_alias}.delivery_interval_key",
        ),
        _field(
            "tenant_authorization_scope_id",
            "String",
            "c.tenant_authorization_scope_id",
        ),
        _field(
            "customer_access_status",
            "LowCardinality(String)",
            f"{source_alias}.customer_access_status",
        ),
        _field(
            "date_key",
            "UInt32",
            f"{source_alias}.date_key",
            value_kind="integer",
        ),
        _field("customer_id", "String", f"{source_alias}.customer_natural_id"),
        _field("customer_name", "String", "c.display_name"),
        _field("site_id", "String", f"{source_alias}.site_natural_id"),
        _field("site_name", "String", "s.site_name"),
        _field(
            "delivery_point_id",
            "String",
            f"{source_alias}.delivery_point_natural_id",
        ),
        _field("delivery_point_name", "String", "dp.delivery_point_name"),
        _field(
            "reporting_date",
            "Date",
            "CAST(i.reporting_date AS VARCHAR)",
            value_kind="date",
        ),
        _field(
            "local_period_number",
            "UInt16",
            "i.local_period_number",
            value_kind="integer",
        ),
        _field(
            "interval_start_at",
            "DateTime64(6, 'UTC')",
            f"to_iso8601({source_alias}.interval_start_utc)",
            value_kind="utc_datetime",
        ),
        _field(
            "interval_end_at",
            "DateTime64(6, 'UTC')",
            f"to_iso8601({source_alias}.interval_end_utc)",
            value_kind="utc_datetime",
        ),
        _field(
            "interval_start_local",
            "DateTime64(6)",
            "to_iso8601(i.interval_start_local)",
            value_kind="local_datetime",
        ),
        _field(
            "interval_end_local",
            "DateTime64(6)",
            "to_iso8601(i.interval_end_local)",
            value_kind="local_datetime",
        ),
        _field("operating_timezone", "String", "i.operating_timezone"),
        _field(
            "utc_offset_minutes",
            "Int16",
            "i.utc_offset_minutes",
            value_kind="integer",
        ),
        _field(
            "is_daylight_saving_time",
            "Bool",
            "i.is_daylight_saving_time",
            value_kind="boolean",
        ),
        *(
            _decimal_field(name, source_alias, precision=20, scale=6)
            for name in (
                "committed_mwh_th",
                "delivered_mwh_th",
                "shortfall_mwh_th",
                "excess_mwh_th",
                "deliverable_capacity_mwh_th",
                "billable_mwh_th",
            )
        ),
        *(
            _decimal_field(name, source_alias, precision=38, scale=12)
            for name in (
                "gross_earned_revenue_gbp",
                "accrued_sla_penalty_gbp",
                "net_earned_revenue_gbp",
            )
        ),
        _field(
            "currency_code",
            "Nullable(String)",
            f"{source_alias}.currency_code",
            nullable=True,
        ),
        *(
            _field(
                name,
                "LowCardinality(String)",
                f"{source_alias}.{name}",
            )
            for name in (
                "delivery_measurement_status",
                "commitment_status",
                "capacity_status",
                "sla_result_status",
                "availability_result_status",
                "financial_result_status",
                "correction_status",
            )
        ),
    )


CURRENT_DATASET = DatasetSpec(
    table_name=CURRENT_TABLE,
    key_name="interval_key",
    fields=(
        *_common_fields("f"),
        _decimal_field("sla_attainment_numerator_mwh_th", "f", precision=20, scale=6),
        _decimal_field(
            "contractual_availability_numerator_mwh_th",
            "f",
            precision=20,
            scale=6,
        ),
        *(
            _field(name, "UInt8", f"f.{name}", value_kind="integer")
            for name in ("expected_interval_count", "commitment_record_count")
        ),
        *(
            _field(
                name,
                "Nullable(UInt8)",
                f"f.{name}",
                value_kind="integer",
                nullable=True,
            )
            for name in (
                "applicable_interval_count",
                "accepted_applicable_delivery_count",
                "final_applicable_capacity_count",
            )
        ),
        _field(
            "latest_coverage_published_at_utc",
            "Nullable(DateTime64(6, 'UTC'))",
            "to_iso8601(f.latest_coverage_published_at_utc)",
            value_kind="utc_datetime",
            nullable=True,
        ),
    ),
    from_sql="""
            FROM {catalog}.{schema}.fct_steam_delivery_interval AS f
            JOIN {catalog}.{schema}.dim_customer AS c
              ON f.customer_key = c.customer_key
            JOIN {catalog}.{schema}.dim_site AS s
              ON f.site_key = s.site_key
            JOIN {catalog}.{schema}.dim_delivery_point AS dp
              ON f.delivery_point_key = dp.delivery_point_key
            JOIN {catalog}.{schema}.dim_interval AS i
              ON f.interval_key = i.interval_key
    """,
)


HISTORY_DATASET = DatasetSpec(
    table_name=HISTORY_TABLE,
    key_name="history_key",
    fields=(
        _field(
            "history_key",
            "String",
            "h.delivery_interval_history_key",
        ),
        *_common_fields("h"),
        _field(
            "known_from_at",
            "DateTime64(6, 'UTC')",
            "to_iso8601(h.known_from_utc)",
            value_kind="utc_datetime",
        ),
        _field(
            "known_to_at",
            "Nullable(DateTime64(6, 'UTC'))",
            "to_iso8601(h.known_to_utc)",
            value_kind="utc_datetime",
            nullable=True,
        ),
        _field(
            "is_current_knowledge_state",
            "Bool",
            "h.known_to_utc IS NULL",
            value_kind="boolean",
        ),
    ),
    from_sql="""
            FROM {catalog}.{schema}.fct_steam_delivery_interval_history AS h
            JOIN {catalog}.{schema}.dim_customer_revision_audit AS c
              ON h.customer_revision_key = c.customer_revision_key
            JOIN {catalog}.{schema}.dim_site_revision_audit AS s
              ON h.site_revision_key = s.site_revision_key
            JOIN {catalog}.{schema}.dim_delivery_point_revision_audit AS dp
              ON h.delivery_point_revision_key = dp.delivery_point_revision_key
            JOIN {catalog}.{schema}.dim_interval AS i
              ON h.interval_key = i.interval_key
    """,
)

SERVING_DATASETS = (CURRENT_DATASET, HISTORY_DATASET)


@dataclass(frozen=True, slots=True)
class PublisherConfig:
    """Validated non-secret connection and serving-schema settings."""

    trino_endpoint: str
    trino_catalog: str = "r2"
    trino_schema: str = "industrial_energy_marts"
    trino_user: str = "airflow"
    trino_timeout_seconds: float = 60.0
    trino_query_timeout_seconds: float = 300.0
    clickhouse_host: str = "clickhouse"
    clickhouse_port: int = 8123
    clickhouse_database: str = "industrial_energy_serving"
    clickhouse_user: str = "industrial_energy_publisher"
    clickhouse_password: str = ""
    clickhouse_secure: bool = False
    clickhouse_timeout_seconds: float = 60.0
    clickhouse_query_timeout_seconds: float = 300.0
    insert_batch_size: int = 1_000

    def validate(self) -> None:
        endpoint = urllib.parse.urlparse(self.trino_endpoint)
        if endpoint.scheme not in {"http", "https"} or not endpoint.netloc:
            raise ServingPublicationError(
                "Trino endpoint must be an absolute HTTP(S) URL"
            )
        for name, value in (
            ("trino_catalog", self.trino_catalog),
            ("trino_schema", self.trino_schema),
            ("clickhouse_database", self.clickhouse_database),
        ):
            if not SAFE_IDENTIFIER.fullmatch(value):
                raise ServingPublicationError(f"{name} is not a safe SQL identifier")
        if not self.trino_user.strip():
            raise ServingPublicationError("Trino user must be non-empty")
        if not self.clickhouse_host.strip() or "/" in self.clickhouse_host:
            raise ServingPublicationError("ClickHouse host must be a host name")
        if not 1 <= self.clickhouse_port <= 65_535:
            raise ServingPublicationError("ClickHouse port must be between 1 and 65535")
        if not self.clickhouse_user.strip():
            raise ServingPublicationError("ClickHouse publisher user must be non-empty")
        for name, value in (
            ("trino_timeout_seconds", self.trino_timeout_seconds),
            ("trino_query_timeout_seconds", self.trino_query_timeout_seconds),
            ("clickhouse_timeout_seconds", self.clickhouse_timeout_seconds),
            (
                "clickhouse_query_timeout_seconds",
                self.clickhouse_query_timeout_seconds,
            ),
        ):
            if value <= 0:
                raise ServingPublicationError(f"{name} must be positive")
        if not 1 <= self.insert_batch_size <= 10_000:
            raise ServingPublicationError(
                "ClickHouse insert batch size must be between 1 and 10000"
            )


def publisher_config_from_environment(
    environment: Mapping[str, str] | None = None,
    *,
    trino_endpoint: str | None = None,
    default_trino_catalog: str = "r2",
) -> PublisherConfig:
    """Build and validate the shared publisher/retention connection config."""

    if environment is None:
        environment = os.environ

    def positive_number(name: str, default: str) -> float:
        try:
            value = float(environment.get(name, default))
        except (TypeError, ValueError) as exc:
            raise ServingPublicationError(f"{name} must be a positive number") from exc
        if value <= 0:
            raise ServingPublicationError(f"{name} must be a positive number")
        return value

    def positive_integer(name: str, default: str) -> int:
        try:
            value = int(environment.get(name, default))
        except (TypeError, ValueError) as exc:
            raise ServingPublicationError(f"{name} must be a positive integer") from exc
        if value <= 0:
            raise ServingPublicationError(f"{name} must be a positive integer")
        return value

    def required(name: str) -> str:
        value = environment.get(name, "").strip()
        if not value:
            raise ServingPublicationError(
                f"required environment variable {name} is missing"
            )
        return value

    secure_value = environment.get("CLICKHOUSE_SECURE", "false").strip().lower()
    if secure_value not in {"true", "false"}:
        raise ServingPublicationError("CLICKHOUSE_SECURE must be true or false")

    resolved_trino_endpoint = trino_endpoint or environment.get(
        "TRINO_URL", "http://trino:8080"
    )
    config = PublisherConfig(
        trino_endpoint=resolved_trino_endpoint,
        trino_catalog=environment.get(
            "CLICKHOUSE_SOURCE_TRINO_CATALOG",
            environment.get("DBT_TRINO_CATALOG", default_trino_catalog),
        ),
        trino_schema=environment.get(
            "CLICKHOUSE_SOURCE_TRINO_SCHEMA", "industrial_energy_marts"
        ),
        trino_user=environment.get("TRINO_USER", "airflow"),
        trino_timeout_seconds=positive_number("TRINO_HTTP_TIMEOUT_SECONDS", "60"),
        trino_query_timeout_seconds=positive_number(
            "TRINO_QUERY_TIMEOUT_SECONDS", "300"
        ),
        clickhouse_host=environment.get("CLICKHOUSE_HOST", "clickhouse"),
        clickhouse_port=positive_integer("CLICKHOUSE_PORT", "8123"),
        clickhouse_database=environment.get(
            "CLICKHOUSE_DATABASE", "industrial_energy_serving"
        ),
        clickhouse_user=required("CLICKHOUSE_PUBLISHER_USER"),
        clickhouse_password=required("CLICKHOUSE_PUBLISHER_PASSWORD"),
        clickhouse_secure=secure_value == "true",
        clickhouse_timeout_seconds=positive_number(
            "CLICKHOUSE_HTTP_TIMEOUT_SECONDS", "60"
        ),
        clickhouse_query_timeout_seconds=positive_number(
            "CLICKHOUSE_QUERY_TIMEOUT_SECONDS", "300"
        ),
        insert_batch_size=positive_integer("CLICKHOUSE_INSERT_BATCH_SIZE", "1000"),
    )
    config.validate()
    return config


@dataclass(frozen=True, slots=True)
class PublicationRecord:
    """The final marker that makes a validated candidate visible."""

    publication_id: str
    source_fingerprint_sha256: str
    pipeline_run_id: str
    coverage_payload_sha256: str
    dbt_result_identity_sha256: str
    current_row_count: int
    history_row_count: int
    current_content_sha256: str
    history_content_sha256: str
    minimum_reporting_date: str | None
    maximum_reporting_date: str | None
    published_at_utc: str
    publication_status: str = READY_STATUS

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> PublicationRecord:
        return cls(
            publication_id=str(value["publication_id"]),
            source_fingerprint_sha256=str(value["source_fingerprint_sha256"]),
            pipeline_run_id=str(value["pipeline_run_id"]),
            coverage_payload_sha256=str(value["coverage_payload_sha256"]),
            dbt_result_identity_sha256=str(value["dbt_result_identity_sha256"]),
            current_row_count=int(value["current_row_count"]),
            history_row_count=int(value["history_row_count"]),
            current_content_sha256=str(value["current_content_sha256"]),
            history_content_sha256=str(value["history_content_sha256"]),
            minimum_reporting_date=(
                None
                if value.get("minimum_reporting_date") is None
                else str(value["minimum_reporting_date"])
            ),
            maximum_reporting_date=(
                None
                if value.get("maximum_reporting_date") is None
                else str(value["maximum_reporting_date"])
            ),
            published_at_utc=_normalize_utc_datetime(
                value["published_at_utc"], "published_at_utc"
            ),
            publication_status=str(value["publication_status"]),
        )

    def xcom_summary(self, *, disposition: str) -> dict[str, Any]:
        return {
            "pipeline_run_id": self.pipeline_run_id,
            "publication_id": self.publication_id,
            "disposition": disposition,
            "source_fingerprint_sha256": self.source_fingerprint_sha256,
            "coverage_payload_sha256": self.coverage_payload_sha256,
            "dbt_result_identity_sha256": self.dbt_result_identity_sha256,
            "current_row_count": self.current_row_count,
            "history_row_count": self.history_row_count,
            "current_content_sha256": self.current_content_sha256,
            "history_content_sha256": self.history_content_sha256,
            "minimum_reporting_date": self.minimum_reporting_date,
            "maximum_reporting_date": self.maximum_reporting_date,
            "published_at_utc": self.published_at_utc,
        }


class MartReader(Protocol):
    """Read one exact projection from certified Trino marts."""

    def read_dataset(self, dataset: DatasetSpec) -> list[dict[str, Any]]: ...


class ServingStore(Protocol):
    """Write and read marker-gated ClickHouse candidates."""

    def ensure_schema(self) -> None: ...

    def find_ready_publication(
        self, source_fingerprint_sha256: str
    ) -> PublicationRecord | None: ...

    def insert_candidate_rows(
        self,
        dataset: DatasetSpec,
        load_attempt_id: str,
        rows: Sequence[Mapping[str, Any]],
    ) -> None: ...

    def read_candidate_rows(
        self, dataset: DatasetSpec, load_attempt_id: str
    ) -> list[dict[str, Any]]: ...

    def insert_publication(self, publication: PublicationRecord) -> None: ...


class TrinoMartReader:
    """Read certified mart projections through Trino's finite HTTP protocol."""

    def __init__(
        self,
        config: PublisherConfig,
        *,
        statement_runner: StatementRunner | None = None,
    ) -> None:
        self._config = config
        self._runner = statement_runner or TrinoHttpClient(
            config.trino_endpoint,
            user=config.trino_user,
            source="frontend-serving-publisher",
            timeout_seconds=config.trino_timeout_seconds,
            query_timeout_seconds=config.trino_query_timeout_seconds,
        )

    def read_dataset(self, dataset: DatasetSpec) -> list[dict[str, Any]]:
        result: QueryResult = self._runner.execute(
            dataset.source_sql(
                catalog=self._config.trino_catalog,
                schema=self._config.trino_schema,
            )
        )
        if result.columns != dataset.column_names:
            raise ServingPublicationError(
                f"certified {dataset.table_name} projection returned unexpected columns"
            )
        return [dict(zip(result.columns, row, strict=True)) for row in result.rows]


class ClickHouseHttpClient:
    """Small synchronous ClickHouse HTTP client with bounded requests."""

    def __init__(self, config: PublisherConfig) -> None:
        self._config = config
        scheme = "https" if config.clickhouse_secure else "http"
        self._endpoint = (
            f"{scheme}://{config.clickhouse_host}:{config.clickhouse_port}/"
        )
        credentials = f"{config.clickhouse_user}:{config.clickhouse_password}".encode()
        self._authorization = "Basic " + base64.b64encode(credentials).decode("ascii")

    def _request(
        self,
        *,
        query: str,
        data: bytes | None = None,
        database: str | None = None,
        settings: Mapping[str, str] | None = None,
    ) -> bytes:
        parameters = {
            "wait_end_of_query": "1",
            "max_execution_time": f"{self._config.clickhouse_query_timeout_seconds:g}",
            **({"database": database} if database else {}),
            **dict(settings or {}),
        }
        if data is None:
            body = query.encode("utf-8")
        else:
            parameters["query"] = query
            body = data
        request = urllib.request.Request(
            self._endpoint + "?" + urllib.parse.urlencode(parameters),
            data=body,
            headers={
                "Authorization": self._authorization,
                "Content-Type": "application/octet-stream",
                "User-Agent": "steam-delivery-serving-publisher",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=max(
                    self._config.clickhouse_timeout_seconds,
                    self._config.clickhouse_query_timeout_seconds + 5,
                ),
            ) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:500]
            raise ClickHouseProtocolError(
                f"ClickHouse rejected a serving publication request: {detail}"
            ) from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise ClickHouseProtocolError(
                "ClickHouse serving publication request failed or timed out"
            ) from exc

    def execute(self, sql: str, *, database: str | None = None) -> None:
        self._request(query=sql, database=database)

    def query_json_rows(self, sql: str, *, database: str) -> list[dict[str, Any]]:
        payload = self._request(
            query=sql.rstrip().rstrip(";") + " FORMAT JSONEachRow",
            database=database,
            settings={
                "date_time_output_format": "iso",
                "output_format_json_quote_64bit_integers": "1",
                "output_format_json_quote_decimals": "1",
            },
        )
        rows: list[dict[str, Any]] = []
        for line in payload.decode("utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ClickHouseProtocolError(
                    "ClickHouse returned malformed JSONEachRow output"
                ) from exc
            if not isinstance(row, dict):
                raise ClickHouseProtocolError(
                    "ClickHouse returned a non-object JSONEachRow row"
                )
            rows.append(row)
        return rows

    def insert_json_rows(
        self,
        table_name: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        database: str,
    ) -> None:
        if not rows:
            return
        payload = b"".join(
            (
                json.dumps(
                    dict(row),
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                + "\n"
            ).encode("utf-8")
            for row in rows
        )
        self._request(
            query=f"INSERT INTO {_clickhouse_identifier(table_name)} FORMAT JSONEachRow",
            data=payload,
            database=database,
            settings={
                "date_time_input_format": "best_effort",
                "input_format_json_read_numbers_as_strings": "1",
            },
        )


class ClickHouseServingStore:
    """Marker-gated serving tables backed by ordinary immutable MergeTree rows."""

    def __init__(
        self,
        config: PublisherConfig,
        *,
        client: ClickHouseHttpClient | None = None,
    ) -> None:
        self._config = config
        self._client = client or ClickHouseHttpClient(config)

    @property
    def current_table_sql(self) -> str:
        return self._dataset_table_sql(CURRENT_DATASET)

    @property
    def history_table_sql(self) -> str:
        return self._dataset_table_sql(HISTORY_DATASET)

    @property
    def publication_table_sql(self) -> str:
        columns = ",\n                ".join(
            f"{name} {column_type}" for name, column_type in PUBLICATION_COLUMNS
        )
        return f"""
            CREATE TABLE IF NOT EXISTS {_qualified_clickhouse_table(self._config.clickhouse_database, PUBLICATION_TABLE)} (
                {columns}
            ) ENGINE = MergeTree
            ORDER BY ({PUBLICATION_SORTING_KEY})
        """

    def _dataset_table_sql(self, dataset: DatasetSpec) -> str:
        column_lines = ["load_attempt_id String"]
        column_lines.extend(
            f"{_clickhouse_identifier(field.name)} {field.clickhouse_type}"
            for field in dataset.fields
        )
        columns = ",\n                ".join(column_lines)
        if dataset is CURRENT_DATASET:
            order_by = CURRENT_SORTING_KEY
        else:
            order_by = HISTORY_SORTING_KEY
        return f"""
            CREATE TABLE IF NOT EXISTS {_qualified_clickhouse_table(self._config.clickhouse_database, dataset.table_name)} (
                {columns}
            ) ENGINE = MergeTree
            ORDER BY ({order_by})
        """

    def ensure_schema(self) -> None:
        self._client.execute(self.current_table_sql)
        self._client.execute(self.history_table_sql)
        self._client.execute(self.publication_table_sql)
        self._verify_table_contract(
            CURRENT_TABLE,
            (("load_attempt_id", "String"),)
            + tuple(
                (field.name, field.clickhouse_type) for field in CURRENT_DATASET.fields
            ),
            CURRENT_SORTING_KEY,
        )
        self._verify_table_contract(
            HISTORY_TABLE,
            (("load_attempt_id", "String"),)
            + tuple(
                (field.name, field.clickhouse_type) for field in HISTORY_DATASET.fields
            ),
            HISTORY_SORTING_KEY,
        )
        self._verify_table_contract(
            PUBLICATION_TABLE,
            PUBLICATION_COLUMNS,
            PUBLICATION_SORTING_KEY,
        )

    def _verify_table_contract(
        self,
        table_name: str,
        expected_columns: Sequence[tuple[str, str]],
        expected_sorting_key: str,
    ) -> None:
        qualified_table = _qualified_clickhouse_table(
            self._config.clickhouse_database, table_name
        )
        described = self._client.query_json_rows(
            f"DESCRIBE TABLE {qualified_table}",
            database=self._config.clickhouse_database,
        )
        actual_columns = tuple(
            (str(row.get("name", "")), str(row.get("type", ""))) for row in described
        )
        metadata = self._client.query_json_rows(
            f"""
                SELECT engine, sorting_key
                FROM system.tables
                WHERE database = '{self._config.clickhouse_database}'
                  AND name = '{table_name}'
            """,
            database=self._config.clickhouse_database,
        )
        metadata_matches = (
            len(metadata) == 1
            and metadata[0].get("engine") == "MergeTree"
            and metadata[0].get("sorting_key") == expected_sorting_key
        )
        if actual_columns != tuple(expected_columns) or not metadata_matches:
            raise ServingPublicationError(
                f"ClickHouse serving table {self._config.clickhouse_database}.{table_name} "
                f"does not match {SERVING_SCHEMA_VERSION}; apply a controlled schema "
                "migration or rebuild the disposable serving database before retrying "
                "publication"
            )

    def find_ready_publication(
        self, source_fingerprint_sha256: str
    ) -> PublicationRecord | None:
        _require_sha256(source_fingerprint_sha256, "source fingerprint")
        rows = self._client.query_json_rows(
            f"""
                SELECT
                    publication_id,
                    source_fingerprint_sha256,
                    pipeline_run_id,
                    coverage_payload_sha256,
                    dbt_result_identity_sha256,
                    current_row_count,
                    history_row_count,
                    current_content_sha256,
                    history_content_sha256,
                    minimum_reporting_date,
                    maximum_reporting_date,
                    published_at_utc,
                    publication_status
                FROM {_qualified_clickhouse_table(self._config.clickhouse_database, PUBLICATION_TABLE)}
                WHERE source_fingerprint_sha256 = '{source_fingerprint_sha256}'
                  AND publication_status = '{READY_STATUS}'
                ORDER BY published_at_utc DESC, publication_id DESC
                LIMIT 1
            """,
            database=self._config.clickhouse_database,
        )
        return None if not rows else PublicationRecord.from_mapping(rows[0])

    def insert_candidate_rows(
        self,
        dataset: DatasetSpec,
        load_attempt_id: str,
        rows: Sequence[Mapping[str, Any]],
    ) -> None:
        _require_attempt_id(load_attempt_id)
        for offset in range(0, len(rows), self._config.insert_batch_size):
            batch = [
                {"load_attempt_id": load_attempt_id, **dict(row)}
                for row in rows[offset : offset + self._config.insert_batch_size]
            ]
            self._client.insert_json_rows(
                dataset.table_name,
                batch,
                database=self._config.clickhouse_database,
            )

    def read_candidate_rows(
        self, dataset: DatasetSpec, load_attempt_id: str
    ) -> list[dict[str, Any]]:
        _require_attempt_id(load_attempt_id)
        columns = ", ".join(
            (
                f"toString({_clickhouse_identifier(field.name)}) "
                f"AS {_clickhouse_identifier(field.name)}"
                if field.value_kind
                in {"date", "decimal", "local_datetime", "utc_datetime"}
                else _clickhouse_identifier(field.name)
            )
            for field in dataset.fields
        )
        return self._client.query_json_rows(
            f"""
                SELECT {columns}
                FROM {_qualified_clickhouse_table(self._config.clickhouse_database, dataset.table_name)}
                WHERE load_attempt_id = '{load_attempt_id}'
                ORDER BY {_clickhouse_identifier(dataset.key_name)}
            """,
            database=self._config.clickhouse_database,
        )

    def insert_publication(self, publication: PublicationRecord) -> None:
        if publication.publication_status != READY_STATUS:
            raise ServingPublicationError(
                "only ready publication markers may be inserted"
            )
        self._client.insert_json_rows(
            PUBLICATION_TABLE,
            [asdict(publication)],
            database=self._config.clickhouse_database,
        )


class ServingPublisher:
    """Load, validate, and finally mark one immutable serving publication."""

    def __init__(
        self,
        config: PublisherConfig,
        *,
        mart_reader: MartReader | None = None,
        serving_store: ServingStore | None = None,
        clock: Callable[[], datetime] | None = None,
        attempt_id_factory: Callable[[], str] | None = None,
    ) -> None:
        config.validate()
        self._mart_reader = mart_reader or TrinoMartReader(config)
        self._serving_store = serving_store or ClickHouseServingStore(config)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._attempt_id_factory = attempt_id_factory or (
            lambda: f"publication-{uuid.uuid4().hex}"
        )

    def publish(
        self,
        *,
        pipeline_run_id: str,
        coverage_payload_sha256: str,
        dbt_result_identity_sha256: str,
    ) -> dict[str, Any]:
        if not pipeline_run_id.strip():
            raise ServingPublicationError("pipeline_run_id must be non-empty")
        _require_sha256(coverage_payload_sha256, "coverage payload")
        _require_sha256(dbt_result_identity_sha256, "dbt result identity")

        self._serving_store.ensure_schema()
        source_rows = {
            dataset.table_name: _normalize_dataset_rows(
                dataset,
                self._mart_reader.read_dataset(dataset),
                boundary="certified Trino source",
            )
            for dataset in SERVING_DATASETS
        }
        source_profiles = {
            dataset.table_name: _profile_dataset(
                dataset, source_rows[dataset.table_name]
            )
            for dataset in SERVING_DATASETS
        }
        source_fingerprint = _source_fingerprint(
            coverage_payload_sha256=coverage_payload_sha256,
            dbt_result_identity_sha256=dbt_result_identity_sha256,
            profiles=source_profiles,
        )

        existing = self._serving_store.find_ready_publication(source_fingerprint)
        if existing is not None:
            _validate_existing_publication(
                existing,
                pipeline_run_id=pipeline_run_id,
                coverage_payload_sha256=coverage_payload_sha256,
                dbt_result_identity_sha256=dbt_result_identity_sha256,
                profiles=source_profiles,
            )
            if self._stored_publication_matches(existing, source_profiles):
                return existing.xcom_summary(disposition="reused")

        load_attempt_id = self._attempt_id_factory()
        _require_attempt_id(load_attempt_id)
        for dataset in SERVING_DATASETS:
            self._serving_store.insert_candidate_rows(
                dataset,
                load_attempt_id,
                source_rows[dataset.table_name],
            )

        for dataset in SERVING_DATASETS:
            destination_rows = _normalize_dataset_rows(
                dataset,
                self._serving_store.read_candidate_rows(dataset, load_attempt_id),
                boundary="ClickHouse candidate",
            )
            destination_profile = _profile_dataset(dataset, destination_rows)
            if destination_profile != source_profiles[dataset.table_name]:
                raise ServingPublicationError(
                    f"{dataset.table_name} candidate failed exact count, key, tenant, "
                    "date-coverage, or deterministic-content validation"
                )

        current_profile = source_profiles[CURRENT_TABLE]
        history_profile = source_profiles[HISTORY_TABLE]
        minimum_date, maximum_date = _combined_date_coverage(
            current_profile, history_profile
        )
        publication = PublicationRecord(
            publication_id=load_attempt_id,
            source_fingerprint_sha256=source_fingerprint,
            pipeline_run_id=pipeline_run_id,
            coverage_payload_sha256=coverage_payload_sha256,
            dbt_result_identity_sha256=dbt_result_identity_sha256,
            current_row_count=int(current_profile["row_count"]),
            history_row_count=int(history_profile["row_count"]),
            current_content_sha256=str(current_profile["content_sha256"]),
            history_content_sha256=str(history_profile["content_sha256"]),
            minimum_reporting_date=minimum_date,
            maximum_reporting_date=maximum_date,
            published_at_utc=_normalize_utc_datetime(
                self._clock(), "publication clock"
            ),
        )
        # This is deliberately the final write.  Without this marker, partial
        # candidate rows cannot be selected by the frontend repository.
        self._serving_store.insert_publication(publication)
        return publication.xcom_summary(disposition="created")

    def _stored_publication_matches(
        self,
        publication: PublicationRecord,
        source_profiles: Mapping[str, Mapping[str, Any]],
    ) -> bool:
        marker_profiles = {
            CURRENT_TABLE: (
                publication.current_row_count,
                publication.current_content_sha256,
            ),
            HISTORY_TABLE: (
                publication.history_row_count,
                publication.history_content_sha256,
            ),
        }
        for dataset in SERVING_DATASETS:
            # Keep transport/protocol failures distinct from confirmed stored-row
            # damage: an unavailable database must fail rather than trigger writes.
            stored_rows = self._serving_store.read_candidate_rows(
                dataset, publication.publication_id
            )
            try:
                normalized_rows = _normalize_dataset_rows(
                    dataset,
                    stored_rows,
                    boundary="ClickHouse candidate",
                )
                stored_profile = _profile_dataset(dataset, normalized_rows)
            except ServingPublicationError:
                return False
            marker_count, marker_hash = marker_profiles[dataset.table_name]
            if (
                stored_profile != source_profiles[dataset.table_name]
                or stored_profile["row_count"] != marker_count
                or stored_profile["content_sha256"] != marker_hash
            ):
                return False
        return True


def dbt_result_identity(dbt_result: Mapping[str, Any]) -> str:
    """Hash the compact successful dbt test identity without local file paths."""

    identity = {
        "attempt_number": dbt_result.get("attempt_number"),
        "dbt_command_name": dbt_result.get("dbt_command_name"),
        "dbt_invocation_id": dbt_result.get("dbt_invocation_id"),
        "dbt_step_name": dbt_result.get("dbt_step_name"),
        "dbt_version": dbt_result.get("dbt_version"),
        "generated_at_utc": dbt_result.get("generated_at_utc"),
        "model_result_count": dbt_result.get("model_result_count"),
        "pipeline_run_id": dbt_result.get("pipeline_run_id"),
        "result_count": dbt_result.get("result_count"),
        "status": dbt_result.get("status"),
        "status_counts": dbt_result.get("status_counts"),
        "test_result_count": dbt_result.get("test_result_count"),
    }
    return _sha256_json(identity)


def _normalize_dataset_rows(
    dataset: DatasetSpec,
    rows: Sequence[Mapping[str, Any]],
    *,
    boundary: str,
) -> list[dict[str, Any]]:
    expected_names = set(dataset.column_names)
    normalized: list[dict[str, Any]] = []
    for row_number, row in enumerate(rows, start=1):
        if not isinstance(row, Mapping) or set(row) != expected_names:
            raise ServingPublicationError(
                f"{boundary} {dataset.table_name} row {row_number} has an "
                "unexpected column contract"
            )
        normalized.append(
            {
                field.name: _normalize_value(
                    field,
                    row[field.name],
                    row_number=row_number,
                    boundary=boundary,
                )
                for field in dataset.fields
            }
        )
    return normalized


def _normalize_value(
    field: FieldSpec,
    value: Any,
    *,
    row_number: int,
    boundary: str,
) -> Any:
    label = f"{boundary} {field.name} at row {row_number}"
    if value is None:
        if field.nullable:
            return None
        raise ServingPublicationError(f"{label} must not be null")
    try:
        if field.value_kind == "string":
            return str(value)
        if field.value_kind == "integer":
            if isinstance(value, bool):
                raise ValueError
            return int(value)
        if field.value_kind == "boolean":
            if isinstance(value, bool):
                return value
            if value in (0, "0", "false", "False"):
                return False
            if value in (1, "1", "true", "True"):
                return True
            raise ValueError
        if field.value_kind == "date":
            parsed = (
                value if isinstance(value, date) else date.fromisoformat(str(value))
            )
            if isinstance(parsed, datetime):
                raise ValueError
            return parsed.isoformat()
        if field.value_kind == "utc_datetime":
            return _normalize_utc_datetime(
                value,
                label,
                allow_naive=boundary == "ClickHouse candidate",
            )
        if field.value_kind == "local_datetime":
            return _normalize_local_datetime(value, label)
        if field.value_kind == "decimal":
            return _normalize_decimal(value, field, label)
    except (TypeError, ValueError, InvalidOperation, OverflowError) as exc:
        raise ServingPublicationError(f"{label} has an invalid value") from exc
    raise ServingPublicationError(f"unsupported serving value kind {field.value_kind}")


def _normalize_decimal(value: Any, field: FieldSpec, label: str) -> str:
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    if not decimal_value.is_finite():
        raise ServingPublicationError(f"{label} must be finite")
    assert field.decimal_precision is not None
    assert field.decimal_scale is not None
    if decimal_value.as_tuple().exponent < -field.decimal_scale:
        raise ServingPublicationError(f"{label} exceeds its governed decimal scale")
    rendered = format(decimal_value, f".{field.decimal_scale}f")
    integer_digits = len(rendered.lstrip("-").split(".", 1)[0].lstrip("0"))
    if integer_digits > field.decimal_precision - field.decimal_scale:
        raise ServingPublicationError(f"{label} exceeds its governed decimal precision")
    return rendered


def _parse_datetime(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value).strip().replace(" ", "T")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError as exc:
        raise ServingPublicationError(f"{label} is not an ISO timestamp") from exc


def _normalize_utc_datetime(
    value: Any, label: str, *, allow_naive: bool = False
) -> str:
    parsed = _parse_datetime(value, label)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        if not allow_naive:
            raise ServingPublicationError(f"{label} must include a UTC offset")
        parsed = parsed.replace(tzinfo=UTC)
    utc_value = parsed.astimezone(UTC)
    return utc_value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _normalize_local_datetime(value: Any, label: str) -> str:
    parsed = _parse_datetime(value, label)
    if parsed.tzinfo is not None and parsed.utcoffset() is not None:
        raise ServingPublicationError(f"{label} must be a local wall-clock timestamp")
    return parsed.strftime("%Y-%m-%dT%H:%M:%S.%f")


def _profile_dataset(
    dataset: DatasetSpec, rows: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    keys = [str(row[dataset.key_name]) for row in rows]
    if any(not key for key in keys):
        raise ServingPublicationError(f"{dataset.table_name} contains an empty key")
    if len(keys) != len(set(keys)):
        raise ServingPublicationError(
            f"{dataset.table_name} contains duplicate {dataset.key_name} values"
        )
    if any(not str(row["tenant_authorization_scope_id"]).strip() for row in rows):
        raise ServingPublicationError(
            f"{dataset.table_name} contains a missing tenant authorization scope"
        )
    ordered = sorted(rows, key=lambda row: str(row[dataset.key_name]))
    reporting_dates = [str(row["reporting_date"]) for row in ordered]
    content = b"".join(
        (
            json.dumps(
                {name: row[name] for name in dataset.column_names},
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
        for row in ordered
    )
    return {
        "row_count": len(rows),
        "unique_key_count": len(set(keys)),
        "missing_tenant_scope_count": 0,
        "minimum_reporting_date": min(reporting_dates) if reporting_dates else None,
        "maximum_reporting_date": max(reporting_dates) if reporting_dates else None,
        "content_sha256": hashlib.sha256(content).hexdigest(),
    }


def _source_fingerprint(
    *,
    coverage_payload_sha256: str,
    dbt_result_identity_sha256: str,
    profiles: Mapping[str, Mapping[str, Any]],
) -> str:
    return _sha256_json(
        {
            "coverage_payload_sha256": coverage_payload_sha256,
            "datasets": profiles,
            "dbt_result_identity_sha256": dbt_result_identity_sha256,
            "serving_schema_version": SERVING_SCHEMA_VERSION,
        }
    )


def _validate_existing_publication(
    publication: PublicationRecord,
    *,
    pipeline_run_id: str,
    coverage_payload_sha256: str,
    dbt_result_identity_sha256: str,
    profiles: Mapping[str, Mapping[str, Any]],
) -> None:
    current = profiles[CURRENT_TABLE]
    history = profiles[HISTORY_TABLE]
    expected = {
        "pipeline_run_id": pipeline_run_id,
        "coverage_payload_sha256": coverage_payload_sha256,
        "dbt_result_identity_sha256": dbt_result_identity_sha256,
        "current_row_count": int(current["row_count"]),
        "history_row_count": int(history["row_count"]),
        "current_content_sha256": str(current["content_sha256"]),
        "history_content_sha256": str(history["content_sha256"]),
    }
    actual = asdict(publication)
    if publication.publication_status != READY_STATUS or any(
        actual[name] != value for name, value in expected.items()
    ):
        raise ServingPublicationError(
            "an existing publication marker conflicts with its source fingerprint"
        )


def _combined_date_coverage(
    current: Mapping[str, Any], history: Mapping[str, Any]
) -> tuple[str | None, str | None]:
    minimums = [
        str(value)
        for value in (
            current["minimum_reporting_date"],
            history["minimum_reporting_date"],
        )
        if value is not None
    ]
    maximums = [
        str(value)
        for value in (
            current["maximum_reporting_date"],
            history["maximum_reporting_date"],
        )
        if value is not None
    ]
    return (min(minimums) if minimums else None, max(maximums) if maximums else None)


def _sha256_json(value: Mapping[str, Any]) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _require_sha256(value: str, label: str) -> None:
    if not isinstance(value, str) or not SHA256_PATTERN.fullmatch(value):
        raise ServingPublicationError(f"{label} must be a lowercase SHA-256 hash")


def _require_attempt_id(value: str) -> None:
    if not isinstance(value, str) or not SAFE_ATTEMPT_ID.fullmatch(value):
        raise ServingPublicationError("load attempt ID has an invalid format")


def _trino_identifier(value: str) -> str:
    if not SAFE_IDENTIFIER.fullmatch(value):
        raise ServingPublicationError("unsafe Trino identifier")
    return f'"{value}"'


def _clickhouse_identifier(value: str) -> str:
    if not SAFE_IDENTIFIER.fullmatch(value):
        raise ServingPublicationError("unsafe ClickHouse identifier")
    return f"`{value}`"


def _qualified_clickhouse_table(database: str, table: str) -> str:
    return f"{_clickhouse_identifier(database)}.{_clickhouse_identifier(table)}"
