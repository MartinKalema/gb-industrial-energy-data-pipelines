"""Read-only access to atomically published ClickHouse product snapshots."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from threading import Lock
from typing import Any, Protocol

from apps.api.auth import Actor
from apps.api.repository import (
    HISTORY_RESPONSE_LIMIT,
    ContextResult,
    ContextRow,
    DataVersionUnavailable,
    MartIntegrityError,
    QueryScope,
    RepositoryReadiness,
    RepositoryUnavailable,
    SummaryAggregate,
    _integer,
)
from apps.api.settings import Settings


class ClickHouseQueryResult(Protocol):
    column_names: Sequence[str]
    result_rows: Sequence[Sequence[Any]]


class ClickHouseClient(Protocol):
    def query(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
    ) -> ClickHouseQueryResult: ...


@dataclass(frozen=True, slots=True)
class DataPublication:
    data_version: str
    data_published_at_utc: datetime


def _publication_visibility_relation(relation: str) -> str:
    """Deduplicate marker rows without ClickHouse ANY JOIN cardinality loss."""

    return f"(SELECT DISTINCT publication_id, publication_status FROM {relation})"


def _status_predicate(
    alias: str,
    status: str,
    parameters: dict[str, Any],
) -> str:
    """Build a closed-set status filter with server-side parameters."""

    if status == "final":
        parameters.update(
            {
                "provisional_status": "provisional",
                "final_status": "final",
            }
        )
        return (
            f"({alias}.sla_result_status != {{provisional_status:String}} "
            f"AND {alias}.availability_result_status != "
            "{provisional_status:String} "
            f"AND {alias}.financial_result_status = {{final_status:String}})"
        )
    if status == "provisional":
        parameters["provisional_status"] = "provisional"
        return (
            "has(["
            f"{alias}.sla_result_status, {alias}.availability_result_status, "
            f"{alias}.financial_result_status], {{provisional_status:String}})"
        )
    if status == "missing":
        parameters.update(
            {
                "missing_status": "missing",
                "withdrawn_status": "withdrawn",
                "accepted_status": "accepted",
            }
        )
        return (
            f"({alias}.commitment_status IN ({{missing_status:String}}, "
            "{withdrawn_status:String}) "
            f"OR {alias}.capacity_status IN ({{missing_status:String}}, "
            "{withdrawn_status:String}) "
            f"OR {alias}.delivery_measurement_status != {{accepted_status:String}})"
        )
    if status == "corrected":
        parameters["corrected_status"] = "corrected"
        return f"{alias}.correction_status = {{corrected_status:String}}"
    if status == "shortfall":
        return f"{alias}.shortfall_mwh_th > 0"
    if status == "excess":
        return f"{alias}.excess_mwh_th > 0"
    raise ValueError("unsupported status filter")


def _scope_predicates(
    actor: Actor,
    scope: QueryScope | None,
    *,
    fact_alias: str,
    publication_alias: str,
    publication: DataPublication,
) -> tuple[list[str], dict[str, Any]]:
    """Put publication visibility and tenant authorization before UI filters."""

    predicates = [
        f"{publication_alias}.publication_status = {{ready_status:String}}",
        f"{publication_alias}.publication_id = {{publication_id:String}}",
        f"{fact_alias}.customer_access_status = {{authorized_status:String}}",
        f"{fact_alias}.tenant_authorization_scope_id != ''",
    ]
    parameters: dict[str, Any] = {
        "ready_status": "ready",
        "publication_id": publication.data_version,
        "authorized_status": "authorized",
    }
    if actor.tenant_scope_ids is not None:
        if not actor.tenant_scope_ids:
            predicates.append("0")
        else:
            placeholders: list[str] = []
            for index, tenant_scope_id in enumerate(sorted(actor.tenant_scope_ids)):
                parameter_name = f"tenant_scope_{index}"
                placeholders.append(f"{{{parameter_name}:String}}")
                parameters[parameter_name] = tenant_scope_id
            predicates.append(
                f"{fact_alias}.tenant_authorization_scope_id IN "
                f"({', '.join(placeholders)})"
            )
    if scope is None:
        return predicates, parameters

    predicates.extend(
        [
            f"{fact_alias}.date_key >= {{start_date_key:UInt32}}",
            f"{fact_alias}.date_key <= {{end_date_key:UInt32}}",
        ]
    )
    parameters.update(
        {
            "start_date_key": QueryScope.date_key(scope.start_date),
            "end_date_key": QueryScope.date_key(scope.end_date),
        }
    )
    if scope.customer_id is not None:
        predicates.append(f"{fact_alias}.customer_id = {{customer_id:String}}")
        parameters["customer_id"] = scope.customer_id
    if scope.site_id is not None:
        predicates.append(f"{fact_alias}.site_id = {{site_id:String}}")
        parameters["site_id"] = scope.site_id
    if scope.delivery_point_id is not None:
        predicates.append(
            f"{fact_alias}.delivery_point_id = {{delivery_point_id:String}}"
        )
        parameters["delivery_point_id"] = scope.delivery_point_id
    if scope.status is not None:
        predicates.append(_status_predicate(fact_alias, scope.status, parameters))
    return predicates, parameters


class ClickHouseDeliveryPerformanceRepository:
    """Queries only validated, ready serving snapshots with immutable versions."""

    def __init__(
        self,
        settings: Settings,
        *,
        client: ClickHouseClient | None = None,
    ) -> None:
        self._settings = settings
        self._client = client
        self._client_lock = Lock()
        database = settings.clickhouse_database
        self._relations = {
            "current": f"`{database}`.`delivery_interval_current`",
            "history": f"`{database}`.`delivery_interval_history`",
            "publication": f"`{database}`.`data_publication`",
        }

    def _connect(self) -> ClickHouseClient:
        import clickhouse_connect

        return clickhouse_connect.get_client(
            host=self._settings.clickhouse_host,
            port=self._settings.clickhouse_http_port,
            username=self._settings.clickhouse_user,
            password=self._settings.clickhouse_password,
            secure=self._settings.clickhouse_secure,
            connect_timeout=self._settings.clickhouse_timeout_seconds,
            # One ClickHouse query uses one HTTP response, so the transport
            # deadline must outlive the server-side execution deadline.
            send_receive_timeout=max(
                self._settings.clickhouse_timeout_seconds,
                self._settings.clickhouse_query_timeout_seconds + 5,
            ),
            # Preserve UTC offsets on UTC-typed columns while keeping local
            # wall-clock DateTime64 columns naive, exactly as the API contract
            # distinguishes them.
            tz_mode="schema",
            # FastAPI runs synchronous handlers in a thread pool. A generated
            # ClickHouse session permits only one active query, so disable it
            # before sharing this client's HTTP connection pool.
            autogenerate_session_id=False,
        )

    def _get_client(self) -> ClickHouseClient:
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    self._client = self._connect()
        return self._client

    def _fetch_all(
        self, sql: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        try:
            result = self._get_client().query(
                sql,
                parameters=parameters or {},
                settings={
                    "max_execution_time": (
                        self._settings.clickhouse_query_timeout_seconds
                    ),
                },
            )
            columns = list(result.column_names)
            return [dict(zip(columns, row, strict=True)) for row in result.result_rows]
        except (MartIntegrityError, RepositoryUnavailable):
            raise
        except Exception as error:
            # Connection details and credentials must never cross this boundary.
            raise RepositoryUnavailable(
                "The ClickHouse serving query failed"
            ) from error

    def _fetch_one(
        self, sql: str, parameters: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        rows = self._fetch_all(sql, parameters)
        if len(rows) != 1:
            raise MartIntegrityError("Expected exactly one ClickHouse aggregate row")
        return rows[0]

    def _resolve_publication(self, data_version: str | None = None) -> DataPublication:
        publication = self._relations["publication"]
        parameters: dict[str, Any] = {"ready_status": "ready"}
        version_predicate = ""
        if data_version is not None:
            version_predicate = "AND publication_id = {requested_publication_id:String}"
            parameters["requested_publication_id"] = data_version
        rows = self._fetch_all(
            f"""
                SELECT
                    publication_id AS data_version,
                    published_at_utc AS data_published_at_utc
                FROM {publication}
                WHERE publication_status = {{ready_status:String}}
                  {version_predicate}
                ORDER BY published_at_utc DESC, publication_id DESC
                LIMIT 1
            """,
            parameters,
        )
        if not rows:
            if data_version is not None:
                raise DataVersionUnavailable(
                    "The requested product data version is not available"
                )
            raise RepositoryUnavailable("No ready product data publication exists")
        return DataPublication(**rows[0])

    def get_readiness(self) -> RepositoryReadiness:
        try:
            publication = self._resolve_publication()
            counts = self._fetch_one(
                f"""
                    SELECT
                        p.current_row_count AS expected_current_row_count,
                        (
                            SELECT count()
                            FROM {self._relations["current"]}
                            WHERE load_attempt_id = {{publication_id:String}}
                        ) AS actual_current_row_count,
                        p.history_row_count AS expected_history_row_count,
                        (
                            SELECT count()
                            FROM {self._relations["history"]}
                            WHERE load_attempt_id = {{publication_id:String}}
                        ) AS actual_history_row_count
                    FROM {self._relations["publication"]} AS p
                    WHERE p.publication_status = {{ready_status:String}}
                      AND p.publication_id = {{publication_id:String}}
                """,
                {
                    "ready_status": "ready",
                    "publication_id": publication.data_version,
                },
            )
        except MartIntegrityError:
            return RepositoryReadiness(
                backend="clickhouse",
                ready=False,
                reason="integrity_check_failed",
            )
        except RepositoryUnavailable:
            return RepositoryReadiness(
                backend="clickhouse",
                ready=False,
                reason="repository_unavailable",
            )

        expected_current = _integer(counts["expected_current_row_count"])
        actual_current = _integer(counts["actual_current_row_count"])
        expected_history = _integer(counts["expected_history_row_count"])
        actual_history = _integer(counts["actual_history_row_count"])
        counts_match = (
            expected_current == actual_current and expected_history == actual_history
        )
        return RepositoryReadiness(
            backend="clickhouse",
            ready=counts_match,
            reason="ready" if counts_match else "row_count_mismatch",
            data_version=publication.data_version,
            data_published_at_utc=publication.data_published_at_utc,
            expected_current_row_count=expected_current,
            actual_current_row_count=actual_current,
            expected_history_row_count=expected_history,
            actual_history_row_count=actual_history,
        )

    def is_ready(self) -> bool:
        return self.get_readiness().ready

    def get_context(
        self, actor: Actor, *, data_version: str | None = None
    ) -> ContextResult:
        publication = self._resolve_publication(data_version)
        current = self._relations["current"]
        publication_relation = self._relations["publication"]
        visible_publications = _publication_visibility_relation(publication_relation)
        predicates, parameters = _scope_predicates(
            actor,
            None,
            fact_alias="f",
            publication_alias="p",
            publication=publication,
        )
        rows = self._fetch_all(
            f"""
                SELECT
                    f.customer_id,
                    f.customer_name,
                    f.site_id,
                    f.site_name,
                    f.delivery_point_id,
                    f.delivery_point_name,
                    min(f.reporting_date) AS minimum_reporting_date,
                    max(f.reporting_date) AS maximum_reporting_date
                FROM {current} AS f
                INNER JOIN {visible_publications} AS p
                  ON f.load_attempt_id = p.publication_id
                WHERE {" AND ".join(predicates)}
                GROUP BY
                    f.customer_id,
                    f.customer_name,
                    f.site_id,
                    f.site_name,
                    f.delivery_point_id,
                    f.delivery_point_name
                ORDER BY f.customer_id, f.site_id, f.delivery_point_id
            """,
            parameters,
        )
        return ContextResult(
            rows=[ContextRow(**row) for row in rows],
            data_version=publication.data_version,
            data_published_at_utc=publication.data_published_at_utc,
        )

    def get_summary(
        self,
        actor: Actor,
        scope: QueryScope,
        *,
        data_version: str | None = None,
    ) -> SummaryAggregate:
        publication = self._resolve_publication(data_version)
        current = self._relations["current"]
        publication_relation = self._relations["publication"]
        visible_publications = _publication_visibility_relation(publication_relation)
        predicates, parameters = _scope_predicates(
            actor,
            scope,
            fact_alias="f",
            publication_alias="p",
            publication=publication,
        )
        parameters.update(
            {
                "missing_commitment_status": "missing",
                "withdrawn_commitment_status": "withdrawn",
                "final_financial_status": "final",
            }
        )
        row = self._fetch_one(
            f"""
                SELECT
                    count() AS interval_count,
                    sum(f.expected_interval_count) AS expected_interval_count,
                    sum(f.commitment_record_count) AS commitment_record_count,
                    countIf(f.commitment_status IN (
                        {{missing_commitment_status:String}},
                        {{withdrawn_commitment_status:String}}
                    )) AS missing_commitment_count,
                    sum(f.applicable_interval_count) AS applicable_interval_count,
                    sum(f.accepted_applicable_delivery_count)
                        AS accepted_applicable_delivery_count,
                    sum(f.final_applicable_capacity_count)
                        AS final_applicable_capacity_count,
                    sum(f.delivered_mwh_th) AS known_delivered_mwh_th,
                    sum(f.committed_mwh_th) AS committed_mwh_th,
                    sum(f.shortfall_mwh_th) AS known_shortfall_mwh_th,
                    sum(f.excess_mwh_th) AS known_excess_mwh_th,
                    sum(f.billable_mwh_th) AS known_billable_mwh_th,
                    sum(f.sla_attainment_numerator_mwh_th)
                        AS sla_attainment_numerator_mwh_th,
                    sum(f.contractual_availability_numerator_mwh_th)
                        AS contractual_availability_numerator_mwh_th,
                    sum(f.gross_earned_revenue_gbp)
                        AS known_gross_earned_revenue_gbp,
                    sum(f.accrued_sla_penalty_gbp)
                        AS known_accrued_sla_penalty_gbp,
                    countIf(
                        f.financial_result_status != {{final_financial_status:String}}
                    ) AS non_final_financial_count,
                    count(DISTINCT f.currency_code) AS distinct_currency_count,
                    min(f.currency_code) AS currency_code,
                    max(f.latest_coverage_published_at_utc)
                        AS latest_coverage_published_at_utc
                FROM {current} AS f
                INNER JOIN {visible_publications} AS p
                  ON f.load_attempt_id = p.publication_id
                WHERE {" AND ".join(predicates)}
            """,
            parameters,
        )
        return SummaryAggregate(
            interval_count=_integer(row["interval_count"]),
            expected_interval_count=_integer(row["expected_interval_count"]),
            commitment_record_count=_integer(row["commitment_record_count"]),
            missing_commitment_count=_integer(row["missing_commitment_count"]),
            applicable_interval_count=_integer(row["applicable_interval_count"]),
            accepted_applicable_delivery_count=_integer(
                row["accepted_applicable_delivery_count"]
            ),
            final_applicable_capacity_count=_integer(
                row["final_applicable_capacity_count"]
            ),
            known_delivered_mwh_th=row["known_delivered_mwh_th"],
            committed_mwh_th=row["committed_mwh_th"],
            known_shortfall_mwh_th=row["known_shortfall_mwh_th"],
            known_excess_mwh_th=row["known_excess_mwh_th"],
            known_billable_mwh_th=row["known_billable_mwh_th"],
            sla_attainment_numerator_mwh_th=row["sla_attainment_numerator_mwh_th"],
            contractual_availability_numerator_mwh_th=row[
                "contractual_availability_numerator_mwh_th"
            ],
            known_gross_earned_revenue_gbp=row["known_gross_earned_revenue_gbp"],
            known_accrued_sla_penalty_gbp=row["known_accrued_sla_penalty_gbp"],
            non_final_financial_count=_integer(row["non_final_financial_count"]),
            distinct_currency_count=_integer(row["distinct_currency_count"]),
            currency_code=row["currency_code"],
            latest_coverage_published_at_utc=row["latest_coverage_published_at_utc"],
        )

    def get_intervals(
        self,
        actor: Actor,
        scope: QueryScope,
        *,
        page: int,
        limit: int,
        data_version: str | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        publication = self._resolve_publication(data_version)
        current = self._relations["current"]
        publication_relation = self._relations["publication"]
        visible_publications = _publication_visibility_relation(publication_relation)
        predicates, parameters = _scope_predicates(
            actor,
            scope,
            fact_alias="f",
            publication_alias="p",
            publication=publication,
        )
        joins = f"""
            FROM {current} AS f
            INNER JOIN {visible_publications} AS p
              ON f.load_attempt_id = p.publication_id
            WHERE {" AND ".join(predicates)}
        """
        parameters.update(
            {
                "offset": (page - 1) * limit,
                "limit": limit,
            }
        )
        rows = self._fetch_all(
            f"""
                SELECT
                    count() OVER () AS total_count,
                    f.interval_key,
                    f.customer_id,
                    f.customer_name,
                    f.site_id,
                    f.site_name,
                    f.delivery_point_id,
                    f.delivery_point_name,
                    f.reporting_date,
                    f.local_period_number,
                    f.interval_start_at,
                    f.interval_end_at,
                    f.interval_start_local,
                    f.interval_end_local,
                    f.operating_timezone,
                    f.utc_offset_minutes,
                    f.is_daylight_saving_time,
                    f.committed_mwh_th,
                    f.delivered_mwh_th,
                    f.shortfall_mwh_th,
                    f.excess_mwh_th,
                    f.deliverable_capacity_mwh_th,
                    f.billable_mwh_th,
                    f.gross_earned_revenue_gbp,
                    f.accrued_sla_penalty_gbp,
                    f.net_earned_revenue_gbp,
                    f.currency_code,
                    f.delivery_measurement_status,
                    f.commitment_status,
                    f.capacity_status,
                    f.sla_result_status,
                    f.availability_result_status,
                    f.financial_result_status,
                    f.correction_status
                {joins}
                ORDER BY f.interval_start_at, f.delivery_point_id, f.interval_key
                LIMIT {{limit:UInt32}} OFFSET {{offset:UInt64}}
            """,
            parameters,
        )
        if rows:
            total = _integer(rows[0]["total_count"])
            for row in rows:
                del row["total_count"]
            return rows, total

        # An out-of-range page has no window-count row.
        count_parameters = {
            key: value
            for key, value in parameters.items()
            if key not in {"offset", "limit"}
        }
        count_row = self._fetch_one(
            f"SELECT count() AS total {joins}", count_parameters
        )
        return [], _integer(count_row["total"])

    def get_interval_history(
        self,
        actor: Actor,
        interval_key: str,
        *,
        as_of: datetime | None = None,
        data_version: str | None = None,
    ) -> list[dict[str, Any]]:
        publication = self._resolve_publication(data_version)
        history = self._relations["history"]
        publication_relation = self._relations["publication"]
        visible_publications = _publication_visibility_relation(publication_relation)
        predicates, parameters = _scope_predicates(
            actor,
            None,
            fact_alias="h",
            publication_alias="p",
            publication=publication,
        )
        predicates.append("h.interval_key = {interval_key:String}")
        parameters["interval_key"] = interval_key
        if as_of is not None:
            predicates.extend(
                [
                    "h.known_from_at <= {as_of:DateTime64(6, 'UTC')}",
                    (
                        "(h.known_to_at IS NULL "
                        "OR {as_of:DateTime64(6, 'UTC')} < h.known_to_at)"
                    ),
                ]
            )
            parameters["as_of"] = as_of
        return self._fetch_all(
            f"""
                SELECT
                    h.interval_key,
                    h.history_key,
                    h.customer_id,
                    h.customer_name,
                    h.site_id,
                    h.site_name,
                    h.delivery_point_id,
                    h.delivery_point_name,
                    h.reporting_date,
                    h.local_period_number,
                    h.interval_start_at,
                    h.interval_end_at,
                    h.interval_start_local,
                    h.interval_end_local,
                    h.operating_timezone,
                    h.utc_offset_minutes,
                    h.is_daylight_saving_time,
                    h.known_from_at,
                    h.known_to_at,
                    h.is_current_knowledge_state,
                    h.committed_mwh_th,
                    h.delivered_mwh_th,
                    h.shortfall_mwh_th,
                    h.excess_mwh_th,
                    h.deliverable_capacity_mwh_th,
                    h.billable_mwh_th,
                    h.gross_earned_revenue_gbp,
                    h.accrued_sla_penalty_gbp,
                    h.net_earned_revenue_gbp,
                    h.currency_code,
                    h.delivery_measurement_status,
                    h.commitment_status,
                    h.capacity_status,
                    h.sla_result_status,
                    h.availability_result_status,
                    h.financial_result_status,
                    h.correction_status
                FROM {history} AS h
                INNER JOIN {visible_publications} AS p
                  ON h.load_attempt_id = p.publication_id
                WHERE {" AND ".join(predicates)}
                ORDER BY h.known_from_at DESC, h.history_key DESC
                LIMIT {HISTORY_RESPONSE_LIMIT + 1}
            """,
            parameters,
        )
