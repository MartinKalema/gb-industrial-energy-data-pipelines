"""Read-only, parameterized Trino access to the governed Iceberg marts."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import closing, suppress
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Protocol

from apps.api.auth import Actor
from apps.api.settings import Settings

HISTORY_RESPONSE_LIMIT = 200


class RepositoryUnavailable(Exception):
    """The governed mart could not be queried."""


class MartIntegrityError(Exception):
    """The mart violates an assumption required by the API contract."""


class DataVersionUnavailable(Exception):
    """The requested immutable product publication is not ready or retained."""


@dataclass(frozen=True, slots=True)
class QueryScope:
    start_date: date
    end_date: date
    customer_id: str | None = None
    site_id: str | None = None
    delivery_point_id: str | None = None
    status: str | None = None

    @staticmethod
    def date_key(value: date) -> int:
        return value.year * 10_000 + value.month * 100 + value.day


@dataclass(frozen=True, slots=True)
class ContextRow:
    customer_id: str
    customer_name: str
    site_id: str
    site_name: str
    delivery_point_id: str
    delivery_point_name: str
    minimum_reporting_date: date
    maximum_reporting_date: date


@dataclass(frozen=True, slots=True)
class ContextResult:
    rows: list[ContextRow]
    data_version: str | None
    data_published_at_utc: datetime | None


@dataclass(frozen=True, slots=True)
class RepositoryReadiness:
    """Small, value-safe evidence used by the operational readiness endpoint."""

    backend: str
    ready: bool
    reason: str
    data_version: str | None = None
    data_published_at_utc: datetime | None = None
    expected_current_row_count: int | None = None
    actual_current_row_count: int | None = None
    expected_history_row_count: int | None = None
    actual_history_row_count: int | None = None


@dataclass(frozen=True, slots=True)
class SummaryAggregate:
    interval_count: int
    expected_interval_count: int
    commitment_record_count: int
    missing_commitment_count: int
    applicable_interval_count: int
    accepted_applicable_delivery_count: int
    final_applicable_capacity_count: int
    known_delivered_mwh_th: Decimal | None
    committed_mwh_th: Decimal | None
    known_shortfall_mwh_th: Decimal | None
    known_excess_mwh_th: Decimal | None
    known_billable_mwh_th: Decimal | None
    sla_attainment_numerator_mwh_th: Decimal | None
    contractual_availability_numerator_mwh_th: Decimal | None
    known_gross_earned_revenue_gbp: Decimal | None
    known_accrued_sla_penalty_gbp: Decimal | None
    non_final_financial_count: int
    distinct_currency_count: int
    currency_code: str | None
    latest_coverage_published_at_utc: datetime | None


class DeliveryPerformanceRepository(Protocol):
    def get_readiness(self) -> RepositoryReadiness: ...

    def is_ready(self) -> bool: ...

    def get_context(
        self, actor: Actor, *, data_version: str | None = None
    ) -> ContextResult: ...

    def get_summary(
        self, actor: Actor, scope: QueryScope, *, data_version: str | None = None
    ) -> SummaryAggregate: ...

    def get_intervals(
        self,
        actor: Actor,
        scope: QueryScope,
        *,
        page: int,
        limit: int,
        data_version: str | None = None,
    ) -> tuple[list[dict[str, Any]], int]: ...

    def get_interval_history(
        self,
        actor: Actor,
        interval_key: str,
        *,
        as_of: datetime | None = None,
        data_version: str | None = None,
    ) -> list[dict[str, Any]]: ...


def _integer(value: Any) -> int:
    """Turn a nullable governed count aggregate into its defined empty count."""

    return 0 if value is None else int(value)


def _status_predicate(alias: str, status: str) -> tuple[str, list[Any]]:
    """Return one closed-set presentation filter without interpolating its value."""

    if status == "final":
        clause = (
            f"({alias}.sla_result_status <> ? "
            f"AND {alias}.availability_result_status <> ? "
            f"AND {alias}.financial_result_status = ?)"
        )
        return clause, ["provisional", "provisional", "final"]
    if status == "provisional":
        clause = (
            f"(? IN ({alias}.sla_result_status, "
            f"{alias}.availability_result_status, {alias}.financial_result_status))"
        )
        return clause, ["provisional"]
    if status == "missing":
        clause = (
            f"({alias}.commitment_status IN (?, ?) "
            f"OR {alias}.capacity_status IN (?, ?) "
            f"OR {alias}.delivery_measurement_status <> ?)"
        )
        return clause, [
            "missing",
            "withdrawn",
            "missing",
            "withdrawn",
            "accepted",
        ]
    if status == "corrected":
        return f"{alias}.correction_status = ?", ["corrected"]
    if status == "shortfall":
        return f"{alias}.shortfall_mwh_th > ?", [Decimal(0)]
    if status == "excess":
        return f"{alias}.excess_mwh_th > ?", [Decimal(0)]
    raise ValueError("unsupported status filter")


def _scope_predicates(
    actor: Actor,
    scope: QueryScope | None,
    *,
    fact_alias: str,
    customer_alias: str,
) -> tuple[list[str], list[Any]]:
    """Build mandatory tenant authorization before optional caller filters."""

    predicates = [
        f"{fact_alias}.customer_access_status = ?",
        f"{customer_alias}.tenant_authorization_scope_id IS NOT NULL",
    ]
    parameters: list[Any] = ["authorized"]
    if actor.tenant_scope_ids is not None:
        placeholders = ", ".join("?" for _ in actor.tenant_scope_ids)
        predicates.append(
            f"{customer_alias}.tenant_authorization_scope_id IN ({placeholders})"
        )
        parameters.extend(sorted(actor.tenant_scope_ids))
    if scope is None:
        return predicates, parameters

    predicates.extend([f"{fact_alias}.date_key >= ?", f"{fact_alias}.date_key <= ?"])
    parameters.extend(
        [QueryScope.date_key(scope.start_date), QueryScope.date_key(scope.end_date)]
    )
    if scope.customer_id is not None:
        predicates.append(f"{fact_alias}.customer_natural_id = ?")
        parameters.append(scope.customer_id)
    if scope.site_id is not None:
        predicates.append(f"{fact_alias}.site_natural_id = ?")
        parameters.append(scope.site_id)
    if scope.delivery_point_id is not None:
        predicates.append(f"{fact_alias}.delivery_point_natural_id = ?")
        parameters.append(scope.delivery_point_id)
    if scope.status is not None:
        clause, status_parameters = _status_predicate(fact_alias, scope.status)
        predicates.append(clause)
        parameters.extend(status_parameters)
    return predicates, parameters


class TrinoDeliveryPerformanceRepository:
    """Finite DBAPI queries over only the current/history facts and dimensions."""

    def __init__(
        self,
        settings: Settings,
        *,
        connection_factory: Callable[[], Any] | None = None,
    ) -> None:
        self._settings = settings
        self._connection_factory = connection_factory or self._connect
        mart = f'"{settings.trino_catalog}"."{settings.trino_schema}"'
        self._relations = {
            "current_fact": f'{mart}."fct_steam_delivery_interval"',
            "history_fact": f'{mart}."fct_steam_delivery_interval_history"',
            "customer": f'{mart}."dim_customer"',
            "site": f'{mart}."dim_site"',
            "delivery_point": f'{mart}."dim_delivery_point"',
            "interval": f'{mart}."dim_interval"',
            "customer_audit": f'{mart}."dim_customer_revision_audit"',
            "site_audit": f'{mart}."dim_site_revision_audit"',
            "delivery_point_audit": (f'{mart}."dim_delivery_point_revision_audit"'),
        }

    def _connect(self) -> Any:
        from trino.dbapi import connect

        query_limit = f"{self._settings.trino_query_timeout_seconds}s"
        return connect(
            host=self._settings.trino_host,
            port=self._settings.trino_port,
            user=self._settings.trino_user,
            catalog=self._settings.trino_catalog,
            schema=self._settings.trino_schema,
            http_scheme=self._settings.trino_http_scheme,
            source="historical-delivery-api",
            client_tags=["read-only-product"],
            request_timeout=self._settings.trino_timeout_seconds,
            # ``request_timeout`` only bounds one HTTP exchange. These Trino
            # session limits bound the complete queued/running query and make
            # the coordinator cancel work that exceeds the product deadline.
            session_properties={
                "query_max_run_time": query_limit,
                "query_max_execution_time": query_limit,
            },
        )

    def _fetch_all(self, sql: str, parameters: Sequence[Any]) -> list[dict[str, Any]]:
        try:
            with (
                closing(self._connection_factory()) as connection,
                closing(connection.cursor()) as cursor,
            ):
                try:
                    cursor.execute(sql, list(parameters))
                    columns = [description[0] for description in cursor.description]
                    return [
                        dict(zip(columns, row, strict=True))
                        for row in cursor.fetchall()
                    ]
                except Exception:
                    # The Trino cursor's close() also cancels, but make the
                    # cancellation explicit while the cursor is still usable.
                    # Cancellation failures must never mask the query failure.
                    cancel = getattr(cursor, "cancel", None)
                    if cancel is not None:
                        with suppress(Exception):
                            cancel()
                    raise
        except (MartIntegrityError, RepositoryUnavailable):
            raise
        except Exception as error:
            raise RepositoryUnavailable("The Trino mart query failed") from error

    def _fetch_one(self, sql: str, parameters: Sequence[Any]) -> dict[str, Any]:
        rows = self._fetch_all(sql, parameters)
        if len(rows) != 1:
            raise MartIntegrityError("Expected exactly one aggregate row")
        return rows[0]

    def get_readiness(self) -> RepositoryReadiness:
        # An intentionally empty mart is still ready, so success must depend on
        # whether the core fact and its authorization dimension can be queried,
        # not on either relation returning a data row. Each query reads at most
        # one row and therefore remains a bounded readiness probe.
        try:
            for relation_name in ("current_fact", "customer"):
                relation = self._relations[relation_name]
                self._fetch_all(
                    f"SELECT 1 AS relation_is_queryable FROM {relation} LIMIT 1", []
                )
        except RepositoryUnavailable:
            return RepositoryReadiness(
                backend="trino",
                ready=False,
                reason="repository_unavailable",
            )
        return RepositoryReadiness(
            backend="trino",
            ready=True,
            reason="queryable_relations",
        )

    def is_ready(self) -> bool:
        return self.get_readiness().ready

    def get_context(
        self, actor: Actor, *, data_version: str | None = None
    ) -> ContextResult:
        del data_version
        current_fact = self._relations["current_fact"]
        customer_relation = self._relations["customer"]
        site_relation = self._relations["site"]
        delivery_point_relation = self._relations["delivery_point"]
        interval_relation = self._relations["interval"]
        predicates, parameters = _scope_predicates(
            actor, None, fact_alias="f", customer_alias="c"
        )
        sql = f"""
            SELECT
                f.customer_natural_id AS customer_id,
                max_by(c.display_name, c.effective_from_utc) AS customer_name,
                f.site_natural_id AS site_id,
                max_by(s.site_name, s.effective_from_utc) AS site_name,
                f.delivery_point_natural_id AS delivery_point_id,
                max_by(dp.delivery_point_name, dp.effective_from_utc)
                    AS delivery_point_name,
                min(d.reporting_date) AS minimum_reporting_date,
                max(d.reporting_date) AS maximum_reporting_date
            FROM {current_fact} AS f
            JOIN {customer_relation} AS c ON f.customer_key = c.customer_key
            JOIN {site_relation} AS s ON f.site_key = s.site_key
            JOIN {delivery_point_relation} AS dp
              ON f.delivery_point_key = dp.delivery_point_key
            JOIN {interval_relation} AS d ON f.interval_key = d.interval_key
            WHERE {" AND ".join(predicates)}
            GROUP BY 1, 3, 5
            ORDER BY 1, 3, 5
        """
        return ContextResult(
            rows=[ContextRow(**row) for row in self._fetch_all(sql, parameters)],
            data_version=None,
            data_published_at_utc=None,
        )

    def get_summary(
        self,
        actor: Actor,
        scope: QueryScope,
        *,
        data_version: str | None = None,
    ) -> SummaryAggregate:
        if data_version is not None:
            raise DataVersionUnavailable(
                "The Trino reference repository does not expose publications"
            )
        current_fact = self._relations["current_fact"]
        customer_relation = self._relations["customer"]
        predicates, parameters = _scope_predicates(
            actor, scope, fact_alias="f", customer_alias="c"
        )
        sql = f"""
            SELECT
                count(*) AS interval_count,
                sum(f.expected_interval_count) AS expected_interval_count,
                sum(f.commitment_record_count) AS commitment_record_count,
                count_if(f.commitment_status IN (?, ?))
                    AS missing_commitment_count,
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
                count_if(f.financial_result_status <> ?)
                    AS non_final_financial_count,
                count(DISTINCT f.currency_code) AS distinct_currency_count,
                min(f.currency_code) AS currency_code,
                max(f.latest_coverage_published_at_utc)
                    AS latest_coverage_published_at_utc
            FROM {current_fact} AS f
            JOIN {customer_relation} AS c ON f.customer_key = c.customer_key
            WHERE {" AND ".join(predicates)}
        """
        row = self._fetch_one(sql, ["missing", "withdrawn", "final", *parameters])
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
        if data_version is not None:
            raise DataVersionUnavailable(
                "The Trino reference repository does not expose publications"
            )
        current_fact = self._relations["current_fact"]
        customer_relation = self._relations["customer"]
        site_relation = self._relations["site"]
        delivery_point_relation = self._relations["delivery_point"]
        interval_relation = self._relations["interval"]
        predicates, parameters = _scope_predicates(
            actor, scope, fact_alias="f", customer_alias="c"
        )
        joins = f"""
            FROM {current_fact} AS f
            JOIN {customer_relation} AS c ON f.customer_key = c.customer_key
            JOIN {site_relation} AS s ON f.site_key = s.site_key
            JOIN {delivery_point_relation} AS dp
              ON f.delivery_point_key = dp.delivery_point_key
            JOIN {interval_relation} AS i ON f.interval_key = i.interval_key
            WHERE {" AND ".join(predicates)}
        """
        sql = f"""
            SELECT
                count(*) OVER () AS total_count,
                f.delivery_interval_key AS interval_key,
                f.customer_natural_id AS customer_id,
                c.display_name AS customer_name,
                f.site_natural_id AS site_id,
                s.site_name,
                f.delivery_point_natural_id AS delivery_point_id,
                dp.delivery_point_name,
                i.reporting_date,
                i.local_period_number,
                f.interval_start_utc AS interval_start_at,
                f.interval_end_utc AS interval_end_at,
                i.interval_start_local,
                i.interval_end_local,
                i.operating_timezone,
                i.utc_offset_minutes,
                i.is_daylight_saving_time,
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
            ORDER BY
                f.interval_start_utc,
                f.delivery_point_natural_id,
                f.delivery_interval_key
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """
        offset = (page - 1) * limit
        rows = self._fetch_all(sql, [*parameters, offset, limit])
        if rows:
            total = _integer(rows[0]["total_count"])
            for row in rows:
                del row["total_count"]
            return rows, total

        # A page beyond the end has no window-count row. Only that uncommon
        # request pays for a second bounded-scope count query.
        count_row = self._fetch_one(f"SELECT count(*) AS total {joins}", parameters)
        return [], _integer(count_row["total"])

    def get_interval_history(
        self,
        actor: Actor,
        interval_key: str,
        *,
        as_of: datetime | None = None,
        data_version: str | None = None,
    ) -> list[dict[str, Any]]:
        if data_version is not None:
            raise DataVersionUnavailable(
                "The Trino reference repository does not expose publications"
            )
        history_fact = self._relations["history_fact"]
        customer_audit = self._relations["customer_audit"]
        site_audit = self._relations["site_audit"]
        delivery_point_audit = self._relations["delivery_point_audit"]
        interval_relation = self._relations["interval"]
        predicates, parameters = _scope_predicates(
            actor, None, fact_alias="h", customer_alias="c"
        )
        predicates.append("h.delivery_interval_key = ?")
        parameters.append(interval_key)
        if as_of is not None:
            predicates.append("h.known_from_utc <= ?")
            predicates.append("(h.known_to_utc IS NULL OR ? < h.known_to_utc)")
            parameters.extend([as_of, as_of])
        sql = f"""
            SELECT
                h.delivery_interval_key AS interval_key,
                h.delivery_interval_history_key AS history_key,
                h.customer_natural_id AS customer_id,
                c.display_name AS customer_name,
                h.site_natural_id AS site_id,
                s.site_name,
                h.delivery_point_natural_id AS delivery_point_id,
                dp.delivery_point_name,
                i.reporting_date,
                i.local_period_number,
                h.interval_start_utc AS interval_start_at,
                h.interval_end_utc AS interval_end_at,
                i.interval_start_local,
                i.interval_end_local,
                i.operating_timezone,
                i.utc_offset_minutes,
                i.is_daylight_saving_time,
                h.known_from_utc AS known_from_at,
                h.known_to_utc AS known_to_at,
                h.known_to_utc IS NULL AS is_current_knowledge_state,
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
            FROM {history_fact} AS h
            JOIN {customer_audit} AS c
              ON h.customer_revision_key = c.customer_revision_key
            JOIN {site_audit} AS s
              ON h.site_revision_key = s.site_revision_key
            JOIN {delivery_point_audit} AS dp
              ON h.delivery_point_revision_key = dp.delivery_point_revision_key
            JOIN {interval_relation} AS i ON h.interval_key = i.interval_key
            WHERE {" AND ".join(predicates)}
            ORDER BY h.known_from_utc DESC, h.delivery_interval_history_key DESC
            LIMIT {HISTORY_RESPONSE_LIMIT + 1}
        """
        return self._fetch_all(sql, parameters)
