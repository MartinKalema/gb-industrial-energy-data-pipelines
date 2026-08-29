from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest
from fastapi.testclient import TestClient

from apps.api.app import create_app
from apps.api.auth import DEMO_ACTORS
from apps.api.clickhouse_repository import ClickHouseDeliveryPerformanceRepository
from apps.api.repository import DataVersionUnavailable, QueryScope
from apps.api.settings import Settings

DATA_VERSION = f"publication-{'a' * 32}"
PUBLISHED_AT = datetime(2026, 8, 29, 8, 30, tzinfo=UTC)
INTERVAL_KEY = "a" * 64


class FakeResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.column_names = list(rows[0]) if rows else []
        self.result_rows = [tuple(row.values()) for row in rows]


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

    def query(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
    ) -> FakeResult:
        parameters = parameters or {}
        settings = settings or {}
        self.calls.append((query, parameters, settings))
        if "publication_id AS data_version" in query:
            requested = parameters.get("requested_publication_id")
            if requested is not None and requested != DATA_VERSION:
                return FakeResult([])
            return FakeResult(
                [
                    {
                        "data_version": DATA_VERSION,
                        "data_published_at_utc": PUBLISHED_AT,
                    }
                ]
            )
        if "AS current_matches" in query:
            return FakeResult([{"current_matches": True, "history_matches": True}])
        if "GROUP BY" in query and "minimum_reporting_date" in query:
            return FakeResult(
                [
                    {
                        "customer_id": "CUST-001",
                        "customer_name": "Fictional Customer One",
                        "site_id": "SITE-001",
                        "site_name": "Synthetic Site One",
                        "delivery_point_id": "DP-001",
                        "delivery_point_name": "Synthetic Delivery Point One",
                        "minimum_reporting_date": date(2026, 8, 26),
                        "maximum_reporting_date": date(2026, 8, 26),
                    }
                ]
            )
        if "AS interval_count" in query:
            return FakeResult([summary_row()])
        if "count() OVER () AS total_count" in query:
            return FakeResult([{"total_count": 1, **interval_row()}])
        if "delivery_interval_history" in query:
            return FakeResult(
                [
                    {
                        **interval_row(),
                        "history_key": "history-001",
                        "known_from_at": PUBLISHED_AT,
                        "known_to_at": None,
                        "is_current_knowledge_state": True,
                    }
                ]
            )
        raise AssertionError(f"Unexpected ClickHouse query: {query}")


def summary_row() -> dict[str, Any]:
    return {
        "interval_count": 1,
        "expected_interval_count": 1,
        "commitment_record_count": 1,
        "missing_commitment_count": 0,
        "applicable_interval_count": 1,
        "accepted_applicable_delivery_count": 1,
        "final_applicable_capacity_count": 1,
        "known_delivered_mwh_th": Decimal("4.900000"),
        "committed_mwh_th": Decimal("5.000000"),
        "known_shortfall_mwh_th": Decimal("0.100000"),
        "known_excess_mwh_th": Decimal("0.000000"),
        "known_billable_mwh_th": Decimal("4.900000"),
        "sla_attainment_numerator_mwh_th": Decimal("4.900000"),
        "contractual_availability_numerator_mwh_th": Decimal("5.000000"),
        "known_gross_earned_revenue_gbp": Decimal("254.800000000000"),
        "known_accrued_sla_penalty_gbp": Decimal("10.000000000000"),
        "non_final_financial_count": 0,
        "distinct_currency_count": 1,
        "currency_code": "GBP",
        "latest_coverage_published_at_utc": PUBLISHED_AT,
    }


def interval_row() -> dict[str, Any]:
    return {
        "interval_key": INTERVAL_KEY,
        "customer_id": "CUST-001",
        "customer_name": "Fictional Customer One",
        "site_id": "SITE-001",
        "site_name": "Synthetic Site One",
        "delivery_point_id": "DP-001",
        "delivery_point_name": "Synthetic Delivery Point One",
        "reporting_date": date(2026, 8, 26),
        "local_period_number": 1,
        "interval_start_at": datetime(2026, 8, 25, 23, 0, tzinfo=UTC),
        "interval_end_at": datetime(2026, 8, 25, 23, 30, tzinfo=UTC),
        "interval_start_local": datetime(2026, 8, 26, 0, 0),  # noqa: DTZ001
        "interval_end_local": datetime(2026, 8, 26, 0, 30),  # noqa: DTZ001
        "operating_timezone": "Europe/London",
        "utc_offset_minutes": 60,
        "is_daylight_saving_time": True,
        "committed_mwh_th": Decimal("5.000000"),
        "delivered_mwh_th": Decimal("4.900000"),
        "shortfall_mwh_th": Decimal("0.100000"),
        "excess_mwh_th": Decimal("0.000000"),
        "deliverable_capacity_mwh_th": Decimal("5.000000"),
        "billable_mwh_th": Decimal("4.900000"),
        "gross_earned_revenue_gbp": Decimal("254.800000000000"),
        "accrued_sla_penalty_gbp": Decimal("10.000000000000"),
        "net_earned_revenue_gbp": Decimal("244.800000000000"),
        "currency_code": "GBP",
        "delivery_measurement_status": "accepted",
        "commitment_status": "accepted",
        "capacity_status": "final",
        "sla_result_status": "final",
        "availability_result_status": "final",
        "financial_result_status": "final",
        "correction_status": "original",
    }


def repository_with_fake() -> tuple[
    ClickHouseDeliveryPerformanceRepository, FakeClickHouseClient
]:
    client = FakeClickHouseClient()
    settings = Settings(
        demo_mode=True,
        repository_backend="clickhouse",
        clickhouse_password="unit-test-only-password",
    )
    return ClickHouseDeliveryPerformanceRepository(settings, client=client), client


def test_clickhouse_transport_deadline_outlives_query_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import clickhouse_connect

    captured: dict[str, Any] = {}

    def fake_get_client(**kwargs: Any) -> FakeClickHouseClient:
        captured.update(kwargs)
        return FakeClickHouseClient()

    monkeypatch.setattr(clickhouse_connect, "get_client", fake_get_client)
    repository = ClickHouseDeliveryPerformanceRepository(
        Settings(
            demo_mode=True,
            repository_backend="clickhouse",
            clickhouse_password="unit-test-only-password",
            clickhouse_timeout_seconds=7,
            clickhouse_query_timeout_seconds=19,
        )
    )

    repository._connect()

    assert captured["connect_timeout"] == 7
    assert captured["send_receive_timeout"] == 24
    assert captured["tz_mode"] == "schema"


def scope(*, customer_id: str | None = None) -> QueryScope:
    return QueryScope(
        start_date=date(2026, 8, 26),
        end_date=date(2026, 8, 26),
        customer_id=customer_id,
    )


def test_context_uses_latest_ready_publication_and_returns_version() -> None:
    repository, client = repository_with_fake()

    result = repository.get_context(DEMO_ACTORS["customer-cust-001"])

    assert result.data_version == DATA_VERSION
    assert result.data_published_at_utc == PUBLISHED_AT
    resolution_sql, resolution_parameters, _settings = client.calls[0]
    assert "publication_status = {ready_status:String}" in resolution_sql
    assert "ORDER BY published_at_utc DESC, publication_id DESC" in resolution_sql
    assert "requested_publication_id" not in resolution_parameters


def test_context_can_be_pinned_to_a_ready_publication() -> None:
    repository, client = repository_with_fake()

    result = repository.get_context(
        DEMO_ACTORS["customer-cust-001"], data_version=DATA_VERSION
    )

    assert result.data_version == DATA_VERSION
    assert client.calls[0][1]["requested_publication_id"] == DATA_VERSION


def test_readiness_checks_both_published_serving_tables() -> None:
    repository, client = repository_with_fake()

    assert repository.is_ready() is True

    integrity_sql, parameters, _settings = client.calls[1]
    assert "delivery_interval_current" in integrity_sql
    assert "delivery_interval_history" in integrity_sql
    assert "p.current_row_count" in integrity_sql
    assert "p.history_row_count" in integrity_sql
    assert parameters == {
        "ready_status": "ready",
        "publication_id": DATA_VERSION,
    }


def test_readiness_fails_when_a_published_serving_table_count_does_not_match() -> None:
    class MismatchedServingClient(FakeClickHouseClient):
        def query(
            self,
            query: str,
            parameters: dict[str, Any] | None = None,
            settings: dict[str, Any] | None = None,
        ) -> FakeResult:
            if "AS current_matches" in query:
                return FakeResult([{"current_matches": True, "history_matches": False}])
            return super().query(query, parameters, settings)

    repository = ClickHouseDeliveryPerformanceRepository(
        Settings(
            demo_mode=True,
            repository_backend="clickhouse",
            clickhouse_password="unit-test-only-password",
        ),
        client=MismatchedServingClient(),
    )

    assert repository.is_ready() is False


def test_summary_without_version_uses_the_latest_ready_publication() -> None:
    repository, client = repository_with_fake()

    repository.get_summary(DEMO_ACTORS["commercial-manager"], scope())

    _resolution_sql, resolution_parameters, _settings = client.calls[0]
    _summary_sql, summary_parameters, _settings = client.calls[1]
    assert "requested_publication_id" not in resolution_parameters
    assert summary_parameters["publication_id"] == DATA_VERSION


def test_summary_filters_ready_publication_and_tenant_before_optional_scope() -> None:
    repository, client = repository_with_fake()
    hostile_customer = "CUST-001' OR 1=1 --"

    repository.get_summary(
        DEMO_ACTORS["customer-cust-001"],
        scope(customer_id=hostile_customer),
        data_version=DATA_VERSION,
    )

    sql, parameters, settings = client.calls[-1]
    assert hostile_customer not in sql
    assert hostile_customer == parameters["customer_id"]
    assert parameters["publication_id"] == DATA_VERSION
    assert parameters["tenant_scope_0"] == "TENANT-CUST-001"
    assert "ANY INNER JOIN" not in sql
    assert "INNER JOIN (SELECT DISTINCT publication_id, publication_status" in sql
    assert "f.load_attempt_id = p.publication_id" in sql
    assert "p.publication_status = {ready_status:String}" in sql
    assert "p.publication_id = {publication_id:String}" in sql
    assert "f.tenant_authorization_scope_id IN ({tenant_scope_0:String})" in sql
    assert sql.index("f.tenant_authorization_scope_id IN") < sql.index(
        "f.customer_id = {customer_id:String}"
    )
    assert settings == {"max_execution_time": 60}


def test_interval_page_uses_a_cardinality_preserving_marker_join() -> None:
    repository, client = repository_with_fake()

    rows, total = repository.get_intervals(
        DEMO_ACTORS["commercial-manager"],
        scope(customer_id="CUST-001"),
        page=1,
        limit=25,
        data_version=DATA_VERSION,
    )

    sql, parameters, _settings = client.calls[-1]
    assert len(rows) == 1
    assert total == 1
    assert parameters["limit"] == 25
    assert "ANY INNER JOIN" not in sql
    assert "INNER JOIN (SELECT DISTINCT publication_id, publication_status" in sql


def test_unknown_requested_version_is_rejected_before_fact_query() -> None:
    repository, client = repository_with_fake()

    with pytest.raises(DataVersionUnavailable):
        repository.get_intervals(
            DEMO_ACTORS["commercial-manager"],
            scope(),
            page=1,
            limit=25,
            data_version=f"publication-{'b' * 32}",
        )

    assert len(client.calls) == 1
    assert client.calls[0][1]["requested_publication_id"] == f"publication-{'b' * 32}"


def test_repository_never_issues_mutation_queries() -> None:
    repository, client = repository_with_fake()
    actor = DEMO_ACTORS["customer-cust-001"]

    repository.get_context(actor)
    repository.get_summary(actor, scope(), data_version=DATA_VERSION)
    repository.get_intervals(
        actor, scope(), page=1, limit=25, data_version=DATA_VERSION
    )
    repository.get_interval_history(actor, INTERVAL_KEY, data_version=DATA_VERSION)

    assert client.calls
    assert all(sql.lstrip().upper().startswith("SELECT") for sql, _, _ in client.calls)
    assert all(
        not any(
            mutation in sql.upper()
            for mutation in ("INSERT ", "ALTER ", "DELETE ", "DROP ", "TRUNCATE ")
        )
        for sql, _, _ in client.calls
    )


def test_api_forwards_validated_version_header_and_rejects_unknown() -> None:
    repository, _client = repository_with_fake()
    app = create_app(
        settings=Settings(
            demo_mode=True,
            repository_backend="clickhouse",
            clickhouse_password="unit-test-only-password",
        ),
        repository=repository,
    )
    client = TestClient(app, raise_server_exceptions=False)
    headers = {
        "X-Demo-Actor": "customer-cust-001",
        "X-Product-Data-Version": DATA_VERSION,
    }

    response = client.get(
        "/api/v1/delivery-performance/summary"
        "?start_date=2026-08-26&end_date=2026-08-26",
        headers=headers,
    )
    unknown = client.get(
        "/api/v1/delivery-performance/summary"
        "?start_date=2026-08-26&end_date=2026-08-26",
        headers={
            **headers,
            "X-Product-Data-Version": f"publication-{'b' * 32}",
        },
    )
    malformed = client.get(
        "/api/v1/delivery-performance/summary"
        "?start_date=2026-08-26&end_date=2026-08-26",
        headers={**headers, "X-Product-Data-Version": "bad version with spaces"},
    )

    assert response.status_code == 200
    assert unknown.status_code == 409
    assert unknown.json()["detail"]["code"] == "data_version_unavailable"
    assert malformed.status_code == 422
    assert malformed.json()["detail"]["code"] == "invalid_data_version"


def test_create_app_selects_clickhouse_repository_backend() -> None:
    app = create_app(
        settings=Settings(
            demo_mode=True,
            repository_backend="clickhouse",
            clickhouse_password="unit-test-only-password",
        )
    )

    assert isinstance(app.state.repository, ClickHouseDeliveryPerformanceRepository)


def test_clickhouse_client_disables_sessions_for_parallel_request_safety(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, _client = repository_with_fake()
    repository._client = None
    captured: dict[str, Any] = {}

    def fake_get_client(**kwargs: Any) -> FakeClickHouseClient:
        captured.update(kwargs)
        return FakeClickHouseClient()

    monkeypatch.setattr("clickhouse_connect.get_client", fake_get_client)

    repository._connect()

    assert captured["autogenerate_session_id"] is False
    assert captured["username"] == "historical_delivery_api"
    assert captured["password"] == "unit-test-only-password"


@pytest.mark.parametrize(
    "overrides",
    [
        {"repository_backend": "unknown"},
        {
            "repository_backend": "clickhouse",
            "clickhouse_host": "http://clickhouse:8123/path",
            "clickhouse_password": "valid-password",
        },
        {
            "repository_backend": "clickhouse",
            "clickhouse_user": "invalid user",
            "clickhouse_password": "valid-password",
        },
        {
            "repository_backend": "clickhouse",
            "clickhouse_password": "",
        },
        {
            "repository_backend": "clickhouse",
            "clickhouse_password": "valid-password",
            "clickhouse_database": "serving; DROP DATABASE serving",
        },
    ],
)
def test_clickhouse_connection_and_identifier_settings_fail_closed(
    overrides: dict[str, Any],
) -> None:
    with pytest.raises(ValueError):
        Settings(**overrides)
