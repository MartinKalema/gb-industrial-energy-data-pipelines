from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, ClassVar

import pytest
from fastapi.testclient import TestClient

from apps.api.app import create_app
from apps.api.auth import DEMO_ACTORS, Actor
from apps.api.repository import (
    ContextResult,
    ContextRow,
    QueryScope,
    RepositoryReadiness,
    RepositoryUnavailable,
    SummaryAggregate,
    TrinoDeliveryPerformanceRepository,
)
from apps.api.request_logging import AUDIT_HANDLER_NAME, LOGGER
from apps.api.settings import Settings

INTERVAL_KEY = "a" * 64
OTHER_INTERVAL_KEY = "b" * 64
AT = datetime(2026, 8, 25, 23, 0, tzinfo=UTC)
LATER = datetime(2026, 8, 25, 23, 30, tzinfo=UTC)


def complete_summary() -> SummaryAggregate:
    return SummaryAggregate(
        interval_count=48,
        expected_interval_count=48,
        commitment_record_count=48,
        missing_commitment_count=0,
        applicable_interval_count=48,
        accepted_applicable_delivery_count=48,
        final_applicable_capacity_count=48,
        known_delivered_mwh_th=Decimal("192.960000"),
        committed_mwh_th=Decimal("192.000000"),
        known_shortfall_mwh_th=Decimal("0.000000"),
        known_excess_mwh_th=Decimal("0.960000"),
        known_billable_mwh_th=Decimal("192.000000"),
        sla_attainment_numerator_mwh_th=Decimal("192.000000"),
        contractual_availability_numerator_mwh_th=Decimal("192.000000"),
        known_gross_earned_revenue_gbp=Decimal("9984.000000000000"),
        known_accrued_sla_penalty_gbp=Decimal("0.000000000000"),
        non_final_financial_count=0,
        distinct_currency_count=1,
        currency_code="GBP",
        latest_coverage_published_at_utc=datetime(2026, 8, 28, 12, 0, tzinfo=UTC),
    )


def provisional_portfolio_summary() -> SummaryAggregate:
    return SummaryAggregate(
        interval_count=96,
        expected_interval_count=96,
        commitment_record_count=95,
        missing_commitment_count=1,
        applicable_interval_count=94,
        accepted_applicable_delivery_count=94,
        final_applicable_capacity_count=92,
        known_delivered_mwh_th=Decimal("429.290000"),
        committed_mwh_th=Decimal("422.500000"),
        known_shortfall_mwh_th=Decimal("0.700000"),
        known_excess_mwh_th=Decimal("2.590000"),
        known_billable_mwh_th=Decimal("422.300000"),
        sla_attainment_numerator_mwh_th=Decimal("421.800000"),
        contractual_availability_numerator_mwh_th=Decimal("412.500000"),
        known_gross_earned_revenue_gbp=Decimal("22399.000000000000"),
        known_accrued_sla_penalty_gbp=Decimal("70.000000000000"),
        non_final_financial_count=1,
        distinct_currency_count=1,
        currency_code="GBP",
        latest_coverage_published_at_utc=datetime(2026, 8, 28, 12, 0, tzinfo=UTC),
    )


def interval_row(*, key: str, committed: Decimal | None) -> dict[str, Any]:
    has_commitment = committed is not None
    return {
        "interval_key": key,
        "customer_id": "CUST-001",
        "customer_name": "Fictional Customer One",
        "site_id": "SITE-001",
        "site_name": "Synthetic Site One",
        "delivery_point_id": "DP-001",
        "delivery_point_name": "Synthetic Steam Delivery Point One",
        "reporting_date": date(2026, 8, 26),
        "local_period_number": 1,
        "interval_start_at": AT,
        "interval_end_at": LATER,
        # The mart intentionally stores wall-clock local timestamps without an offset.
        "interval_start_local": datetime(2026, 8, 26, 0, 0),  # noqa: DTZ001
        "interval_end_local": datetime(2026, 8, 26, 0, 30),  # noqa: DTZ001
        "operating_timezone": "Europe/London",
        "utc_offset_minutes": 60,
        "is_daylight_saving_time": True,
        "committed_mwh_th": committed,
        "delivered_mwh_th": Decimal("4.900000"),
        "shortfall_mwh_th": Decimal("0.000000") if has_commitment else None,
        "excess_mwh_th": Decimal("0.000000") if has_commitment else None,
        "deliverable_capacity_mwh_th": Decimal("5.000000"),
        "billable_mwh_th": Decimal("0.000000") if has_commitment else None,
        "gross_earned_revenue_gbp": (
            Decimal("0.000000000000") if has_commitment else None
        ),
        "accrued_sla_penalty_gbp": (
            Decimal("0.000000000000") if has_commitment else None
        ),
        "net_earned_revenue_gbp": (
            Decimal("0.000000000000") if has_commitment else None
        ),
        "currency_code": "GBP",
        "delivery_measurement_status": "accepted",
        "commitment_status": "no_commitment" if has_commitment else "missing",
        "capacity_status": "final",
        "sla_result_status": "not_applicable" if has_commitment else "provisional",
        "availability_result_status": (
            "not_applicable" if has_commitment else "provisional"
        ),
        "financial_result_status": "final" if has_commitment else "provisional",
        "correction_status": "original",
    }


def history_row(position: int) -> dict[str, Any]:
    return {
        **interval_row(key=INTERVAL_KEY, committed=Decimal("5.000000")),
        "history_key": f"history-{position:03d}",
        "known_from_at": AT,
        "known_to_at": LATER,
        "is_current_knowledge_state": False,
    }


class FakeRepository:
    def __init__(self) -> None:
        self.ready: bool | Exception = True
        self.readiness_override: RepositoryReadiness | None = None
        self.summary_value = complete_summary()
        self.interval_rows = [
            interval_row(key=INTERVAL_KEY, committed=None),
            interval_row(key=OTHER_INTERVAL_KEY, committed=Decimal("0.000000")),
        ]
        self.history_rows: list[dict[str, Any]] = []
        self.calls: list[tuple[Any, ...]] = []

    def is_ready(self) -> bool:
        if isinstance(self.ready, Exception):
            raise self.ready
        return self.ready

    def get_readiness(self) -> RepositoryReadiness:
        if isinstance(self.ready, Exception):
            raise self.ready
        if self.readiness_override is not None:
            return self.readiness_override
        return RepositoryReadiness(
            backend="trino",
            ready=self.ready,
            reason="queryable_relations" if self.ready else "repository_unavailable",
        )

    def get_context(
        self, actor: Actor, *, data_version: str | None = None
    ) -> ContextResult:
        self.calls.append(("context", actor, data_version))
        customer = "CUST-002" if actor.actor_id == "customer-cust-002" else "CUST-001"
        site = "SITE-002" if customer == "CUST-002" else "SITE-001"
        point = "DP-002" if customer == "CUST-002" else "DP-001"
        return ContextResult(
            rows=[
                ContextRow(
                    customer_id=customer,
                    customer_name=f"Fictional {customer}",
                    site_id=site,
                    site_name=f"Synthetic {site}",
                    delivery_point_id=point,
                    delivery_point_name=f"Synthetic {point}",
                    minimum_reporting_date=date(2026, 8, 26),
                    maximum_reporting_date=date(2026, 8, 26),
                )
            ],
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
        self.calls.append(("summary", actor, scope, data_version))
        return self.summary_value

    def get_intervals(
        self,
        actor: Actor,
        scope: QueryScope,
        *,
        page: int,
        limit: int,
        data_version: str | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        self.calls.append(("intervals", actor, scope, page, limit, data_version))
        return self.interval_rows, len(self.interval_rows)

    def get_interval_history(
        self,
        actor: Actor,
        interval_key: str,
        *,
        as_of: datetime | None = None,
        data_version: str | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append(("history", actor, interval_key, as_of, data_version))
        return self.history_rows


@pytest.fixture
def repository() -> FakeRepository:
    return FakeRepository()


@pytest.fixture
def client(repository: FakeRepository) -> TestClient:
    app = create_app(settings=Settings(demo_mode=True), repository=repository)
    return TestClient(app, raise_server_exceptions=False)


def query(path: str) -> str:
    return f"{path}?start_date=2026-08-26&end_date=2026-08-26"


def actor_headers(actor: str = "commercial-manager") -> dict[str, str]:
    return {"X-Demo-Actor": actor}


def test_liveness_does_not_require_identity(client: TestClient) -> None:
    response = client.get("/health/live", headers={"X-Request-ID": "health-1"})

    assert response.status_code == 200
    assert response.json() == {"status": "live"}
    assert response.headers["X-Request-ID"] == "health-1"


def test_readiness_distinguishes_ready_and_unavailable(
    client: TestClient, repository: FakeRepository
) -> None:
    ready = client.get("/health/ready")
    assert ready.status_code == 200
    assert ready.json()["status"] == "ready"
    assert ready.json()["checks"]["identity_provider"]["status"] == "warning"
    assert ready.json()["checks"]["serving_repository"]["status"] == "pass"

    repository.ready = False
    response = client.get("/health/ready")
    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"
    assert response.json()["checks"]["serving_repository"]["status"] == "fail"

    repository.ready = RepositoryUnavailable("secret connection detail")
    response = client.get("/health/ready")
    assert response.status_code == 503
    assert "secret" not in response.text


def test_readiness_fails_when_clickhouse_publication_is_stale(
    repository: FakeRepository,
) -> None:
    repository.readiness_override = RepositoryReadiness(
        backend="clickhouse",
        ready=True,
        reason="ready",
        data_version=f"publication-{'a' * 32}",
        data_published_at_utc=datetime(2026, 8, 29, 8, 0, tzinfo=UTC),
        expected_current_row_count=96,
        actual_current_row_count=96,
        expected_history_row_count=558,
        actual_history_row_count=558,
    )
    app = create_app(
        settings=Settings(
            demo_mode=True,
            repository_backend="clickhouse",
            clickhouse_password="unit-test-password",
            maximum_publication_age_seconds=3_600,
        ),
        repository=repository,
        clock=lambda: datetime(2026, 8, 29, 10, 0, tzinfo=UTC),
    )
    response = TestClient(app, raise_server_exceptions=False).get("/health/ready")

    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"
    assert response.json()["publication_age_seconds"] == 7_200
    assert response.json()["checks"]["publication_freshness"] == {
        "status": "fail",
        "message": "The latest serving publication is too old",
    }
    assert response.json()["checks"]["serving_row_counts"]["status"] == "pass"


def test_readiness_reports_fresh_publication_evidence(
    repository: FakeRepository,
) -> None:
    repository.readiness_override = RepositoryReadiness(
        backend="clickhouse",
        ready=True,
        reason="ready",
        data_version=f"publication-{'a' * 32}",
        data_published_at_utc=datetime(2026, 8, 29, 9, 30, tzinfo=UTC),
        expected_current_row_count=96,
        actual_current_row_count=96,
        expected_history_row_count=558,
        actual_history_row_count=558,
    )
    app = create_app(
        settings=Settings(
            demo_mode=True,
            repository_backend="clickhouse",
            clickhouse_password="unit-test-password",
            maximum_publication_age_seconds=3_600,
        ),
        repository=repository,
        clock=lambda: datetime(2026, 8, 29, 10, 0, tzinfo=UTC),
    )
    response = TestClient(app, raise_server_exceptions=False).get("/health/ready")

    assert response.status_code == 200
    assert response.json()["status"] == "ready"
    assert response.json()["publication_age_seconds"] == 1_800
    assert response.json()["expected_current_row_count"] == 96
    assert response.json()["actual_history_row_count"] == 558


def test_readiness_exposes_missing_production_identity_provider(
    repository: FakeRepository,
) -> None:
    app = create_app(settings=Settings(demo_mode=False), repository=repository)
    response = TestClient(app, raise_server_exceptions=False).get("/health/ready")

    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"
    assert response.json()["checks"]["identity_provider"] == {
        "status": "fail",
        "message": "No production identity provider is configured",
    }


def test_process_metrics_are_bounded_and_do_not_contain_business_values(
    client: TestClient,
) -> None:
    client.get("/health/live")
    client.get("/missing-route")
    response = client.get("/health/metrics")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["completed_request_count"] >= 2
    assert payload["response_4xx_count"] >= 1
    assert payload["response_5xx_count"] == 0
    assert payload["in_flight_requests"] == 1
    assert "customer" not in response.text.lower()


def test_container_readiness_deadline_covers_both_bounded_queries() -> None:
    dockerfile = (
        Path(__file__).resolve().parents[2] / "apps" / "api" / "Dockerfile"
    ).read_text(encoding="utf-8")

    assert "--timeout=135s" in dockerfile
    assert '"apps.api.operational_check"' in dockerfile
    assert '"--timeout-seconds", "130"' in dockerfile


def test_compose_enables_daily_publication_age_backstop() -> None:
    compose = (
        Path(__file__).resolve().parents[2] / "infrastructure" / "compose.yaml"
    ).read_text(encoding="utf-8")

    assert (
        "PRODUCT_MAX_PUBLICATION_AGE_SECONDS: "
        "${PRODUCT_MAX_PUBLICATION_AGE_SECONDS:-108000}"
    ) in compose
    assert "condition: service_healthy" in compose


def test_publication_age_setting_rejects_negative_values() -> None:
    with pytest.raises(ValueError, match="maximum_publication_age_seconds"):
        Settings(maximum_publication_age_seconds=-1)


def test_repository_readiness_accepts_queryable_empty_mart() -> None:
    captured: list[tuple[str, list[Any]]] = []

    class EmptyCursor(CapturingCursor):
        def execute(self, sql: str, parameters: list[Any]) -> None:
            self.captured.append((sql, parameters))
            self.description = [("relation_is_queryable",)]
            self.rows = []

    class EmptyConnection(CapturingConnection):
        def cursor(self) -> EmptyCursor:
            return EmptyCursor(self.captured)

    repository = TrinoDeliveryPerformanceRepository(
        Settings(demo_mode=True),
        connection_factory=lambda: EmptyConnection(captured),
    )

    assert repository.is_ready() is True
    assert len(captured) == 2
    assert "fct_steam_delivery_interval" in captured[0][0]
    assert "dim_customer" in captured[1][0]
    assert all("LIMIT 1" in sql for sql, _parameters in captured)


@pytest.mark.parametrize("header", [None, "unknown-persona", ""])
def test_missing_and_unknown_demo_identities_are_rejected(
    client: TestClient, header: str | None
) -> None:
    headers = {} if header is None else {"X-Demo-Actor": header}
    response = client.get("/api/v1/context", headers=headers)

    assert response.status_code == 401
    assert response.json()["detail"]["code"] == "authentication_failed"


def test_demo_identity_adapter_is_opt_in(repository: FakeRepository) -> None:
    app = create_app(settings=Settings(demo_mode=False), repository=repository)
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get(
        "/api/v1/context", headers=actor_headers("commercial-manager")
    )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "identity_provider_unavailable"


@pytest.mark.parametrize(
    "filters",
    [
        "customer_id=CUST-002",
        "site_id=SITE-002",
        "delivery_point_id=DP-002",
    ],
)
def test_customer_persona_cannot_request_another_tenant_collection_scope(
    client: TestClient, repository: FakeRepository, filters: str
) -> None:
    response = client.get(
        query("/api/v1/delivery-performance/summary") + f"&{filters}",
        headers=actor_headers("customer-cust-001"),
    )

    assert response.status_code == 403
    assert response.json()["detail"] == {
        "code": "authorization_denied",
        "message": "The requested scope is not allowed",
    }
    assert repository.calls == []


def test_cross_scope_interval_resource_is_indistinguishable_from_missing(
    client: TestClient,
) -> None:
    response = client.get(
        f"/api/v1/delivery-performance/intervals/{OTHER_INTERVAL_KEY}/history",
        headers=actor_headers("customer-cust-001"),
    )

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "interval_not_found"


def test_context_returns_only_repository_authorized_options(
    client: TestClient,
) -> None:
    response = client.get("/api/v1/context", headers=actor_headers("customer-cust-001"))

    assert response.status_code == 200
    assert response.json() == {
        "identity": {"actor_id": "customer-cust-001", "role": "customer"},
        "customers": [
            {
                "customer_id": "CUST-001",
                "display_name": "Fictional CUST-001",
                "sites": [
                    {
                        "site_id": "SITE-001",
                        "site_name": "Synthetic SITE-001",
                        "delivery_points": [
                            {
                                "delivery_point_id": "DP-001",
                                "delivery_point_name": "Synthetic DP-001",
                            }
                        ],
                    }
                ],
            }
        ],
        "available_reporting_dates": {
            "start": "2026-08-26",
            "end": "2026-08-26",
            "time_zone": "Europe/London",
        },
        "data_version": None,
        "data_published_at_utc": None,
    }


def test_exact_complete_aggregate_uses_governed_numerators(
    client: TestClient,
) -> None:
    response = client.get(
        query("/api/v1/delivery-performance/summary") + "&customer_id=CUST-002",
        headers=actor_headers("commercial-manager"),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["interval_count"] == 48
    assert body["missing_commitment_count"] == 0
    assert body["non_final_financial_count"] == 0
    assert body["commitment_completeness_percent"] == "100.000000"
    assert body["delivery_data_completeness_percent"] == "100.000000"
    assert body["sla_attainment_percent"] == "100.000000"
    assert body["contractual_availability_percent"] == "100.000000"
    assert body["known_gross_earned_revenue_gbp"] == "9984.000000000000"
    assert body["accrued_sla_penalty_gbp"] == "0.000000000000"
    assert body["net_earned_revenue_gbp"] == "9984.000000000000"
    assert body["completeness_status"] == "final"
    assert body["financial_result_status"] == "final"


def test_incomplete_commitments_keep_official_results_null(
    client: TestClient, repository: FakeRepository
) -> None:
    repository.summary_value = provisional_portfolio_summary()

    response = client.get(
        query("/api/v1/delivery-performance/summary"),
        headers=actor_headers(),
    )

    body = response.json()
    assert body["missing_commitment_count"] == 1
    assert body["non_final_financial_count"] == 1
    assert body["commitment_completeness_percent"] == "98.958333"
    assert body["delivery_data_completeness_percent"] is None
    assert body["sla_attainment_percent"] is None
    assert body["contractual_availability_percent"] is None
    assert body["known_gross_earned_revenue_gbp"] == "22399.000000000000"
    assert body["accrued_sla_penalty_gbp"] is None
    assert body["net_earned_revenue_gbp"] is None
    assert body["completeness_status"] == "provisional"
    assert body["financial_result_status"] == "provisional"


def test_missing_measure_is_not_serialized_as_real_zero(client: TestClient) -> None:
    response = client.get(
        query("/api/v1/delivery-performance/intervals"), headers=actor_headers()
    )

    assert response.status_code == 200
    missing, real_zero = response.json()["items"]
    assert missing["committed_mwh_th"] is None
    assert missing["shortfall_mwh_th"] is None
    assert missing["billable_mwh_th"] is None
    assert real_zero["committed_mwh_th"] == "0.000000"
    assert real_zero["shortfall_mwh_th"] == "0.000000"
    assert real_zero["billable_mwh_th"] == "0.000000"


@pytest.mark.parametrize(
    ("row_count", "expected_count", "truncated"),
    [(2, 2, False), (201, 200, True)],
)
def test_history_contract_reports_when_older_revisions_are_omitted(
    client: TestClient,
    repository: FakeRepository,
    row_count: int,
    expected_count: int,
    truncated: bool,
) -> None:
    repository.history_rows = [history_row(index) for index in range(row_count)]

    response = client.get(
        f"/api/v1/delivery-performance/intervals/{INTERVAL_KEY}/history",
        headers=actor_headers(),
    )

    assert response.status_code == 200
    assert len(response.json()["items"]) == expected_count
    assert response.json()["truncated"] is truncated


def test_model_validation_failure_does_not_log_governed_values(
    client: TestClient,
    repository: FakeRepository,
    caplog: pytest.LogCaptureFixture,
) -> None:
    governed_value = "private-governed-customer-value"
    repository.interval_rows = [
        {
            **interval_row(key=INTERVAL_KEY, committed=Decimal("1.000000")),
            "customer_name": {"invalid": governed_value},
        }
    ]

    with caplog.at_level(logging.ERROR, logger="historical_delivery_api"):
        response = client.get(
            query("/api/v1/delivery-performance/intervals"),
            headers=actor_headers(),
        )

    assert response.status_code == 500
    assert response.json()["detail"]["code"] == "internal_error"
    assert governed_value not in caplog.text
    assert governed_value not in response.text
    assert "ValidationError" in caplog.records[-1].error_type


def test_date_and_pagination_inputs_are_bounded(client: TestClient) -> None:
    too_long = client.get(
        "/api/v1/delivery-performance/summary"
        "?start_date=2026-08-01&end_date=2026-09-01",
        headers=actor_headers(),
    )
    too_large_page = client.get(
        query("/api/v1/delivery-performance/intervals") + "&page=100001",
        headers=actor_headers(),
    )
    too_large_limit = client.get(
        query("/api/v1/delivery-performance/intervals") + "&limit=201",
        headers=actor_headers(),
    )

    assert too_long.status_code == 422
    assert too_large_page.status_code == 422
    assert too_large_limit.status_code == 422


def test_reporting_dates_reach_repository_without_utc_midnight_conversion(
    client: TestClient, repository: FakeRepository
) -> None:
    response = client.get(
        query("/api/v1/delivery-performance/summary"), headers=actor_headers()
    )

    assert response.status_code == 200
    scope = repository.calls[-1][2]
    assert scope.start_date == date(2026, 8, 26)
    assert scope.end_date == date(2026, 8, 26)


def test_request_audit_log_contains_safe_scope_and_request_id(
    client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    LOGGER.addHandler(caplog.handler)
    try:
        response = client.get(
            query("/api/v1/delivery-performance/summary") + "&customer_id=CUST-001",
            headers={
                **actor_headers("customer-cust-001"),
                "X-Request-ID": "audit-7",
            },
        )
    finally:
        LOGGER.removeHandler(caplog.handler)

    event = json.loads(caplog.records[-1].message)
    assert response.headers["X-Request-ID"] == "audit-7"
    assert event["request_id"] == "audit-7"
    assert event["actor_id"] == "customer-cust-001"
    assert event["authorized_tenant_scope"] == ["TENANT-CUST-001"]
    assert event["customer_scope"] == "CUST-001"
    assert event["status"] == 200
    assert "9984" not in caplog.text


def test_runtime_audit_logger_emits_info_without_root_logger_configuration(
    client: TestClient,
) -> None:
    assert LOGGER.getEffectiveLevel() == logging.INFO
    assert LOGGER.propagate is False
    handlers = [
        handler
        for handler in LOGGER.handlers
        if handler.get_name() == AUDIT_HANDLER_NAME
    ]
    assert len(handlers) == 1
    assert handlers[0].level == logging.INFO
    assert handlers[0].formatter is not None
    assert handlers[0].formatter._fmt == "%(message)s"


class CapturingCursor:
    def __init__(self, captured: list[tuple[str, list[Any]]]) -> None:
        self.captured = captured
        self.description: list[tuple[str]] = []
        self.rows: list[tuple[Any, ...]] = []

    def execute(self, sql: str, parameters: list[Any]) -> None:
        self.captured.append((sql, parameters))
        values = complete_summary()
        row = {
            "interval_count": values.interval_count,
            "expected_interval_count": values.expected_interval_count,
            "commitment_record_count": values.commitment_record_count,
            "missing_commitment_count": values.missing_commitment_count,
            "applicable_interval_count": values.applicable_interval_count,
            "accepted_applicable_delivery_count": (
                values.accepted_applicable_delivery_count
            ),
            "final_applicable_capacity_count": values.final_applicable_capacity_count,
            "known_delivered_mwh_th": values.known_delivered_mwh_th,
            "committed_mwh_th": values.committed_mwh_th,
            "known_shortfall_mwh_th": values.known_shortfall_mwh_th,
            "known_excess_mwh_th": values.known_excess_mwh_th,
            "known_billable_mwh_th": values.known_billable_mwh_th,
            "sla_attainment_numerator_mwh_th": (values.sla_attainment_numerator_mwh_th),
            "contractual_availability_numerator_mwh_th": (
                values.contractual_availability_numerator_mwh_th
            ),
            "known_gross_earned_revenue_gbp": (values.known_gross_earned_revenue_gbp),
            "known_accrued_sla_penalty_gbp": (values.known_accrued_sla_penalty_gbp),
            "non_final_financial_count": values.non_final_financial_count,
            "distinct_currency_count": values.distinct_currency_count,
            "currency_code": values.currency_code,
            "latest_coverage_published_at_utc": (
                values.latest_coverage_published_at_utc
            ),
        }
        self.description = [(name,) for name in row]
        self.rows = [tuple(row.values())]

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def close(self) -> None:
        pass


class CapturingConnection:
    def __init__(self, captured: list[tuple[str, list[Any]]]) -> None:
        self.captured = captured

    def cursor(self) -> CapturingCursor:
        return CapturingCursor(self.captured)

    def close(self) -> None:
        pass


class IntervalPageCursor(CapturingCursor):
    def execute(self, sql: str, parameters: list[Any]) -> None:
        self.captured.append((sql, parameters))
        row = {"total_count": 48, **interval_row(key=INTERVAL_KEY, committed=None)}
        self.description = [(name,) for name in row]
        self.rows = [tuple(row.values())]


class IntervalPageConnection(CapturingConnection):
    def cursor(self) -> IntervalPageCursor:
        return IntervalPageCursor(self.captured)


def test_trino_query_parameterizes_scope_and_enforces_tenant_join() -> None:
    captured: list[tuple[str, list[Any]]] = []
    repository = TrinoDeliveryPerformanceRepository(
        Settings(demo_mode=True),
        connection_factory=lambda: CapturingConnection(captured),
    )
    hostile_customer = "CUST-001' OR 1=1 --"

    repository.get_summary(
        DEMO_ACTORS["customer-cust-001"],
        QueryScope(
            start_date=date(2026, 8, 26),
            end_date=date(2026, 8, 26),
            customer_id=hostile_customer,
        ),
    )

    sql, parameters = captured[0]
    assert hostile_customer not in sql
    assert "TENANT-CUST-001" not in sql
    assert hostile_customer in parameters
    assert "TENANT-CUST-001" in parameters
    assert "f.customer_key = c.customer_key" in sql
    assert "c.tenant_authorization_scope_id IN (?)" in sql
    assert "f.customer_access_status = ?" in sql
    assert "f.date_key >= ?" in sql and "f.date_key <= ?" in sql
    assert 20260826 in parameters


def test_history_query_authorizes_revision_scope_before_key_and_as_of() -> None:
    captured: list[tuple[str, list[Any]]] = []
    repository = TrinoDeliveryPerformanceRepository(
        Settings(demo_mode=True),
        connection_factory=lambda: CapturingConnection(captured),
    )
    cutoff = datetime(2026, 8, 27, 9, 30, tzinfo=UTC)

    repository.get_interval_history(
        DEMO_ACTORS["customer-cust-001"],
        INTERVAL_KEY,
        as_of=cutoff,
    )

    sql, parameters = captured[0]
    assert "dim_customer_revision_audit" in sql
    assert "dim_site_revision_audit" in sql
    assert "dim_delivery_point_revision_audit" in sql
    assert "h.customer_revision_key = c.customer_revision_key" in sql
    assert "h.customer_access_status = ?" in sql
    assert "c.tenant_authorization_scope_id IN (?)" in sql
    assert sql.index("c.tenant_authorization_scope_id IN (?)") < sql.index(
        "h.delivery_interval_key = ?"
    )
    assert "h.known_from_utc <= ?" in sql
    assert "(h.known_to_utc IS NULL OR ? < h.known_to_utc)" in sql
    assert INTERVAL_KEY not in sql
    assert "TENANT-CUST-001" not in sql
    assert parameters[-3:] == [INTERVAL_KEY, cutoff, cutoff]
    assert "LIMIT 201" in sql


def test_normal_interval_page_gets_total_without_a_second_fact_scan() -> None:
    captured: list[tuple[str, list[Any]]] = []
    repository = TrinoDeliveryPerformanceRepository(
        Settings(demo_mode=True),
        connection_factory=lambda: IntervalPageConnection(captured),
    )

    rows, total = repository.get_intervals(
        DEMO_ACTORS["customer-cust-001"],
        QueryScope(
            start_date=date(2026, 8, 26),
            end_date=date(2026, 8, 26),
            customer_id="CUST-001",
        ),
        page=1,
        limit=25,
    )

    assert len(captured) == 1
    assert "count(*) OVER () AS total_count" in captured[0][0]
    assert total == 48
    assert rows[0]["interval_key"] == INTERVAL_KEY
    assert "total_count" not in rows[0]


def test_configured_catalog_and_schema_are_identifier_validated() -> None:
    with pytest.raises(ValueError, match="trino_catalog"):
        Settings(trino_catalog='r2".raw')


def test_trino_connection_has_server_side_total_query_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    sentinel = object()

    def fake_connect(**kwargs: Any) -> object:
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr("trino.dbapi.connect", fake_connect)
    repository = TrinoDeliveryPerformanceRepository(
        Settings(
            demo_mode=True,
            trino_timeout_seconds=17,
            trino_query_timeout_seconds=43,
        )
    )

    assert repository._connect() is sentinel
    assert captured["request_timeout"] == 17
    assert captured["session_properties"] == {
        "query_max_run_time": "43s",
        "query_max_execution_time": "43s",
    }


def test_failed_trino_query_is_explicitly_cancelled() -> None:
    cancelled = False

    class FailingCursor:
        description: ClassVar[list[tuple[str]]] = []

        def execute(self, _sql: str, _parameters: list[Any]) -> None:
            raise TimeoutError("query deadline exceeded")

        def cancel(self) -> None:
            nonlocal cancelled
            cancelled = True

        def close(self) -> None:
            pass

    class FailingConnection:
        def cursor(self) -> FailingCursor:
            return FailingCursor()

        def close(self) -> None:
            pass

    repository = TrinoDeliveryPerformanceRepository(
        Settings(demo_mode=True),
        connection_factory=FailingConnection,
    )

    with pytest.raises(RepositoryUnavailable):
        repository._fetch_all("SELECT 1", [])

    assert cancelled is True
