"""Explicit HTTP response contracts for the historical delivery product."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, field_serializer


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include a UTC offset")
    return value.astimezone(UTC)


UtcDatetime = Annotated[datetime, AfterValidator(_as_utc)]
AggregateResultStatus = Literal["final", "provisional", "not_applicable", "no_data"]


class ResponseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class HealthResponse(ResponseModel):
    status: Literal["live", "ready"]


class OperationalCheckResponse(ResponseModel):
    status: Literal["pass", "fail", "warning", "disabled"]
    message: str


class ReadinessResponse(ResponseModel):
    status: Literal["ready", "not_ready"]
    checked_at_utc: UtcDatetime
    repository_backend: Literal["clickhouse", "trino"]
    checks: dict[str, OperationalCheckResponse]
    data_version: str | None
    data_published_at_utc: UtcDatetime | None
    publication_age_seconds: int | None = Field(default=None, ge=0)
    maximum_publication_age_seconds: int | None = Field(default=None, ge=1)
    expected_current_row_count: int | None = Field(default=None, ge=0)
    actual_current_row_count: int | None = Field(default=None, ge=0)
    expected_history_row_count: int | None = Field(default=None, ge=0)
    actual_history_row_count: int | None = Field(default=None, ge=0)


class OperationalMetricsResponse(ResponseModel):
    status: Literal["ok"] = "ok"
    process_started_at_utc: UtcDatetime
    uptime_seconds: int = Field(ge=0)
    in_flight_requests: int = Field(ge=0)
    completed_request_count: int = Field(ge=0)
    response_4xx_count: int = Field(ge=0)
    response_5xx_count: int = Field(ge=0)
    average_duration_ms: float = Field(ge=0)
    maximum_duration_ms: float = Field(ge=0)


class ActorResponse(ResponseModel):
    actor_id: str
    role: Literal["commercial_manager", "customer"]


class DeliveryPointOption(ResponseModel):
    delivery_point_id: str
    delivery_point_name: str


class SiteOption(ResponseModel):
    site_id: str
    site_name: str
    delivery_points: list[DeliveryPointOption]


class CustomerOption(ResponseModel):
    customer_id: str
    display_name: str
    sites: list[SiteOption]


class AvailableReportingDates(ResponseModel):
    start: date
    end: date
    time_zone: Literal["Europe/London"] = "Europe/London"


class ProductContextResponse(ResponseModel):
    identity: ActorResponse
    customers: list[CustomerOption]
    available_reporting_dates: AvailableReportingDates | None
    data_version: str | None
    data_published_at_utc: UtcDatetime | None


class QueryScopeResponse(ResponseModel):
    start_date: date
    end_date: date
    customer_id: str | None
    site_id: str | None
    delivery_point_id: str | None
    status: (
        Literal["final", "provisional", "missing", "corrected", "shortfall", "excess"]
        | None
    )


class FinancialLabels(ResponseModel):
    gross_amount: Literal["earned_revenue", "projected_service_charge"]
    deduction: Literal["sla_penalty", "projected_sla_credit"]
    net_amount: Literal["net_earned_revenue", "projected_net_service_charge"]


class DeliveryPerformanceSummaryResponse(ResponseModel):
    scope: QueryScopeResponse
    interval_count: int = Field(ge=0)
    expected_interval_count: int = Field(ge=0)
    commitment_record_count: int = Field(ge=0)
    missing_commitment_count: int = Field(ge=0)
    commitment_completeness_percent: Decimal | None
    applicable_interval_count: int = Field(ge=0)
    accepted_applicable_delivery_count: int = Field(ge=0)
    final_applicable_capacity_count: int = Field(ge=0)
    non_final_financial_count: int = Field(ge=0)
    completeness_status: AggregateResultStatus
    delivery_data_completeness_percent: Decimal | None
    known_delivered_mwh_th: Decimal | None
    committed_mwh_th: Decimal | None
    known_shortfall_mwh_th: Decimal | None
    known_excess_mwh_th: Decimal | None
    known_billable_mwh_th: Decimal | None
    sla_attainment_percent: Decimal | None
    sla_result_status: AggregateResultStatus
    contractual_availability_percent: Decimal | None
    availability_result_status: AggregateResultStatus
    known_gross_earned_revenue_gbp: Decimal | None
    accrued_sla_penalty_gbp: Decimal | None
    net_earned_revenue_gbp: Decimal | None
    financial_result_status: Literal["final", "provisional", "no_data"]
    currency_code: str | None
    latest_coverage_published_at_utc: UtcDatetime | None
    financial_labels: FinancialLabels

    @field_serializer(
        "commitment_completeness_percent",
        "delivery_data_completeness_percent",
        "known_delivered_mwh_th",
        "committed_mwh_th",
        "known_shortfall_mwh_th",
        "known_excess_mwh_th",
        "known_billable_mwh_th",
        "sla_attainment_percent",
        "contractual_availability_percent",
        "known_gross_earned_revenue_gbp",
        "accrued_sla_penalty_gbp",
        "net_earned_revenue_gbp",
        when_used="json",
    )
    def serialize_decimal(self, value: Decimal | None) -> str | None:
        return None if value is None else format(value, "f")


class DeliveryIntervalResponse(ResponseModel):
    interval_key: str
    customer_id: str
    customer_name: str
    site_id: str
    site_name: str
    delivery_point_id: str
    delivery_point_name: str
    reporting_date: date
    local_period_number: int = Field(ge=1, le=50)
    interval_start_at: UtcDatetime
    interval_end_at: UtcDatetime
    interval_start_local: datetime
    interval_end_local: datetime
    operating_timezone: str
    utc_offset_minutes: int
    is_daylight_saving_time: bool
    committed_mwh_th: Decimal | None
    delivered_mwh_th: Decimal | None
    shortfall_mwh_th: Decimal | None
    excess_mwh_th: Decimal | None
    deliverable_capacity_mwh_th: Decimal | None
    billable_mwh_th: Decimal | None
    gross_earned_revenue_gbp: Decimal | None
    accrued_sla_penalty_gbp: Decimal | None
    net_earned_revenue_gbp: Decimal | None
    currency_code: str | None
    delivery_measurement_status: str
    commitment_status: str
    capacity_status: str
    sla_result_status: str
    availability_result_status: str
    financial_result_status: str
    correction_status: str

    @field_serializer(
        "committed_mwh_th",
        "delivered_mwh_th",
        "shortfall_mwh_th",
        "excess_mwh_th",
        "deliverable_capacity_mwh_th",
        "billable_mwh_th",
        "gross_earned_revenue_gbp",
        "accrued_sla_penalty_gbp",
        "net_earned_revenue_gbp",
        when_used="json",
    )
    def serialize_decimal(self, value: Decimal | None) -> str | None:
        return None if value is None else format(value, "f")


class DeliveryIntervalsPageResponse(ResponseModel):
    items: list[DeliveryIntervalResponse]
    page: int = Field(ge=1)
    limit: int = Field(ge=1, le=200)
    total: int = Field(ge=0)


class DeliveryIntervalHistoryItemResponse(DeliveryIntervalResponse):
    history_key: str
    known_from_at: UtcDatetime
    known_to_at: UtcDatetime | None
    is_current_knowledge_state: bool


class DeliveryIntervalHistoryResponse(ResponseModel):
    interval_key: str
    items: list[DeliveryIntervalHistoryItemResponse]
    truncated: bool


class ErrorBody(ResponseModel):
    code: str
    message: str


class ErrorResponse(ResponseModel):
    detail: ErrorBody
