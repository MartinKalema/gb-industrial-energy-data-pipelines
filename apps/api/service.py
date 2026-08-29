"""Business-safe presentation of governed mart values."""

from __future__ import annotations

from collections import OrderedDict
from datetime import datetime
from decimal import Decimal
from typing import Any

from apps.api.auth import Actor
from apps.api.models import (
    ActorResponse,
    AvailableReportingDates,
    CustomerOption,
    DeliveryIntervalHistoryItemResponse,
    DeliveryIntervalHistoryResponse,
    DeliveryIntervalResponse,
    DeliveryIntervalsPageResponse,
    DeliveryPerformanceSummaryResponse,
    DeliveryPointOption,
    FinancialLabels,
    ProductContextResponse,
    QueryScopeResponse,
    SiteOption,
)
from apps.api.repository import (
    DeliveryPerformanceRepository,
    HISTORY_RESPONSE_LIMIT,
    MartIntegrityError,
    QueryScope,
    SummaryAggregate,
)

PERCENT_QUANTUM = Decimal("0.000001")


def _percent(numerator: Decimal | int, denominator: Decimal | int) -> Decimal:
    return (Decimal(100) * Decimal(numerator) / Decimal(denominator)).quantize(
        PERCENT_QUANTUM
    )


def _financial_labels(actor: Actor) -> FinancialLabels:
    if actor.role == "customer":
        return FinancialLabels(
            gross_amount="projected_service_charge",
            deduction="projected_sla_credit",
            net_amount="projected_net_service_charge",
        )
    return FinancialLabels(
        gross_amount="earned_revenue",
        deduction="sla_penalty",
        net_amount="net_earned_revenue",
    )


class DeliveryPerformanceService:
    def __init__(self, repository: DeliveryPerformanceRepository) -> None:
        self._repository = repository

    def context(self, actor: Actor) -> ProductContextResponse:
        rows = self._repository.get_context(actor)
        customers: OrderedDict[str, dict[str, Any]] = OrderedDict()
        for row in rows:
            customer = customers.setdefault(
                row.customer_id,
                {
                    "customer_id": row.customer_id,
                    "display_name": row.customer_name,
                    "sites": OrderedDict(),
                },
            )
            sites: OrderedDict[str, dict[str, Any]] = customer["sites"]
            site = sites.setdefault(
                row.site_id,
                {
                    "site_id": row.site_id,
                    "site_name": row.site_name,
                    "delivery_points": [],
                },
            )
            point = DeliveryPointOption(
                delivery_point_id=row.delivery_point_id,
                delivery_point_name=row.delivery_point_name,
            )
            if point not in site["delivery_points"]:
                site["delivery_points"].append(point)

        customer_models = [
            CustomerOption(
                customer_id=customer["customer_id"],
                display_name=customer["display_name"],
                sites=[SiteOption(**site) for site in customer["sites"].values()],
            )
            for customer in customers.values()
        ]
        available_dates = None
        if rows:
            available_dates = AvailableReportingDates(
                start=min(row.minimum_reporting_date for row in rows),
                end=max(row.maximum_reporting_date for row in rows),
            )
        return ProductContextResponse(
            identity=ActorResponse(actor_id=actor.actor_id, role=actor.role),
            customers=customer_models,
            available_reporting_dates=available_dates,
        )

    def summary(
        self, actor: Actor, scope: QueryScope
    ) -> DeliveryPerformanceSummaryResponse:
        aggregate = self._repository.get_summary(actor, scope)
        return build_summary_response(actor, scope, aggregate)

    def intervals(
        self,
        actor: Actor,
        scope: QueryScope,
        *,
        page: int,
        limit: int,
    ) -> DeliveryIntervalsPageResponse:
        rows, total = self._repository.get_intervals(
            actor, scope, page=page, limit=limit
        )
        return DeliveryIntervalsPageResponse(
            items=[DeliveryIntervalResponse(**row) for row in rows],
            page=page,
            limit=limit,
            total=total,
        )

    def interval_history(
        self, actor: Actor, interval_key: str, *, as_of: datetime | None = None
    ) -> DeliveryIntervalHistoryResponse | None:
        rows = self._repository.get_interval_history(actor, interval_key, as_of=as_of)
        if not rows:
            return None
        return DeliveryIntervalHistoryResponse(
            interval_key=interval_key,
            items=[
                DeliveryIntervalHistoryItemResponse(**row)
                for row in rows[:HISTORY_RESPONSE_LIMIT]
            ],
            truncated=len(rows) > HISTORY_RESPONSE_LIMIT,
        )


def build_summary_response(
    actor: Actor,
    scope: QueryScope,
    aggregate: SummaryAggregate,
) -> DeliveryPerformanceSummaryResponse:
    """Apply accepted completeness gates to stored governed numerators."""

    if aggregate.distinct_currency_count > 1:
        raise MartIntegrityError("A summary scope contains multiple currencies")

    no_data = aggregate.interval_count == 0
    commitments_complete = (
        not no_data
        and aggregate.commitment_record_count == aggregate.expected_interval_count
        and aggregate.missing_commitment_count == 0
    )
    applicable = aggregate.applicable_interval_count
    delivery_final = (
        commitments_complete
        and aggregate.accepted_applicable_delivery_count == applicable
    )
    availability_final = (
        commitments_complete and aggregate.final_applicable_capacity_count == applicable
    )
    financial_final = delivery_final and aggregate.non_final_financial_count == 0

    if no_data:
        completeness_status = "no_data"
        sla_status = "no_data"
        availability_status = "no_data"
        financial_status = "no_data"
    elif commitments_complete and applicable == 0:
        completeness_status = "not_applicable"
        sla_status = "not_applicable"
        availability_status = "not_applicable"
        financial_status = "final" if financial_final else "provisional"
    else:
        completeness_status = "final" if delivery_final else "provisional"
        sla_status = "final" if delivery_final else "provisional"
        availability_status = "final" if availability_final else "provisional"
        financial_status = "final" if financial_final else "provisional"

    commitment_completeness_percent = None
    if aggregate.expected_interval_count > 0:
        commitment_completeness_percent = _percent(
            aggregate.commitment_record_count, aggregate.expected_interval_count
        )

    delivery_completeness_percent = None
    if commitments_complete and applicable > 0:
        delivery_completeness_percent = _percent(
            aggregate.accepted_applicable_delivery_count, applicable
        )

    sla_attainment_percent = None
    if delivery_final and applicable > 0:
        if (
            aggregate.sla_attainment_numerator_mwh_th is None
            or aggregate.committed_mwh_th is None
            or aggregate.committed_mwh_th <= 0
        ):
            raise MartIntegrityError("Final SLA aggregate is missing a governed value")
        sla_attainment_percent = _percent(
            aggregate.sla_attainment_numerator_mwh_th,
            aggregate.committed_mwh_th,
        )

    availability_percent = None
    if availability_final and applicable > 0:
        if (
            aggregate.contractual_availability_numerator_mwh_th is None
            or aggregate.committed_mwh_th is None
            or aggregate.committed_mwh_th <= 0
        ):
            raise MartIntegrityError(
                "Final availability aggregate is missing a governed value"
            )
        availability_percent = _percent(
            aggregate.contractual_availability_numerator_mwh_th,
            aggregate.committed_mwh_th,
        )

    official_penalty = None
    official_net = None
    if financial_final:
        if (
            aggregate.known_gross_earned_revenue_gbp is None
            or aggregate.known_accrued_sla_penalty_gbp is None
        ):
            raise MartIntegrityError(
                "Final financial aggregate is missing a governed value"
            )
        official_penalty = aggregate.known_accrued_sla_penalty_gbp
        official_net = aggregate.known_gross_earned_revenue_gbp - official_penalty

    return DeliveryPerformanceSummaryResponse(
        scope=QueryScopeResponse(
            start_date=scope.start_date,
            end_date=scope.end_date,
            customer_id=scope.customer_id,
            site_id=scope.site_id,
            delivery_point_id=scope.delivery_point_id,
            status=scope.status,
        ),
        interval_count=aggregate.interval_count,
        expected_interval_count=aggregate.expected_interval_count,
        commitment_record_count=aggregate.commitment_record_count,
        missing_commitment_count=aggregate.missing_commitment_count,
        commitment_completeness_percent=commitment_completeness_percent,
        applicable_interval_count=applicable,
        accepted_applicable_delivery_count=(
            aggregate.accepted_applicable_delivery_count
        ),
        final_applicable_capacity_count=aggregate.final_applicable_capacity_count,
        non_final_financial_count=aggregate.non_final_financial_count,
        completeness_status=completeness_status,
        delivery_data_completeness_percent=delivery_completeness_percent,
        known_delivered_mwh_th=aggregate.known_delivered_mwh_th,
        committed_mwh_th=aggregate.committed_mwh_th,
        known_shortfall_mwh_th=aggregate.known_shortfall_mwh_th,
        known_excess_mwh_th=aggregate.known_excess_mwh_th,
        known_billable_mwh_th=aggregate.known_billable_mwh_th,
        sla_attainment_percent=sla_attainment_percent,
        sla_result_status=sla_status,
        contractual_availability_percent=availability_percent,
        availability_result_status=availability_status,
        known_gross_earned_revenue_gbp=(aggregate.known_gross_earned_revenue_gbp),
        accrued_sla_penalty_gbp=official_penalty,
        net_earned_revenue_gbp=official_net,
        financial_result_status=financial_status,
        currency_code=aggregate.currency_code,
        latest_coverage_published_at_utc=(aggregate.latest_coverage_published_at_utc),
        financial_labels=_financial_labels(actor),
    )
