"""FastAPI boundary for governed historical steam-delivery performance."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, date, datetime
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from apps.api.auth import (
    Actor,
    AuthenticationFailed,
    AuthorizationDenied,
    DemoIdentityAdapter,
    IdentityProviderUnavailable,
    authorize_filters,
)
from apps.api.clickhouse_repository import ClickHouseDeliveryPerformanceRepository
from apps.api.models import (
    DeliveryIntervalHistoryResponse,
    DeliveryIntervalsPageResponse,
    DeliveryPerformanceSummaryResponse,
    ErrorResponse,
    HealthResponse,
    OperationalCheckResponse,
    OperationalMetricsResponse,
    ProductContextResponse,
    ReadinessResponse,
)
from apps.api.repository import (
    DataVersionUnavailable,
    DeliveryPerformanceRepository,
    MartIntegrityError,
    QueryScope,
    RepositoryReadiness,
    RepositoryUnavailable,
    TrinoDeliveryPerformanceRepository,
)
from apps.api.request_logging import (
    RequestAuditMiddleware,
    RequestMetrics,
    configure_request_audit_logging,
)
from apps.api.service import DeliveryPerformanceService
from apps.api.settings import Settings

StatusFilter = Literal[
    "final", "provisional", "missing", "corrected", "shortfall", "excess"
]
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
INTERVAL_KEY_PATTERN = r"^[a-f0-9]{64}$"
DATA_VERSION_PATTERN = re.compile(r"^publication-[a-f0-9]{32}$")


def _error(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"detail": {"code": code, "message": message}},
    )


def _actor(
    request: Request,
    x_demo_actor: Annotated[str | None, Header(alias="X-Demo-Actor")] = None,
) -> Actor:
    actor = request.app.state.identity_adapter.authenticate(x_demo_actor)
    request.state.actor_id = actor.actor_id
    request.state.authorized_tenant_scope = (
        "portfolio"
        if actor.tenant_scope_ids is None
        else sorted(actor.tenant_scope_ids)
    )
    return actor


def _validated_identifier(value: str | None, field_name: str) -> str | None:
    if value is not None and IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "invalid_filter",
                "message": f"{field_name} has an invalid format",
            },
        )
    return value


def _scope(
    request: Request,
    actor: Annotated[Actor, Depends(_actor)],
    start_date: Annotated[date, Query(description="Inclusive Europe/London date")],
    end_date: Annotated[date, Query(description="Inclusive Europe/London date")],
    customer_id: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
    site_id: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
    delivery_point_id: Annotated[
        str | None, Query(min_length=1, max_length=128)
    ] = None,
    status: Annotated[StatusFilter | None, Query()] = None,
) -> QueryScope:
    if end_date < start_date:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "invalid_date_range",
                "message": "end_date must be on or after start_date",
            },
        )
    inclusive_days = (end_date - start_date).days + 1
    if inclusive_days > request.app.state.settings.maximum_query_days:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "date_range_too_large",
                "message": (
                    "The inclusive reporting-date range exceeds the configured limit"
                ),
            },
        )
    customer_id = _validated_identifier(customer_id, "customer_id")
    site_id = _validated_identifier(site_id, "site_id")
    delivery_point_id = _validated_identifier(delivery_point_id, "delivery_point_id")
    request.state.customer_scope = customer_id
    request.state.site_scope = site_id
    authorize_filters(
        actor,
        customer_id=customer_id,
        site_id=site_id,
        delivery_point_id=delivery_point_id,
    )
    return QueryScope(
        start_date=start_date,
        end_date=end_date,
        customer_id=customer_id,
        site_id=site_id,
        delivery_point_id=delivery_point_id,
        status=status,
    )


def _aware_as_of(as_of: datetime | None) -> datetime | None:
    if as_of is None:
        return None
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "invalid_as_of",
                "message": "as_of must include a UTC offset",
            },
        )
    return as_of.astimezone(UTC)


def _data_version(
    x_product_data_version: Annotated[
        str | None, Header(alias="X-Product-Data-Version", max_length=128)
    ] = None,
) -> str | None:
    if x_product_data_version is None:
        return None
    value = x_product_data_version.strip()
    if DATA_VERSION_PATTERN.fullmatch(value) is None:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "invalid_data_version",
                "message": "X-Product-Data-Version has an invalid format",
            },
        )
    return value


def create_app(
    *,
    settings: Settings | None = None,
    repository: DeliveryPerformanceRepository | None = None,
    clock: Callable[[], datetime] | None = None,
) -> FastAPI:
    settings = settings or Settings.from_environment()
    if repository is None:
        if settings.repository_backend == "clickhouse":
            repository = ClickHouseDeliveryPerformanceRepository(settings)
        else:
            repository = TrinoDeliveryPerformanceRepository(settings)

    app = FastAPI(
        title="Historical Steam Delivery Performance API",
        version="1.0.0",
        docs_url="/docs",
        redoc_url=None,
    )
    app.state.settings = settings
    app.state.repository = repository
    app.state.service = DeliveryPerformanceService(repository)
    app.state.identity_adapter = DemoIdentityAdapter(enabled=settings.demo_mode)
    app.state.clock = clock or (lambda: datetime.now(UTC))
    app.state.request_metrics = RequestMetrics()
    configure_request_audit_logging()
    app.add_middleware(RequestAuditMiddleware, metrics=app.state.request_metrics)

    @app.exception_handler(AuthenticationFailed)
    async def authentication_error(
        _request: Request, _error_value: AuthenticationFailed
    ) -> JSONResponse:
        return _error(401, "authentication_failed", "A valid demo actor is required")

    @app.exception_handler(IdentityProviderUnavailable)
    async def identity_provider_error(
        _request: Request, _error_value: IdentityProviderUnavailable
    ) -> JSONResponse:
        return _error(
            503,
            "identity_provider_unavailable",
            "No identity provider is configured",
        )

    @app.exception_handler(AuthorizationDenied)
    async def authorization_error(
        _request: Request, _error_value: AuthorizationDenied
    ) -> JSONResponse:
        return _error(403, "authorization_denied", "The requested scope is not allowed")

    @app.exception_handler(RepositoryUnavailable)
    async def repository_error(
        _request: Request, _error_value: RepositoryUnavailable
    ) -> JSONResponse:
        return _error(
            503,
            "mart_unavailable",
            "Historical delivery data is temporarily unavailable",
        )

    @app.exception_handler(MartIntegrityError)
    async def integrity_error(
        _request: Request, _error_value: MartIntegrityError
    ) -> JSONResponse:
        return _error(
            500,
            "mart_integrity_error",
            "Historical delivery data failed an integrity check",
        )

    @app.exception_handler(DataVersionUnavailable)
    async def data_version_error(
        _request: Request, _error_value: DataVersionUnavailable
    ) -> JSONResponse:
        return _error(
            409,
            "data_version_unavailable",
            "The requested product data version is not available",
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(
        _request: Request, _error_value: RequestValidationError
    ) -> JSONResponse:
        return _error(422, "invalid_request", "The request is not valid")

    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, error: Exception) -> JSONResponse:
        # Do not attach the exception or traceback: validation failures can
        # embed governed field values in their string representation. The
        # request ID and exception class are enough to correlate safe telemetry.
        logging.getLogger("historical_delivery_api").error(
            "Unhandled request error",
            extra={
                "request_id": getattr(request.state, "request_id", None),
                "error_type": type(error).__name__,
            },
        )
        return _error(500, "internal_error", "The request could not be completed")

    @app.get("/health/live", response_model=HealthResponse)
    def health_live() -> HealthResponse:
        return HealthResponse(status="live")

    @app.get(
        "/health/ready",
        response_model=ReadinessResponse,
        responses={503: {"model": ReadinessResponse}},
    )
    def health_ready() -> ReadinessResponse | JSONResponse:
        checked_at = app.state.clock()
        if checked_at.tzinfo is None or checked_at.utcoffset() is None:
            raise RuntimeError("The readiness clock must be timezone-aware")
        checked_at = checked_at.astimezone(UTC)
        try:
            repository_readiness = repository.get_readiness()
        except (MartIntegrityError, RepositoryUnavailable):
            repository_readiness = RepositoryReadiness(
                backend=settings.repository_backend,
                ready=False,
                reason="repository_unavailable",
            )

        if settings.demo_mode:
            identity_check = OperationalCheckResponse(
                status="warning",
                message=(
                    "The local demo identity adapter is enabled; it is not "
                    "production authentication"
                ),
            )
            identity_ready = True
        else:
            identity_check = OperationalCheckResponse(
                status="fail",
                message="No production identity provider is configured",
            )
            identity_ready = False

        repository_failed = repository_readiness.reason in {
            "repository_unavailable",
            "integrity_check_failed",
        }
        repository_check = OperationalCheckResponse(
            status="fail" if repository_failed else "pass",
            message=(
                "The serving repository could not complete its bounded checks"
                if repository_failed
                else "The serving repository completed its bounded checks"
            ),
        )

        has_count_evidence = (
            repository_readiness.expected_current_row_count is not None
            and repository_readiness.actual_current_row_count is not None
            and repository_readiness.expected_history_row_count is not None
            and repository_readiness.actual_history_row_count is not None
        )
        if not has_count_evidence:
            row_count_check = OperationalCheckResponse(
                status="disabled",
                message="This repository backend does not publish serving-copy counts",
            )
        elif repository_readiness.reason == "row_count_mismatch":
            row_count_check = OperationalCheckResponse(
                status="fail",
                message="Published and stored serving row counts do not match",
            )
        else:
            row_count_check = OperationalCheckResponse(
                status="pass",
                message="Published and stored serving row counts match",
            )

        published_at = repository_readiness.data_published_at_utc
        publication_age_seconds: int | None = None
        freshness_ready = True
        if published_at is not None and (
            published_at.tzinfo is None or published_at.utcoffset() is None
        ):
            freshness_check = OperationalCheckResponse(
                status="fail",
                message="The publication timestamp is not timezone-aware",
            )
            freshness_ready = False
            published_at = None
        elif settings.maximum_publication_age_seconds == 0:
            freshness_check = OperationalCheckResponse(
                status="disabled",
                message="No maximum publication age is configured",
            )
        elif published_at is None:
            freshness_check = OperationalCheckResponse(
                status="fail",
                message="Publication freshness cannot be proved",
            )
            freshness_ready = False
        else:
            published_at = published_at.astimezone(UTC)
            publication_age_seconds = int((checked_at - published_at).total_seconds())
            if publication_age_seconds < 0:
                publication_age_seconds = 0
                freshness_check = OperationalCheckResponse(
                    status="fail",
                    message="The publication timestamp is in the future",
                )
                freshness_ready = False
            elif (
                publication_age_seconds > settings.maximum_publication_age_seconds
            ):
                freshness_check = OperationalCheckResponse(
                    status="fail",
                    message="The latest serving publication is too old",
                )
                freshness_ready = False
            else:
                freshness_check = OperationalCheckResponse(
                    status="pass",
                    message="The latest serving publication is within the age limit",
                )

        ready = repository_readiness.ready and freshness_ready and identity_ready
        response = ReadinessResponse(
            status="ready" if ready else "not_ready",
            checked_at_utc=checked_at,
            repository_backend=repository_readiness.backend,
            checks={
                "identity_provider": identity_check,
                "serving_repository": repository_check,
                "serving_row_counts": row_count_check,
                "publication_freshness": freshness_check,
            },
            data_version=repository_readiness.data_version,
            data_published_at_utc=published_at,
            publication_age_seconds=publication_age_seconds,
            maximum_publication_age_seconds=(
                settings.maximum_publication_age_seconds or None
            ),
            expected_current_row_count=(
                repository_readiness.expected_current_row_count
            ),
            actual_current_row_count=repository_readiness.actual_current_row_count,
            expected_history_row_count=(
                repository_readiness.expected_history_row_count
            ),
            actual_history_row_count=repository_readiness.actual_history_row_count,
        )
        if not ready:
            return JSONResponse(status_code=503, content=response.model_dump(mode="json"))
        return response

    @app.get("/health/metrics", response_model=OperationalMetricsResponse)
    def health_metrics() -> OperationalMetricsResponse:
        snapshot = app.state.request_metrics.snapshot()
        return OperationalMetricsResponse(**asdict(snapshot))

    @app.get(
        "/api/v1/context",
        response_model=ProductContextResponse,
        responses={401: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
    )
    def context(
        actor: Annotated[Actor, Depends(_actor)],
        data_version: Annotated[str | None, Depends(_data_version)],
    ) -> ProductContextResponse:
        return app.state.service.context(actor, data_version=data_version)

    @app.get(
        "/api/v1/delivery-performance/summary",
        response_model=DeliveryPerformanceSummaryResponse,
    )
    def summary(
        actor: Annotated[Actor, Depends(_actor)],
        scope: Annotated[QueryScope, Depends(_scope)],
        data_version: Annotated[str | None, Depends(_data_version)],
    ) -> DeliveryPerformanceSummaryResponse:
        return app.state.service.summary(actor, scope, data_version=data_version)

    @app.get(
        "/api/v1/delivery-performance/intervals",
        response_model=DeliveryIntervalsPageResponse,
    )
    def intervals(
        request: Request,
        actor: Annotated[Actor, Depends(_actor)],
        scope: Annotated[QueryScope, Depends(_scope)],
        data_version: Annotated[str | None, Depends(_data_version)],
        page: Annotated[int, Query(ge=1, le=100_000)] = 1,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
    ) -> DeliveryIntervalsPageResponse:
        if limit > request.app.state.settings.maximum_page_size:
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "page_size_too_large",
                    "message": "limit exceeds the configured maximum page size",
                },
            )
        return app.state.service.intervals(
            actor,
            scope,
            page=page,
            limit=limit,
            data_version=data_version,
        )

    @app.get(
        "/api/v1/delivery-performance/intervals/{interval_key}/history",
        response_model=DeliveryIntervalHistoryResponse,
        responses={404: {"model": ErrorResponse}},
    )
    def interval_history(
        actor: Annotated[Actor, Depends(_actor)],
        data_version: Annotated[str | None, Depends(_data_version)],
        interval_key: Annotated[
            str, Path(min_length=64, max_length=64, pattern=INTERVAL_KEY_PATTERN)
        ],
        as_of: Annotated[datetime | None, Query()] = None,
    ) -> DeliveryIntervalHistoryResponse | JSONResponse:
        result = app.state.service.interval_history(
            actor,
            interval_key,
            as_of=_aware_as_of(as_of),
            data_version=data_version,
        )
        if result is None:
            return _error(404, "interval_not_found", "Delivery interval was not found")
        return result

    return app


app = create_app()
