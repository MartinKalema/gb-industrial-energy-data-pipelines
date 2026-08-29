"""FastAPI boundary for governed historical steam-delivery performance."""

from __future__ import annotations

import logging
import re
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
from apps.api.models import (
    DeliveryIntervalHistoryResponse,
    DeliveryIntervalsPageResponse,
    DeliveryPerformanceSummaryResponse,
    ErrorResponse,
    HealthResponse,
    ProductContextResponse,
)
from apps.api.repository import (
    DeliveryPerformanceRepository,
    MartIntegrityError,
    QueryScope,
    RepositoryUnavailable,
    TrinoDeliveryPerformanceRepository,
)
from apps.api.request_logging import (
    RequestAuditMiddleware,
    configure_request_audit_logging,
)
from apps.api.service import DeliveryPerformanceService
from apps.api.settings import Settings

StatusFilter = Literal[
    "final", "provisional", "missing", "corrected", "shortfall", "excess"
]
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
INTERVAL_KEY_PATTERN = r"^[a-f0-9]{64}$"


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


def create_app(
    *,
    settings: Settings | None = None,
    repository: DeliveryPerformanceRepository | None = None,
) -> FastAPI:
    settings = settings or Settings.from_environment()
    repository = repository or TrinoDeliveryPerformanceRepository(settings)

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
    configure_request_audit_logging()
    app.add_middleware(RequestAuditMiddleware)

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
        response_model=HealthResponse,
        responses={503: {"model": ErrorResponse}},
    )
    def health_ready() -> HealthResponse | JSONResponse:
        try:
            ready = repository.is_ready()
        except RepositoryUnavailable:
            ready = False
        if not ready:
            return _error(
                503,
                "mart_not_ready",
                "The governed delivery mart is not ready",
            )
        return HealthResponse(status="ready")

    @app.get(
        "/api/v1/context",
        response_model=ProductContextResponse,
        responses={401: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
    )
    def context(actor: Annotated[Actor, Depends(_actor)]) -> ProductContextResponse:
        return app.state.service.context(actor)

    @app.get(
        "/api/v1/delivery-performance/summary",
        response_model=DeliveryPerformanceSummaryResponse,
    )
    def summary(
        actor: Annotated[Actor, Depends(_actor)],
        scope: Annotated[QueryScope, Depends(_scope)],
    ) -> DeliveryPerformanceSummaryResponse:
        return app.state.service.summary(actor, scope)

    @app.get(
        "/api/v1/delivery-performance/intervals",
        response_model=DeliveryIntervalsPageResponse,
    )
    def intervals(
        request: Request,
        actor: Annotated[Actor, Depends(_actor)],
        scope: Annotated[QueryScope, Depends(_scope)],
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
        return app.state.service.intervals(actor, scope, page=page, limit=limit)

    @app.get(
        "/api/v1/delivery-performance/intervals/{interval_key}/history",
        response_model=DeliveryIntervalHistoryResponse,
        responses={404: {"model": ErrorResponse}},
    )
    def interval_history(
        actor: Annotated[Actor, Depends(_actor)],
        interval_key: Annotated[
            str, Path(min_length=64, max_length=64, pattern=INTERVAL_KEY_PATTERN)
        ],
        as_of: Annotated[datetime | None, Query()] = None,
    ) -> DeliveryIntervalHistoryResponse | JSONResponse:
        result = app.state.service.interval_history(
            actor, interval_key, as_of=_aware_as_of(as_of)
        )
        if result is None:
            return _error(404, "interval_not_found", "Delivery interval was not found")
        return result

    return app


app = create_app()
