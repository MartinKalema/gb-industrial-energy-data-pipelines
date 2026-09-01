"""Structured, value-safe request and authorization audit logging."""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Lock

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.types import ASGIApp

LOGGER = logging.getLogger("historical_delivery_api.requests")
REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
AUDIT_HANDLER_NAME = "historical-delivery-json-audit"


@dataclass(frozen=True, slots=True)
class RequestMetricsSnapshot:
    process_started_at_utc: datetime
    uptime_seconds: int
    in_flight_requests: int
    completed_request_count: int
    response_4xx_count: int
    response_5xx_count: int
    average_duration_ms: float
    maximum_duration_ms: float


class RequestMetrics:
    """Bounded, process-local request counters without customer-value labels."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._process_started_at_utc = datetime.now(UTC)
        self._started_monotonic = time.monotonic()
        self._in_flight = 0
        self._completed = 0
        self._response_4xx = 0
        self._response_5xx = 0
        self._total_duration_ms = 0.0
        self._maximum_duration_ms = 0.0

    def request_started(self) -> None:
        with self._lock:
            self._in_flight += 1

    def request_finished(self, *, status_code: int, duration_ms: float) -> None:
        with self._lock:
            self._in_flight = max(0, self._in_flight - 1)
            self._completed += 1
            if 400 <= status_code < 500:
                self._response_4xx += 1
            elif status_code >= 500:
                self._response_5xx += 1
            self._total_duration_ms += max(0.0, duration_ms)
            self._maximum_duration_ms = max(
                self._maximum_duration_ms, max(0.0, duration_ms)
            )

    def snapshot(self) -> RequestMetricsSnapshot:
        with self._lock:
            completed = self._completed
            average = self._total_duration_ms / completed if completed else 0.0
            return RequestMetricsSnapshot(
                process_started_at_utc=self._process_started_at_utc,
                uptime_seconds=max(
                    0, int(time.monotonic() - self._started_monotonic)
                ),
                in_flight_requests=self._in_flight,
                completed_request_count=completed,
                response_4xx_count=self._response_4xx,
                response_5xx_count=self._response_5xx,
                average_duration_ms=round(average, 3),
                maximum_duration_ms=round(self._maximum_duration_ms, 3),
            )


def configure_request_audit_logging() -> None:
    """Make INFO audit events visible even when Uvicorn owns logging config."""

    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False
    if any(handler.get_name() == AUDIT_HANDLER_NAME for handler in LOGGER.handlers):
        return
    handler = logging.StreamHandler()
    handler.set_name(AUDIT_HANDLER_NAME)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(message)s"))
    LOGGER.addHandler(handler)


class RequestAuditMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, *, metrics: RequestMetrics) -> None:
        super().__init__(app)
        self._metrics = metrics

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        supplied_request_id = request.headers.get("X-Request-ID", "")
        request_id = (
            supplied_request_id
            if REQUEST_ID_PATTERN.fullmatch(supplied_request_id)
            else str(uuid.uuid4())
        )
        request.state.request_id = request_id
        started = time.perf_counter()
        self._metrics.request_started()
        response: Response | None = None
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            response.headers["X-Request-ID"] = request_id
            return response
        finally:
            duration_ms = (time.perf_counter() - started) * 1_000
            self._metrics.request_finished(
                status_code=status_code,
                duration_ms=duration_ms,
            )
            route = request.scope.get("route")
            route_path = getattr(route, "path", request.url.path)
            event = {
                "event": "http_request",
                "request_id": request_id,
                "actor_id": getattr(request.state, "actor_id", None),
                "authorized_tenant_scope": getattr(
                    request.state, "authorized_tenant_scope", None
                ),
                "route": route_path,
                "method": request.method,
                "customer_scope": getattr(request.state, "customer_scope", None),
                "site_scope": getattr(request.state, "site_scope", None),
                "status": status_code,
                "duration_ms": round(duration_ms, 3),
            }
            LOGGER.info(json.dumps(event, separators=(",", ":"), sort_keys=True))
