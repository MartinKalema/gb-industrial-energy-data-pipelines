"""Structured, value-safe request and authorization audit logging."""

from __future__ import annotations

import json
import logging
import re
import time
import uuid

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

LOGGER = logging.getLogger("historical_delivery_api.requests")
REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
AUDIT_HANDLER_NAME = "historical-delivery-json-audit"


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
        response: Response | None = None
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            response.headers["X-Request-ID"] = request_id
            return response
        finally:
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
                "duration_ms": round((time.perf_counter() - started) * 1_000, 3),
            }
            LOGGER.info(json.dumps(event, separators=(",", ":"), sort_keys=True))
