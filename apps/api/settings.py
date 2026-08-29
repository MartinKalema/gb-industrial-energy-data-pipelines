"""Environment-backed configuration for the read-only product API."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass


def _boolean(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false")


def _positive_integer(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    """Validated service settings with conservative local defaults."""

    demo_mode: bool = False
    trino_host: str = "trino"
    trino_port: int = 8080
    trino_user: str = "historical-delivery-api"
    trino_http_scheme: str = "http"
    trino_timeout_seconds: int = 20
    trino_query_timeout_seconds: int = 60
    trino_catalog: str = "r2"
    trino_schema: str = "industrial_energy_marts"
    maximum_query_days: int = 31
    maximum_page_size: int = 200

    @classmethod
    def from_environment(cls) -> Settings:
        scheme = os.getenv("TRINO_HTTP_SCHEME", "http").strip().lower()
        if scheme not in {"http", "https"}:
            raise ValueError("TRINO_HTTP_SCHEME must be http or https")
        return cls(
            demo_mode=_boolean("PRODUCT_DEMO_MODE"),
            trino_host=os.getenv("TRINO_HOST", "trino").strip(),
            trino_port=_positive_integer("TRINO_PORT", 8080),
            trino_user=os.getenv(
                "PRODUCT_TRINO_USER", "historical-delivery-api"
            ).strip(),
            trino_http_scheme=scheme,
            trino_timeout_seconds=_positive_integer(
                "PRODUCT_TRINO_TIMEOUT_SECONDS", 20
            ),
            trino_query_timeout_seconds=_positive_integer(
                "PRODUCT_TRINO_QUERY_TIMEOUT_SECONDS", 60
            ),
            trino_catalog=os.getenv("PRODUCT_TRINO_CATALOG", "r2").strip(),
            trino_schema=os.getenv(
                "PRODUCT_TRINO_SCHEMA", "industrial_energy_marts"
            ).strip(),
            maximum_query_days=_positive_integer("PRODUCT_MAX_QUERY_DAYS", 31),
            maximum_page_size=_positive_integer("PRODUCT_MAX_PAGE_SIZE", 200),
        )

    def __post_init__(self) -> None:
        if not self.trino_host:
            raise ValueError("TRINO_HOST must not be empty")
        if not self.trino_user:
            raise ValueError("PRODUCT_TRINO_USER must not be empty")
        if self.trino_http_scheme not in {"http", "https"}:
            raise ValueError("trino_http_scheme must be http or https")
        identifier_pattern = re.compile(r"^[a-z][a-z0-9_]*$")
        for name in ("trino_catalog", "trino_schema"):
            if identifier_pattern.fullmatch(getattr(self, name)) is None:
                raise ValueError(
                    f"{name} must contain only lower-case letters, numbers, and underscores"
                )
        for name in (
            "trino_port",
            "trino_timeout_seconds",
            "trino_query_timeout_seconds",
            "maximum_query_days",
            "maximum_page_size",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be greater than zero")
        if self.maximum_page_size > 200:
            raise ValueError(
                "maximum_page_size must not exceed the API contract cap of 200"
            )
