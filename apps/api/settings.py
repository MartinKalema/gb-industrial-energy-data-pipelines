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


def _non_negative_integer(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value < 0:
        raise ValueError(f"{name} must be zero or greater")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    """Validated service settings with conservative local defaults."""

    demo_mode: bool = False
    repository_backend: str = "trino"
    trino_host: str = "trino"
    trino_port: int = 8080
    trino_user: str = "historical-delivery-api"
    trino_http_scheme: str = "http"
    trino_timeout_seconds: int = 20
    trino_query_timeout_seconds: int = 60
    trino_catalog: str = "r2"
    trino_schema: str = "industrial_energy_marts"
    clickhouse_host: str = "clickhouse"
    clickhouse_http_port: int = 8123
    clickhouse_user: str = "historical_delivery_api"
    clickhouse_password: str = ""
    clickhouse_secure: bool = False
    clickhouse_timeout_seconds: int = 20
    clickhouse_query_timeout_seconds: int = 60
    clickhouse_database: str = "industrial_energy_serving"
    maximum_query_days: int = 31
    maximum_page_size: int = 200
    maximum_publication_age_seconds: int = 0

    @classmethod
    def from_environment(cls) -> Settings:
        scheme = os.getenv("TRINO_HTTP_SCHEME", "http").strip().lower()
        if scheme not in {"http", "https"}:
            raise ValueError("TRINO_HTTP_SCHEME must be http or https")
        return cls(
            demo_mode=_boolean("PRODUCT_DEMO_MODE"),
            repository_backend=os.getenv("PRODUCT_REPOSITORY_BACKEND", "trino")
            .strip()
            .lower(),
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
            clickhouse_host=os.getenv("CLICKHOUSE_HOST", "clickhouse").strip(),
            clickhouse_http_port=_positive_integer("CLICKHOUSE_HTTP_PORT", 8123),
            clickhouse_user=os.getenv(
                "CLICKHOUSE_USER", "historical_delivery_api"
            ).strip(),
            clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            clickhouse_secure=_boolean("CLICKHOUSE_SECURE"),
            clickhouse_timeout_seconds=_positive_integer(
                "PRODUCT_CLICKHOUSE_TIMEOUT_SECONDS", 20
            ),
            clickhouse_query_timeout_seconds=_positive_integer(
                "PRODUCT_CLICKHOUSE_QUERY_TIMEOUT_SECONDS", 60
            ),
            clickhouse_database=os.getenv(
                "PRODUCT_CLICKHOUSE_DATABASE", "industrial_energy_serving"
            ).strip(),
            maximum_query_days=_positive_integer("PRODUCT_MAX_QUERY_DAYS", 31),
            maximum_page_size=_positive_integer("PRODUCT_MAX_PAGE_SIZE", 200),
            maximum_publication_age_seconds=_non_negative_integer(
                "PRODUCT_MAX_PUBLICATION_AGE_SECONDS", 0
            ),
        )

    def __post_init__(self) -> None:
        if self.repository_backend not in {"trino", "clickhouse"}:
            raise ValueError("PRODUCT_REPOSITORY_BACKEND must be trino or clickhouse")
        if not self.trino_host:
            raise ValueError("TRINO_HOST must not be empty")
        if not self.trino_user:
            raise ValueError("PRODUCT_TRINO_USER must not be empty")
        if self.trino_http_scheme not in {"http", "https"}:
            raise ValueError("trino_http_scheme must be http or https")
        if not self.clickhouse_host:
            raise ValueError("CLICKHOUSE_HOST must not be empty")
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]*", self.clickhouse_host) is None:
            raise ValueError("CLICKHOUSE_HOST must be a host name without a URL path")
        if (
            not self.clickhouse_user
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", self.clickhouse_user)
            is None
        ):
            raise ValueError("CLICKHOUSE_USER has an invalid format")
        if self.repository_backend == "clickhouse":
            if not self.clickhouse_password:
                raise ValueError(
                    "CLICKHOUSE_PASSWORD must not be empty for the clickhouse backend"
                )
            if len(self.clickhouse_password) > 1024 or any(
                ord(character) < 32 for character in self.clickhouse_password
            ):
                raise ValueError("CLICKHOUSE_PASSWORD has an invalid format")
        identifier_pattern = re.compile(r"^[a-z][a-z0-9_]*$")
        for name in ("trino_catalog", "trino_schema", "clickhouse_database"):
            if identifier_pattern.fullmatch(getattr(self, name)) is None:
                raise ValueError(
                    f"{name} must contain only lower-case letters, numbers, and underscores"
                )
        for name in (
            "trino_port",
            "trino_timeout_seconds",
            "trino_query_timeout_seconds",
            "clickhouse_http_port",
            "clickhouse_timeout_seconds",
            "clickhouse_query_timeout_seconds",
            "maximum_query_days",
            "maximum_page_size",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be greater than zero")
        for name in ("trino_port", "clickhouse_http_port"):
            if getattr(self, name) > 65_535:
                raise ValueError(f"{name} must not exceed 65535")
        if self.maximum_page_size > 200:
            raise ValueError(
                "maximum_page_size must not exceed the API contract cap of 200"
            )
        if self.maximum_publication_age_seconds < 0:
            raise ValueError(
                "maximum_publication_age_seconds must be zero or greater"
            )
