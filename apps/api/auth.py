"""Fail-closed demo identity and server-side authorization scopes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


class AuthenticationFailed(Exception):
    """Raised when a request has no accepted identity."""


class IdentityProviderUnavailable(Exception):
    """Raised when no authentication adapter is configured."""


class AuthorizationDenied(Exception):
    """Raised when an actor requests data outside its server-side scope."""


@dataclass(frozen=True, slots=True)
class Actor:
    actor_id: str
    role: Literal["commercial_manager", "customer"]
    tenant_scope_ids: frozenset[str] | None
    permitted_customer_ids: frozenset[str] | None
    permitted_site_ids: frozenset[str] | None
    permitted_delivery_point_ids: frozenset[str] | None

    @property
    def has_all_customer_access(self) -> bool:
        return self.tenant_scope_ids is None


DEMO_ACTORS: dict[str, Actor] = {
    "commercial-manager": Actor(
        actor_id="commercial-manager",
        role="commercial_manager",
        tenant_scope_ids=None,
        permitted_customer_ids=None,
        permitted_site_ids=None,
        permitted_delivery_point_ids=None,
    ),
    "customer-cust-001": Actor(
        actor_id="customer-cust-001",
        role="customer",
        tenant_scope_ids=frozenset({"TENANT-CUST-001"}),
        permitted_customer_ids=frozenset({"CUST-001"}),
        permitted_site_ids=frozenset({"SITE-001"}),
        permitted_delivery_point_ids=frozenset({"DP-001"}),
    ),
    "customer-cust-002": Actor(
        actor_id="customer-cust-002",
        role="customer",
        tenant_scope_ids=frozenset({"TENANT-CUST-002"}),
        permitted_customer_ids=frozenset({"CUST-002"}),
        permitted_site_ids=frozenset({"SITE-002"}),
        permitted_delivery_point_ids=frozenset({"DP-002"}),
    ),
}


class DemoIdentityAdapter:
    """Local authorization-test adapter; it is not production authentication."""

    def __init__(self, *, enabled: bool) -> None:
        self._enabled = enabled

    def authenticate(self, actor_header: str | None) -> Actor:
        if not self._enabled:
            raise IdentityProviderUnavailable(
                "No identity provider is configured for this API"
            )
        if actor_header is None or not actor_header.strip():
            raise AuthenticationFailed("X-Demo-Actor is required")
        actor = DEMO_ACTORS.get(actor_header.strip())
        if actor is None:
            raise AuthenticationFailed("X-Demo-Actor is not recognized")
        return actor


def authorize_filters(
    actor: Actor,
    *,
    customer_id: str | None,
    site_id: str | None,
    delivery_point_id: str | None,
) -> None:
    """Reject a known demo persona's explicit cross-scope filters early.

    Repository queries independently apply the immutable tenant scope. This
    check provides a clear 403 without making browser identifiers authoritative.
    """

    if (
        customer_id is not None
        and actor.permitted_customer_ids is not None
        and customer_id not in actor.permitted_customer_ids
    ):
        raise AuthorizationDenied("The requested customer is outside your scope")
    if (
        site_id is not None
        and actor.permitted_site_ids is not None
        and site_id not in actor.permitted_site_ids
    ):
        raise AuthorizationDenied("The requested site is outside your scope")
    if (
        delivery_point_id is not None
        and actor.permitted_delivery_point_ids is not None
        and delivery_point_id not in actor.permitted_delivery_point_ids
    ):
        raise AuthorizationDenied("The requested delivery point is outside your scope")
