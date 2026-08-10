"""Provider-neutral bounded-capacity and tenant-quota contracts."""

from __future__ import annotations

import uuid  # noqa: TC003 - Pydantic resolves UUID fields at runtime
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol, Self

from pydantic import Field, model_validator

from agent_core.domain.base import AwareTimestamp, DomainModel
from agent_core.domain.models import IdentifierString  # noqa: TC001 - runtime field
from agent_core.gateway import GatewayRequestIdentifier  # noqa: TC001 - runtime field

if TYPE_CHECKING:
    from datetime import timedelta


class CapacityScope(StrEnum):
    """Closed reason describing which independently bounded resource is full."""

    TENANT_ACTIVE_RUNS = "tenant_active_runs"
    TENANT_QUEUED_RUNS = "tenant_queued_runs"
    TENANT_GATEWAY_REQUESTS = "tenant_gateway_requests"
    PROVIDER_REQUESTS = "provider_requests"
    PROVIDER_TOKENS = "provider_tokens"
    GLOBAL_QUEUE = "global_queue"
    WORKER_RUNS = "worker_runs"
    WORKER_SANDBOXES = "worker_sandboxes"


class TenantQuota(DomainModel):
    """Validated durable limits controlled by platform composition, never callers."""

    max_active_runs: int = Field(default=4, ge=1, le=10_000)
    max_queued_runs: int = Field(default=100, ge=1, le=100_000)
    max_gateway_requests: int = Field(default=4, ge=1, le=10_000)
    memory_enabled: bool = True


class CapacityRejection(DomainModel):
    """Predictable retry metadata returned without acquiring partial capacity."""

    scope: CapacityScope
    retry_after_seconds: float = Field(gt=0, le=3600)


class GatewayCapacityLease(DomainModel):
    """Expiring distributed ownership over gateway and route capacity."""

    id: uuid.UUID
    tenant_id: uuid.UUID
    route_name: IdentifierString
    request_id: GatewayRequestIdentifier
    reserved_tokens: int = Field(ge=1, le=1_000_000_000)
    acquired_at: AwareTimestamp
    expires_at: AwareTimestamp

    @model_validator(mode="after")
    def validate_window(self) -> Self:
        if self.expires_at <= self.acquired_at:
            raise ValueError("gateway capacity expiry must follow acquisition")
        return self


class GatewayCapacityClaim(DomainModel):
    """Atomic all-or-nothing gateway admission result."""

    lease: GatewayCapacityLease | None = None
    rejection: CapacityRejection | None = None

    @model_validator(mode="after")
    def validate_outcome(self) -> Self:
        if (self.lease is None) == (self.rejection is None):
            raise ValueError("capacity claim requires exactly one lease or rejection")
        return self


class GatewayCapacityStore(Protocol):
    """Distributed gateway/provider slot and provider-token admission boundary."""

    async def acquire(
        self,
        tenant_id: uuid.UUID,
        route_name: str,
        request_id: str,
        *,
        reserved_tokens: int,
        lease_duration: timedelta,
    ) -> GatewayCapacityClaim:
        """Atomically acquire tenant, route, and token capacity or reject all."""

    async def release(
        self,
        lease: GatewayCapacityLease,
        *,
        consumed_tokens: int,
    ) -> None:
        """Idempotently release request slots and reconcile reserved token usage."""

    async def renew(
        self,
        lease: GatewayCapacityLease,
        *,
        lease_duration: timedelta,
    ) -> GatewayCapacityLease:
        """Extend one exact unexpired lease or reject stale ownership."""


class QueueDepth(DomainModel):
    """Bounded queue depth grouped by scheduling class for later metrics export."""

    interactive: int = Field(ge=0)
    background: int = Field(ge=0)
    evaluation: int = Field(ge=0)

    @property
    def total(self) -> int:
        return self.interactive + self.background + self.evaluation


class QueueSnapshot(DomainModel):
    """Point-in-time queue pressure without observability-provider dependencies."""

    depth: QueueDepth
    oldest_age_seconds: float = Field(ge=0, le=31_536_000)
    captured_at: AwareTimestamp


__all__ = [
    "CapacityRejection",
    "CapacityScope",
    "GatewayCapacityClaim",
    "GatewayCapacityLease",
    "GatewayCapacityStore",
    "QueueDepth",
    "QueueSnapshot",
    "TenantQuota",
]
