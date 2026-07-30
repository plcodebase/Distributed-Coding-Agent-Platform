"""Provider-neutral gateway idempotency and admission interfaces."""

from __future__ import annotations

import uuid  # noqa: TC003 - Pydantic resolves tenant identifiers at runtime
from enum import StrEnum
from typing import Protocol

from agent_core.domain.base import DomainModel
from agent_core.domain.errors import ErrorDetail  # noqa: TC001 - Pydantic resolves at runtime
from agent_core.domain.models import Sha256Hex  # noqa: TC001 - Pydantic resolves at runtime
from agent_core.gateway import (
    GatewayEvent,
    GatewayRequestIdentifier,
    GatewayResponseCompleted,
)


class GatewayRequestClaimStatus(StrEnum):
    """Outcome of atomically claiming one stable logical request."""

    EXECUTE = "execute"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CONFLICT = "conflict"


class GatewayRequestClaim(DomainModel):
    """Stored state returned by a request-id idempotency boundary."""

    status: GatewayRequestClaimStatus
    request_hash: Sha256Hex
    events: tuple[GatewayEvent, ...] = ()
    error: ErrorDetail | None = None

    def model_post_init(self, context: object, /) -> None:
        del context
        if self.status is GatewayRequestClaimStatus.COMPLETED:
            if not self.events or self.error is not None:
                raise ValueError("completed claim requires events and no error")
            terminal_positions = tuple(
                index
                for index, event in enumerate(self.events)
                if isinstance(event, GatewayResponseCompleted)
            )
            if terminal_positions != (len(self.events) - 1,):
                raise ValueError(
                    "completed claim requires exactly one final response-completed event"
                )
        elif self.events:
            raise ValueError("non-completed claim may not contain events")
        if self.status is GatewayRequestClaimStatus.FAILED:
            if self.error is None:
                raise ValueError("failed claim requires an error")
        elif self.error is not None:
            raise ValueError("only a failed claim may contain an error")


class GatewayRequestStore(Protocol):
    """Atomic persistence boundary for stable logical gateway requests."""

    async def claim(
        self,
        tenant_id: uuid.UUID,
        request_id: GatewayRequestIdentifier,
        request_hash: Sha256Hex,
    ) -> GatewayRequestClaim:
        """Claim an unseen request or return its existing durable state."""

    async def complete(
        self,
        tenant_id: uuid.UUID,
        request_id: GatewayRequestIdentifier,
        request_hash: Sha256Hex,
        events: tuple[GatewayEvent, ...],
    ) -> None:
        """Persist a complete normalized response for future replay."""

    async def fail(
        self,
        tenant_id: uuid.UUID,
        request_id: GatewayRequestIdentifier,
        request_hash: Sha256Hex,
        error: ErrorDetail,
    ) -> None:
        """Persist an opaque terminal failure to prevent ambiguous re-execution."""

    async def release(
        self,
        tenant_id: uuid.UUID,
        request_id: GatewayRequestIdentifier,
        request_hash: Sha256Hex,
    ) -> None:
        """Release an admission claim before any provider attempt was made."""


class GatewayRateLimiter(Protocol):
    """Tenant-and-route admission boundary for one logical model request."""

    async def acquire(self, tenant_id: uuid.UUID, route_name: str) -> float | None:
        """Return retry-after seconds when the request is over limit, otherwise ``None``."""


class GatewayCircuitBreaker(Protocol):
    """Circuit state boundary keyed by logical model route."""

    async def allow(self, route_name: str) -> bool:
        """Return whether one attempt may contact the configured gateway."""

    async def record_success(self, route_name: str) -> None:
        """Close/reset circuit state after a successful response."""

    async def record_failure(self, route_name: str) -> None:
        """Record a retryable gateway failure."""


__all__ = [
    "GatewayCircuitBreaker",
    "GatewayRateLimiter",
    "GatewayRequestClaim",
    "GatewayRequestClaimStatus",
    "GatewayRequestStore",
]
