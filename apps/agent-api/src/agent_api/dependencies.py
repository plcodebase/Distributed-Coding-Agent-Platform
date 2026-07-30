"""Injected API dependencies and provider-neutral repository protocols."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import uuid
    from collections.abc import AsyncIterator
    from datetime import datetime

    from agent_api.auth import Authenticator
    from agent_core.control import ApprovalDecision, PersistedApproval, RunCreationResult
    from agent_core.domain.models import Run, Session
    from agent_core.event_store import EventPage, StoredEvent


class SessionRepository(Protocol):
    """Tenant-scoped session storage used by HTTP handlers."""

    async def create(self, session: Session) -> Session: ...

    async def get(self, tenant_id: uuid.UUID, session_id: uuid.UUID) -> Session | None: ...


class RunRepository(Protocol):
    """Tenant-scoped run storage used by HTTP handlers."""

    async def create_idempotent(
        self,
        tenant_id: uuid.UUID,
        run: Run,
        *,
        idempotency_key: str,
        creation_hash: str,
    ) -> RunCreationResult: ...

    async def get(self, tenant_id: uuid.UUID, run_id: uuid.UUID) -> Run | None: ...

    async def request_cancel(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        *,
        occurred_at: datetime,
    ) -> Run | None: ...

    async def rewind(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        checkpoint_id: uuid.UUID,
    ) -> Run | None: ...


class ApprovalRepository(Protocol):
    """Tenant-scoped durable approval decision boundary."""

    async def decide(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        approval_id: uuid.UUID,
        decision: ApprovalDecision,
    ) -> PersistedApproval | None: ...


class EventStore(Protocol):
    """Tenant-scoped durable replay and live stream boundary."""

    async def read_page(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        *,
        after: int = 0,
        limit: int = 100,
    ) -> EventPage: ...

    def stream(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        *,
        after: int = 0,
        page_size: int = 100,
    ) -> AsyncIterator[StoredEvent]: ...


class ReadinessProbe(Protocol):
    """Durable dependency readiness boundary."""

    async def ready(self) -> bool: ...


@dataclass(frozen=True, slots=True)
class ApiServices:
    """Complete dependency graph for one API application instance."""

    authenticator: Authenticator
    sessions: SessionRepository
    runs: RunRepository
    approvals: ApprovalRepository
    events: EventStore
    readiness: ReadinessProbe


__all__ = [
    "ApiServices",
    "ApprovalRepository",
    "EventStore",
    "ReadinessProbe",
    "RunRepository",
    "SessionRepository",
]
