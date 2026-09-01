"""Injected API dependencies and provider-neutral repository protocols."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import uuid
    from collections.abc import AsyncIterator
    from datetime import datetime

    from agent_api.auth import Authenticator
    from agent_core.artifacts import Artifact, ObjectStore, SourceSnapshot, Workspace
    from agent_core.audit import AuditSink
    from agent_core.control import (
        ApprovalDecision,
        PersistedApproval,
        PersistedContextCompaction,
        PersistedMemory,
        PersistedTaskState,
        RunCreationResult,
        RunSubmission,
        TaskPlanUpdate,
    )
    from agent_core.domain.models import Run, Session
    from agent_core.event_store import EventPage, StoredEvent


class SessionRepository(Protocol):
    """Tenant-scoped session storage used by HTTP handlers."""

    async def create(self, session: Session) -> Session: ...

    async def get(self, tenant_id: uuid.UUID, session_id: uuid.UUID) -> Session | None: ...


class WorkspaceRepository(Protocol):
    """Tenant-scoped immutable workspace and artifact metadata."""

    async def create(self, workspace: Workspace) -> Workspace: ...

    async def get(self, tenant_id: uuid.UUID, workspace_id: uuid.UUID) -> Workspace | None: ...

    async def create_snapshot(self, snapshot: SourceSnapshot) -> SourceSnapshot: ...

    async def get_snapshot(
        self,
        tenant_id: uuid.UUID,
        workspace_id: uuid.UUID,
        snapshot_id: uuid.UUID,
    ) -> SourceSnapshot | None: ...

    async def begin_validation(
        self,
        snapshot: SourceSnapshot,
        *,
        job_id: uuid.UUID,
    ) -> SourceSnapshot: ...

    async def list_artifacts(
        self,
        tenant_id: uuid.UUID,
        workspace_id: uuid.UUID,
        *,
        run_id: uuid.UUID | None = None,
        limit: int = 100,
    ) -> tuple[Artifact, ...]: ...

    async def get_artifact(
        self,
        tenant_id: uuid.UUID,
        artifact_id: uuid.UUID,
    ) -> Artifact | None: ...


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

    async def create_submission(
        self,
        tenant_id: uuid.UUID,
        submission: RunSubmission,
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


class ContextRepository(Protocol):
    """Tenant-scoped explicit context compaction requests."""

    async def request_compaction(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        *,
        compaction_id: uuid.UUID,
        idempotency_key: str,
        route_name: str,
        requested_at: datetime,
    ) -> PersistedContextCompaction | None: ...

    async def latest_completed(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
    ) -> PersistedContextCompaction | None: ...

    async def get(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        compaction_id: uuid.UUID,
    ) -> PersistedContextCompaction | None: ...


class TaskRepository(Protocol):
    """Versioned task state used by status and task-plan operations."""

    async def get(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
    ) -> PersistedTaskState | None: ...

    async def update(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        update: TaskPlanUpdate,
        *,
        plan_id: uuid.UUID,
        created_at: datetime,
    ) -> PersistedTaskState | None: ...


class MemoryRepository(Protocol):
    """Tenant/session-filtered long-term memory control boundary."""

    async def set_session_enabled(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        *,
        enabled: bool,
        updated_at: datetime,
    ) -> bool: ...

    async def list_active(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        *,
        limit: int = 100,
    ) -> tuple[PersistedMemory, ...]: ...

    async def archive(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        memory_id: uuid.UUID,
        *,
        archived_at: datetime,
    ) -> PersistedMemory | None: ...


@dataclass(frozen=True, slots=True)
class ApiServices:
    """Complete dependency graph for one API application instance."""

    authenticator: Authenticator
    sessions: SessionRepository
    runs: RunRepository
    approvals: ApprovalRepository
    events: EventStore
    readiness: ReadinessProbe
    context: ContextRepository | None = None
    tasks: TaskRepository | None = None
    memories: MemoryRepository | None = None
    workspaces: WorkspaceRepository | None = None
    object_store: ObjectStore | None = None
    audit: AuditSink | None = None


@dataclass(frozen=True, slots=True)
class EventGatewayServices:
    """Least-privilege dependency graph for event replay and streaming only."""

    authenticator: Authenticator
    runs: RunRepository
    events: EventStore
    readiness: ReadinessProbe


__all__ = [
    "ApiServices",
    "ApprovalRepository",
    "ContextRepository",
    "EventGatewayServices",
    "EventStore",
    "MemoryRepository",
    "ReadinessProbe",
    "RunRepository",
    "SessionRepository",
    "TaskRepository",
    "WorkspaceRepository",
]
