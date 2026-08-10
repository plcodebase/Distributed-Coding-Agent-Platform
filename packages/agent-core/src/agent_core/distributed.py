"""Provider-neutral contracts for distributed run execution and recovery."""

from __future__ import annotations

import uuid  # noqa: TC003 - Pydantic resolves fields at runtime
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol, Self

from pydantic import Field, model_validator

from agent_core.domain.base import AwareTimestamp, DomainModel, FrozenJsonObject
from agent_core.domain.errors import (  # noqa: TC001 - Pydantic resolves fields at runtime
    ErrorDetail,
)
from agent_core.domain.models import (  # noqa: TC001 - Pydantic resolves fields at runtime
    Checkpoint,
    IdentifierString,
    Run,
    Sha256Hex,
    ToolName,
)
from agent_core.domain.status import RunStatus, ToolCallStatus
from agent_core.gateway import (  # noqa: TC001 - Pydantic resolves fields at runtime
    GatewayMessage,
)

if TYPE_CHECKING:
    from datetime import datetime, timedelta


class WorkerStatus(StrEnum):
    """Scheduler-visible worker lifecycle."""

    ACTIVE = "active"
    DRAINING = "draining"
    OFFLINE = "offline"


class WorkerRegistration(DomainModel):
    """Bounded worker capacity and liveness advertised to the scheduler."""

    worker_id: IdentifierString
    supported_sandbox_types: tuple[IdentifierString, ...] = Field(
        min_length=1,
        max_length=32,
    )
    total_slots: int = Field(ge=1, le=1024)
    available_slots: int = Field(ge=0, le=1024)
    status: WorkerStatus
    registered_at: AwareTimestamp
    last_heartbeat_at: AwareTimestamp

    @model_validator(mode="after")
    def validate_capacity(self) -> Self:
        if self.available_slots > self.total_slots:
            raise ValueError("available_slots may not exceed total_slots")
        if len(set(self.supported_sandbox_types)) != len(self.supported_sandbox_types):
            raise ValueError("supported_sandbox_types must be unique")
        if self.last_heartbeat_at < self.registered_at:
            raise ValueError("last_heartbeat_at may not precede registered_at")
        return self


class RunLease(DomainModel):
    """One fenced, expiring ownership claim over a queued run."""

    tenant_id: uuid.UUID
    run_id: uuid.UUID
    session_id: uuid.UUID
    workspace_id: uuid.UUID
    worker_id: IdentifierString
    route_name: IdentifierString
    lease_token: uuid.UUID
    generation: int = Field(ge=1)
    attempt: int = Field(ge=1)
    priority: int
    acquired_at: AwareTimestamp
    expires_at: AwareTimestamp
    checkpoint_id: uuid.UUID | None = None
    cancellation_requested: bool = False

    @model_validator(mode="after")
    def validate_window(self) -> Self:
        if self.expires_at <= self.acquired_at:
            raise ValueError("run lease expiry must follow acquisition")
        return self


class RunLeaseHeartbeat(DomainModel):
    """Renewed run ownership plus the current distributed cancellation signal."""

    lease_token: uuid.UUID
    generation: int = Field(ge=1)
    expires_at: AwareTimestamp
    cancellation_requested: bool


class WorkspaceWriterLease(DomainModel):
    """One fenced writer claim for a tenant workspace."""

    tenant_id: uuid.UUID
    workspace_id: uuid.UUID
    run_id: uuid.UUID
    worker_id: IdentifierString
    run_lease_token: uuid.UUID
    lease_token: uuid.UUID
    generation: int = Field(ge=1)
    acquired_at: AwareTimestamp
    expires_at: AwareTimestamp

    @model_validator(mode="after")
    def validate_window(self) -> Self:
        if self.expires_at <= self.acquired_at:
            raise ValueError("workspace lease expiry must follow acquisition")
        return self


class DurableToolOutcome(DomainModel):
    """Terminal tool result that can be injected into a recovered loop attempt."""

    tool_call_id: IdentifierString
    tool_name: ToolName
    turn_number: int = Field(ge=1)
    argument_hash: Sha256Hex
    status: ToolCallStatus
    workspace_version: IdentifierString | None = None
    result: FrozenJsonObject | None = None
    error: ErrorDetail | None = None

    @model_validator(mode="after")
    def validate_terminal_outcome(self) -> Self:
        if self.status not in {
            ToolCallStatus.COMPLETED,
            ToolCallStatus.FAILED,
            ToolCallStatus.CANCELLED,
        }:
            raise ValueError("durable tool outcome must be terminal")
        if self.status is ToolCallStatus.COMPLETED:
            if self.result is None or self.error is not None:
                raise ValueError("completed durable tool outcome requires only a result")
        elif self.error is None or self.result is not None:
            raise ValueError("failed or cancelled durable tool outcome requires only an error")
        return self


class RunRecoveryState(DomainModel):
    """Latest durable state needed to resume a reassigned run."""

    checkpoint: Checkpoint | None = None
    workspace_restore_revision: IdentifierString | None = None
    messages: tuple[GatewayMessage, ...] = Field(min_length=1, max_length=4096)
    task_plan: FrozenJsonObject = Field(default_factory=lambda: FrozenJsonObject({}))
    context_summary: str | None = None
    prior_tool_outcomes: tuple[DurableToolOutcome, ...] = Field(
        default=(),
        max_length=100,
    )

    @model_validator(mode="after")
    def validate_outcomes(self) -> Self:
        if (self.checkpoint is None) != (self.workspace_restore_revision is None):
            raise ValueError(
                "workspace_restore_revision must be present exactly when a checkpoint is present"
            )
        identifiers = [outcome.tool_call_id for outcome in self.prior_tool_outcomes]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("prior durable tool-call IDs must be unique")
        return self


class RunExecutionResult(DomainModel):
    """Worker result consumed by the fenced queue completion operation."""

    status: RunStatus
    last_checkpoint_id: uuid.UUID | None = None
    error: ErrorDetail | None = None

    @model_validator(mode="after")
    def validate_status(self) -> Self:
        if self.status not in {
            RunStatus.COMPLETED,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
            RunStatus.WAITING_APPROVAL,
            RunStatus.RETRY_PENDING,
        }:
            raise ValueError("worker execution result must release its run lease")
        if self.status is RunStatus.FAILED and self.error is None:
            raise ValueError("failed worker execution requires an error")
        if self.status is not RunStatus.FAILED and self.error is not None:
            raise ValueError("only failed worker execution may include an error")
        return self


class RunQueue(Protocol):
    """Durable at-least-once run queue with fenced leases."""

    async def register_worker(self, registration: WorkerRegistration) -> WorkerRegistration:
        """Create or safely refresh one worker registration."""

    async def heartbeat_worker(
        self,
        worker_id: str,
        *,
        occurred_at: datetime,
        available_slots: int,
    ) -> WorkerRegistration:
        """Refresh worker liveness and bounded available capacity."""

    async def set_worker_draining(
        self,
        worker_id: str,
        *,
        draining: bool,
        occurred_at: datetime,
    ) -> WorkerRegistration:
        """Enable or disable graceful draining."""

    async def claim(
        self,
        worker_id: str,
        *,
        occurred_at: datetime,
        lease_duration: timedelta,
    ) -> RunLease | None:
        """Claim one queued run using a database-enforced nonblocking lock."""

    async def start(self, lease: RunLease, *, occurred_at: datetime) -> RunLease:
        """Transition a matching, unexpired leased run to running."""

    async def heartbeat(
        self,
        lease: RunLease,
        *,
        occurred_at: datetime,
        lease_duration: timedelta,
    ) -> RunLeaseHeartbeat:
        """Renew matching ownership and observe distributed cancellation."""

    async def finish(
        self,
        lease: RunLease,
        result: RunExecutionResult,
        *,
        occurred_at: datetime,
    ) -> Run:
        """Release matching ownership and apply the execution result."""

    async def recover_expired(
        self,
        *,
        occurred_at: datetime,
        limit: int,
    ) -> tuple[Run, ...]:
        """Mark expired owners lost and requeue each run exactly once per lease."""


class WorkspaceLeaseStore(Protocol):
    """Exclusive fenced writer leases keyed by tenant and workspace."""

    async def acquire(
        self,
        run_lease: RunLease,
        *,
        occurred_at: datetime,
        lease_duration: timedelta,
    ) -> WorkspaceWriterLease | None:
        """Acquire the workspace writer lease or return ``None`` on contention."""

    async def heartbeat(
        self,
        lease: WorkspaceWriterLease,
        *,
        occurred_at: datetime,
        lease_duration: timedelta,
    ) -> WorkspaceWriterLease:
        """Renew a matching workspace writer lease."""

    async def release(self, lease: WorkspaceWriterLease) -> None:
        """Release only the matching fenced workspace writer lease."""


class RecoveryStore(Protocol):
    """Load a bounded checkpoint and replay state for a claimed attempt."""

    async def load(self, lease: RunLease) -> RunRecoveryState:
        """Load the latest checkpoint, messages, plan, summary, and terminal tools."""


class WorkspaceRestorer(Protocol):
    """Restore the immutable snapshot selected by durable recovery."""

    async def restore(
        self,
        lease: RunLease,
        checkpoint: Checkpoint,
        *,
        writer_lease: WorkspaceWriterLease,
        workspace_revision: str,
    ) -> None:
        """Restore the selected revision under the exact workspace-writer fence."""


class RunExecutor(Protocol):
    """Execute one already-fenced run attempt outside the API process."""

    async def execute(
        self,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        recovery: RunRecoveryState,
    ) -> RunExecutionResult:
        """Execute one attempt under both run and workspace-writer fences."""

    async def cancel(self, lease: RunLease) -> None:
        """Cancel active model/tool/sandbox work for a distributed request."""


__all__ = [
    "DurableToolOutcome",
    "RecoveryStore",
    "RunExecutionResult",
    "RunExecutor",
    "RunLease",
    "RunLeaseHeartbeat",
    "RunQueue",
    "RunRecoveryState",
    "WorkerRegistration",
    "WorkerStatus",
    "WorkspaceLeaseStore",
    "WorkspaceRestorer",
    "WorkspaceWriterLease",
]
