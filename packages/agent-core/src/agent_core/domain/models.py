"""Validated domain entities defined by the implementation design."""

from __future__ import annotations

import decimal  # noqa: TC003 - Pydantic resolves this field type at runtime
import hashlib
import json
import uuid  # noqa: TC003 - Pydantic resolves this field type at runtime
from typing import TYPE_CHECKING, Annotated, Any, Self

from pydantic import Field, StringConstraints, model_validator

from agent_core.domain.base import AwareTimestamp, DomainModel, FrozenJsonObject, JsonObject
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.domain.status import (
    ApprovalMode,
    ModelCallStatus,
    RunStatus,
    SessionStatus,
    ToolCallStatus,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

type NonEmptyString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
type IdentifierString = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
]
type ToolName = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=100,
        pattern=r"^[a-z][a-z0-9_]*$",
    ),
]
type Sha256Hex = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]

_TERMINAL_RUN_STATUSES = frozenset({RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED})
_STARTED_RUN_STATUSES = frozenset(
    {
        RunStatus.RUNNING,
        RunStatus.WAITING_APPROVAL,
        RunStatus.RETRY_PENDING,
        RunStatus.COMPLETED,
        RunStatus.FAILED,
    }
)
_ASSIGNED_RUN_STATUSES = frozenset({RunStatus.LEASED, RunStatus.RUNNING})
_UNASSIGNED_RUN_STATUSES = frozenset(
    {RunStatus.QUEUED, RunStatus.WAITING_APPROVAL, RunStatus.RETRY_PENDING, RunStatus.LOST}
)
_TERMINAL_TOOL_STATUSES = frozenset(
    {ToolCallStatus.COMPLETED, ToolCallStatus.FAILED, ToolCallStatus.CANCELLED}
)
_TERMINAL_MODEL_STATUSES = frozenset({ModelCallStatus.COMPLETED, ModelCallStatus.FAILED})


def canonical_argument_hash(arguments: JsonObject | FrozenJsonObject) -> str:
    """Hash canonical JSON tool arguments for idempotency comparisons."""

    json_arguments = (
        arguments.to_json_object() if isinstance(arguments, FrozenJsonObject) else arguments
    )
    encoded = json.dumps(
        json_arguments,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class Session(DomainModel):
    """An ongoing user conversation bound to one tenant workspace."""

    id: uuid.UUID
    tenant_id: uuid.UUID
    workspace_id: uuid.UUID
    status: SessionStatus
    approval_mode: ApprovalMode
    model_route: IdentifierString
    created_at: AwareTimestamp
    updated_at: AwareTimestamp

    @model_validator(mode="after")
    def validate_timestamps(self) -> Self:
        if self.updated_at < self.created_at:
            raise ValueError("updated_at may not precede created_at")
        return self


class Run(DomainModel):
    """A durable attempt to execute agent work for a session."""

    id: uuid.UUID
    session_id: uuid.UUID
    workspace_id: uuid.UUID
    status: RunStatus
    priority: int
    attempt: int = Field(ge=1)
    assigned_worker_id: IdentifierString | None = None
    lease_expires_at: AwareTimestamp | None = None
    last_checkpoint_id: uuid.UUID | None = None
    cancellation_requested: bool = False
    created_at: AwareTimestamp
    started_at: AwareTimestamp | None = None
    completed_at: AwareTimestamp | None = None

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> Self:
        """Revalidate copies and require the transition API for status changes."""

        if update is not None and "status" in update:
            requested_value = update["status"]
            try:
                requested_status = RunStatus(requested_value)
            except (TypeError, ValueError):
                requested_status = None
            if requested_status is not self.status:
                requested_display = (
                    requested_status.value if requested_status is not None else str(requested_value)
                )
                raise DomainOperationError(
                    code="run_status_update_requires_transition",
                    message="run status changes must use transition_run",
                    details={
                        "run_id": str(self.id),
                        "current_status": self.status.value,
                        "requested_status": requested_display,
                    },
                )
        return super().model_copy(update=update, deep=deep)

    @model_validator(mode="after")
    def validate_lifecycle(self) -> Self:
        if self.started_at is not None and self.started_at < self.created_at:
            raise ValueError("started_at may not precede created_at")
        comparison_start = self.started_at or self.created_at
        if self.completed_at is not None and self.completed_at < comparison_start:
            raise ValueError("completed_at may not precede the run start")

        if self.status in _STARTED_RUN_STATUSES and self.started_at is None:
            raise ValueError(f"{self.status.value} run must have started_at")
        if self.status in _ASSIGNED_RUN_STATUSES and self.assigned_worker_id is None:
            raise ValueError(f"{self.status.value} run must have assigned_worker_id")
        if self.status in _ASSIGNED_RUN_STATUSES and self.lease_expires_at is None:
            raise ValueError(f"{self.status.value} run must have lease_expires_at")
        if self.status in _UNASSIGNED_RUN_STATUSES and (
            self.assigned_worker_id is not None or self.lease_expires_at is not None
        ):
            raise ValueError(f"{self.status.value} run may not retain a worker lease")

        if self.status in _TERMINAL_RUN_STATUSES:
            if self.completed_at is None:
                raise ValueError(f"{self.status.value} run must have completed_at")
        elif self.completed_at is not None:
            raise ValueError("non-terminal run may not have completed_at")
        return self


class ToolCall(DomainModel):
    """A stable, replay-safe logical tool invocation."""

    id: IdentifierString
    run_id: uuid.UUID
    turn_number: int = Field(ge=1)
    tool_name: ToolName
    arguments: FrozenJsonObject
    argument_hash: Sha256Hex
    status: ToolCallStatus
    workspace_version: IdentifierString | None = None
    result: FrozenJsonObject | None = None
    error: ErrorDetail | None = None
    started_at: AwareTimestamp | None = None
    completed_at: AwareTimestamp | None = None

    @model_validator(mode="after")
    def validate_execution(self) -> Self:
        expected_hash = canonical_argument_hash(self.arguments)
        if self.argument_hash != expected_hash:
            raise ValueError("argument_hash does not match canonical arguments")
        if (
            self.completed_at is not None
            and self.started_at is not None
            and self.completed_at < self.started_at
        ):
            raise ValueError("completed_at may not precede started_at")
        if (
            self.status in {ToolCallStatus.RUNNING, ToolCallStatus.COMPLETED}
            and self.started_at is None
        ):
            raise ValueError(f"{self.status.value} tool call must have started_at")
        if self.status in _TERMINAL_TOOL_STATUSES:
            if self.completed_at is None:
                raise ValueError(f"{self.status.value} tool call must have completed_at")
        elif self.completed_at is not None:
            raise ValueError("non-terminal tool call may not have completed_at")
        if self.status is ToolCallStatus.COMPLETED:
            if self.result is None or self.error is not None:
                raise ValueError("completed tool call requires only a result")
        elif self.status in {ToolCallStatus.FAILED, ToolCallStatus.CANCELLED}:
            if self.error is None or self.result is not None:
                raise ValueError("failed or cancelled tool call requires only an error")
        elif self.result is not None or self.error is not None:
            raise ValueError("non-terminal tool call may not have a result or error")
        return self


class Checkpoint(DomainModel):
    """A recoverable conversation and workspace snapshot."""

    id: uuid.UUID
    run_id: uuid.UUID
    session_id: uuid.UUID
    message_sequence: int = Field(ge=0)
    workspace_snapshot_uri: NonEmptyString
    workspace_revision: IdentifierString
    task_plan: FrozenJsonObject
    context_summary: str | None = None
    created_at: AwareTimestamp


class ModelCall(DomainModel):
    """Normalized accounting state for one gateway request."""

    id: IdentifierString
    run_id: uuid.UUID
    request_id: IdentifierString
    route_name: IdentifierString
    provider: IdentifierString | None = None
    model: IdentifierString | None = None
    status: ModelCallStatus
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    cached_tokens: int | None = Field(default=None, ge=0)
    estimated_cost_usd: decimal.Decimal | None = Field(default=None, ge=0)
    retry_count: int = Field(ge=0)
    fallback_count: int = Field(ge=0)
    started_at: AwareTimestamp
    first_token_at: AwareTimestamp | None = None
    completed_at: AwareTimestamp | None = None

    @model_validator(mode="after")
    def validate_lifecycle(self) -> Self:
        if self.first_token_at is not None and self.first_token_at < self.started_at:
            raise ValueError("first_token_at may not precede started_at")
        if self.completed_at is not None and self.completed_at < self.started_at:
            raise ValueError("completed_at may not precede started_at")
        if (
            self.first_token_at is not None
            and self.completed_at is not None
            and self.first_token_at > self.completed_at
        ):
            raise ValueError("first_token_at may not follow completed_at")
        if self.status is ModelCallStatus.STREAMING and self.first_token_at is None:
            raise ValueError("streaming model call must have first_token_at")
        if self.status in _TERMINAL_MODEL_STATUSES:
            if self.completed_at is None:
                raise ValueError(f"{self.status.value} model call must have completed_at")
        elif self.completed_at is not None:
            raise ValueError("non-terminal model call may not have completed_at")
        return self
