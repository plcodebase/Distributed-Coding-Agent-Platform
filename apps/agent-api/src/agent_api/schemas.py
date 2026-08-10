"""Closed HTTP request and response schemas."""

from __future__ import annotations

import uuid  # noqa: TC003 - Pydantic resolves API identifiers at runtime
from typing import Annotated

from pydantic import Field, StringConstraints

from agent_core.control import (
    PersistedApproval,
    PersistedContextCompaction,
    PersistedMemory,
    PersistedTaskState,
)
from agent_core.domain.base import DomainModel, FrozenJsonObject
from agent_core.domain.models import Run, Session
from agent_core.domain.status import ApprovalMode
from agent_core.event_store import MAX_EVENT_PAGE_SIZE, StoredEvent
from agent_core.scheduling import RunPriorityClass

type ModelRoute = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=100,
        pattern=r"^[a-z][a-z0-9-]*$",
    ),
]


class CreateSessionRequest(DomainModel):
    workspace_id: uuid.UUID
    approval_mode: ApprovalMode = ApprovalMode.REQUIRE_SENSITIVE
    model_route: ModelRoute = "coding-default"
    memory_enabled: bool = True


class MemorySettingRequest(DomainModel):
    enabled: bool


class MemoryListResponse(DomainModel):
    memories: tuple[PersistedMemory, ...] = Field(max_length=500)


class RunStatusResponse(DomainModel):
    run: Run
    task_plan: PersistedTaskState | None = None
    latest_compaction: PersistedContextCompaction | None = None


class CreateRunRequest(DomainModel):
    priority: int = Field(default=0, ge=-100, le=100)
    priority_class: RunPriorityClass = RunPriorityClass.INTERACTIVE


class RunCreationResponse(DomainModel):
    run: Run
    created: bool


class ApprovalDecisionRequest(DomainModel):
    approved: bool


class RewindRequest(DomainModel):
    checkpoint_id: uuid.UUID


class EventListResponse(DomainModel):
    events: tuple[StoredEvent, ...] = Field(max_length=MAX_EVENT_PAGE_SIZE)
    next_after: int = Field(ge=0)
    has_more: bool


class HealthResponse(DomainModel):
    status: str


class ErrorResponse(DomainModel):
    error: FrozenJsonObject


type SessionResponse = Session
type RunResponse = Run
type ApprovalResponse = PersistedApproval


__all__ = [
    "ApprovalDecisionRequest",
    "ApprovalResponse",
    "CreateRunRequest",
    "CreateSessionRequest",
    "ErrorResponse",
    "EventListResponse",
    "HealthResponse",
    "MemoryListResponse",
    "MemorySettingRequest",
    "ModelRoute",
    "RewindRequest",
    "RunCreationResponse",
    "RunResponse",
    "RunStatusResponse",
    "SessionResponse",
]
