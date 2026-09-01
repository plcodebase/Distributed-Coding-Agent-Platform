"""Closed HTTP request and response schemas."""

from __future__ import annotations

import uuid  # noqa: TC003 - Pydantic resolves API identifiers at runtime
from typing import Annotated

from pydantic import Field, StringConstraints, field_validator

from agent_core.artifacts import (  # noqa: TC001 - Pydantic resolves API models at runtime
    Artifact,
    PresignedDownload,
    PresignedUpload,
    SourceSnapshot,
)
from agent_core.control import (
    MAX_INITIAL_TASK_BYTES,
    MAX_REFERENCED_WORKSPACE_FILES,
    PersistedApproval,
    PersistedContextCompaction,
    PersistedMemory,
    PersistedTaskState,
    TrackedTask,
)
from agent_core.domain.base import DomainModel, FrozenJsonObject
from agent_core.domain.models import Run, Session, Sha256Hex
from agent_core.domain.status import ApprovalMode
from agent_core.event_store import MAX_EVENT_PAGE_SIZE, StoredEvent
from agent_core.scheduling import RunPriorityClass
from agent_core.workspace_access import WorkspaceFileReference  # noqa: TC001 - Pydantic field

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


class CreateWorkspaceRequest(DomainModel):
    display_name: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
    ]


class SnapshotUploadResponse(DomainModel):
    snapshot: SourceSnapshot
    upload: PresignedUpload


class FinalizeSnapshotRequest(DomainModel):
    sha256: Sha256Hex
    compressed_bytes: int = Field(ge=1)


class ArtifactListResponse(DomainModel):
    artifacts: tuple[Artifact, ...] = Field(max_length=500)


class ArtifactDownloadResponse(DomainModel):
    artifact: Artifact
    download: PresignedDownload


class MemorySettingRequest(DomainModel):
    enabled: bool


class MemoryListResponse(DomainModel):
    memories: tuple[PersistedMemory, ...] = Field(max_length=500)


class RunStatusResponse(DomainModel):
    run: Run
    task_plan: PersistedTaskState | None = None
    latest_compaction: PersistedContextCompaction | None = None


class CreateRunRequest(DomainModel):
    task: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=65_536),
    ]
    initial_tasks: tuple[TrackedTask, ...] = Field(default=(), max_length=500)
    referenced_files: tuple[WorkspaceFileReference, ...] = Field(
        default=(),
        max_length=MAX_REFERENCED_WORKSPACE_FILES,
    )
    priority: int = Field(default=0, ge=-100, le=100)
    priority_class: RunPriorityClass = RunPriorityClass.INTERACTIVE

    @field_validator("task")
    @classmethod
    def validate_task_bytes(cls, value: str) -> str:
        if len(value.encode("utf-8")) > MAX_INITIAL_TASK_BYTES:
            raise ValueError("initial task exceeds its UTF-8 byte limit")
        return value


class RunCreationResponse(DomainModel):
    run: Run
    created: bool


class ApprovalDecisionRequest(DomainModel):
    approved: bool
    response: (
        Annotated[
            str,
            StringConstraints(strip_whitespace=True, min_length=1, max_length=65_536),
        ]
        | None
    ) = None


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
    "ArtifactDownloadResponse",
    "ArtifactListResponse",
    "CreateRunRequest",
    "CreateSessionRequest",
    "CreateWorkspaceRequest",
    "ErrorResponse",
    "EventListResponse",
    "FinalizeSnapshotRequest",
    "HealthResponse",
    "MemoryListResponse",
    "MemorySettingRequest",
    "ModelRoute",
    "RewindRequest",
    "RunCreationResponse",
    "RunResponse",
    "RunStatusResponse",
    "SessionResponse",
    "SnapshotUploadResponse",
]
