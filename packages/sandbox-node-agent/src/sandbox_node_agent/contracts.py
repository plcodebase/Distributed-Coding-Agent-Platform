"""Closed wire contracts for the sandbox node-agent boundary."""

from __future__ import annotations

import uuid  # noqa: TC003 - Pydantic resolves UUIDs at runtime
from typing import Annotated, Literal

from pydantic import Field, StringConstraints, model_validator

from agent_core.artifacts import ObjectKey  # noqa: TC001 - Pydantic runtime field
from agent_core.domain.base import DomainModel, FrozenJsonObject
from agent_core.domain.models import (  # noqa: TC001 - runtime fields
    IdentifierString,
    Sha256Hex,
)
from agent_core.gateway import GatewayToolDefinition  # noqa: TC001 - Pydantic runtime field
from agent_core.sandbox import (  # noqa: TC001 - Pydantic runtime fields
    CommandEvent,
    CommandSpec,
    WorkspaceSnapshot,
)
from agent_core.tools import (  # noqa: TC001 - Pydantic runtime fields
    ToolEffect,
    ToolExecutionContext,
    ToolExecutionEvent,
)

type CapabilityToken = Annotated[
    str,
    StringConstraints(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$"),
]


class CreateSandboxRequest(DomainModel):
    """Lease-bound request to materialize one immutable workspace sandbox."""

    tenant_id: uuid.UUID
    session_id: uuid.UUID
    run_id: uuid.UUID
    execution_epoch: int = Field(default=1, ge=1)
    workspace_id: uuid.UUID
    worker_id: IdentifierString
    run_lease_token: uuid.UUID
    run_lease_generation: int = Field(ge=1)
    source_object_key: ObjectKey
    source_sha256: Sha256Hex
    source_size_bytes: int = Field(ge=1, le=256 * 1024 * 1024)
    restore_object_key: ObjectKey | None = None
    restore_sha256: Sha256Hex | None = None
    restore_size_bytes: int | None = Field(default=None, ge=1, le=256 * 1024 * 1024)

    @model_validator(mode="after")
    def validate_restore_reference(self) -> CreateSandboxRequest:
        values = (self.restore_object_key, self.restore_sha256, self.restore_size_bytes)
        if any(value is not None for value in values) and any(value is None for value in values):
            raise ValueError("restore object key, checksum, and size must be supplied together")
        return self


class NodeToolDefinition(DomainModel):
    definition: GatewayToolDefinition
    effect: ToolEffect


class CreateSandboxResponse(DomainModel):
    sandbox_id: uuid.UUID
    capability_token: CapabilityToken
    tools: tuple[NodeToolDefinition, ...] = Field(default=(), max_length=64)


class ExecuteCommandRequest(DomainModel):
    command: CommandSpec


class ReadFileRequest(DomainModel):
    path: Annotated[str, StringConstraints(min_length=1, max_length=4096)]
    max_bytes: int = Field(default=4 * 1024 * 1024, ge=1, le=100 * 1024 * 1024)


class ReadFileResponse(DomainModel):
    content_base64: Annotated[str, StringConstraints(max_length=140 * 1024 * 1024)]
    size_bytes: int = Field(ge=0, le=100 * 1024 * 1024)
    sha256: Sha256Hex


class WorkspacePatchRequest(DomainModel):
    max_bytes: int = Field(default=1024 * 1024, ge=1, le=1024 * 1024)


class WorkspacePatchResponse(DomainModel):
    content_base64: Annotated[str, StringConstraints(max_length=1400 * 1024)]
    size_bytes: int = Field(ge=0, le=1024 * 1024)
    sha256: Sha256Hex


class WriteFileRequest(DomainModel):
    path: Annotated[str, StringConstraints(min_length=1, max_length=4096)]
    content_base64: Annotated[str, StringConstraints(max_length=140 * 1024 * 1024)]
    sha256: Sha256Hex
    size_bytes: int = Field(ge=0, le=100 * 1024 * 1024)


class RestoreSnapshotRequest(DomainModel):
    snapshot: WorkspaceSnapshot


class NodeAgentError(DomainModel):
    code: Annotated[
        str,
        StringConstraints(min_length=1, max_length=100, pattern=r"^[a-z][a-z0-9_]*$"),
    ]
    message: Annotated[str, StringConstraints(min_length=1, max_length=1000)]
    retryable: bool = False


class NodeAgentErrorResponse(DomainModel):
    error: NodeAgentError


class CommandEnvelope(DomainModel):
    type: Literal["event"] = "event"
    event: CommandEvent


class CommandErrorEnvelope(DomainModel):
    type: Literal["error"] = "error"
    error: NodeAgentError


type CommandStreamEnvelope = CommandEnvelope | CommandErrorEnvelope


class ExecuteToolRequest(DomainModel):
    arguments: FrozenJsonObject
    context: ToolExecutionContext


class ToolEnvelope(DomainModel):
    type: Literal["event"] = "event"
    event: ToolExecutionEvent


class ToolErrorEnvelope(DomainModel):
    type: Literal["error"] = "error"
    error: NodeAgentError


type ToolStreamEnvelope = ToolEnvelope | ToolErrorEnvelope


__all__ = [
    "CapabilityToken",
    "CommandEnvelope",
    "CommandErrorEnvelope",
    "CommandStreamEnvelope",
    "CreateSandboxRequest",
    "CreateSandboxResponse",
    "ExecuteCommandRequest",
    "ExecuteToolRequest",
    "NodeAgentError",
    "NodeAgentErrorResponse",
    "NodeToolDefinition",
    "ReadFileRequest",
    "ReadFileResponse",
    "RestoreSnapshotRequest",
    "ToolEnvelope",
    "ToolErrorEnvelope",
    "ToolStreamEnvelope",
    "WorkspacePatchRequest",
    "WorkspacePatchResponse",
    "WriteFileRequest",
]
