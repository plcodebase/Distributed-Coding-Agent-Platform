"""Provider-neutral workspace and command-execution contracts."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Literal, Protocol, Self

from pydantic import Field, StringConstraints, model_validator

from agent_core.domain.base import DomainModel
from agent_core.domain.models import (  # noqa: TC001 - Pydantic resolves fields at runtime
    IdentifierString,
    NonEmptyString,
)
from agent_core.tools import (  # noqa: TC001 - Pydantic resolves fields at runtime
    ToolOutputChannel,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

type CommandArgument = Annotated[str, StringConstraints(min_length=1, max_length=16_384)]


class CommandSpec(DomainModel):
    """Validated argv-only command request with hard execution ceilings."""

    argv: tuple[CommandArgument, ...] = Field(min_length=1, max_length=256)
    cwd: Annotated[str, StringConstraints(min_length=1, max_length=4096)] = "."
    timeout_seconds: float = Field(gt=0, le=3600)
    max_output_bytes: int = Field(ge=1, le=104_857_600)

    @model_validator(mode="after")
    def validate_argv(self) -> Self:
        if any("\x00" in argument for argument in self.argv):
            raise ValueError("command arguments may not contain NUL bytes")
        if sum(len(argument.encode("utf-8")) for argument in self.argv) > 256 * 1024:
            raise ValueError("serialized command arguments exceed the byte limit")
        return self


class CommandOutput(DomainModel):
    """One observed stdout or stderr fragment."""

    type: Literal["output"] = "output"
    sequence: int = Field(ge=1)
    channel: ToolOutputChannel
    chunk: NonEmptyString


class CommandCompleted(DomainModel):
    """Terminal process outcome."""

    type: Literal["completed"] = "completed"
    exit_code: int
    timed_out: bool = False
    output_truncated: bool = False


type CommandEvent = CommandOutput | CommandCompleted


class WorkspaceSnapshot(DomainModel):
    """Opaque restorable workspace revision owned by a sandbox adapter."""

    id: IdentifierString
    uri: NonEmptyString
    revision: IdentifierString


class Sandbox(Protocol):
    """Filesystem, process, and snapshot boundary used by concrete tools."""

    def execute(self, command: CommandSpec) -> AsyncIterator[CommandEvent]:
        """Yield bounded output followed by one terminal command outcome."""
        ...

    async def read_file(self, path: str) -> bytes:
        """Read one contained workspace file."""
        ...

    async def write_file(self, path: str, content: bytes) -> None:
        """Atomically write one contained workspace file."""
        ...

    async def create_snapshot(self) -> WorkspaceSnapshot:
        """Capture the current workspace state."""
        ...

    async def restore_snapshot(self, snapshot: WorkspaceSnapshot) -> None:
        """Cancel active work and restore a prior state."""
        ...

    async def cancel_active(self) -> None:
        """Terminate all active commands."""
        ...

    async def destroy(self) -> None:
        """Release all workspace and process resources idempotently."""
        ...


__all__ = [
    "CommandCompleted",
    "CommandEvent",
    "CommandOutput",
    "CommandSpec",
    "Sandbox",
    "WorkspaceSnapshot",
]
