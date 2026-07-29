"""Provider-neutral checkpoint contracts for reversible tool execution."""

from __future__ import annotations

import uuid  # noqa: TC003 - Pydantic resolves this field type at runtime
from typing import TYPE_CHECKING, Protocol

from agent_core.domain.base import DomainModel, FrozenJsonObject
from agent_core.domain.models import (  # noqa: TC001 - Pydantic resolves fields at runtime
    Checkpoint,
    IdentifierString,
)
from agent_core.gateway import (  # noqa: TC001 - Pydantic resolves fields at runtime
    GatewayMessage,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


class RewindState(DomainModel):
    """Conversation and workspace state restored from one checkpoint."""

    checkpoint: Checkpoint
    messages: tuple[GatewayMessage, ...]
    task_plan: FrozenJsonObject
    context_summary: str | None = None
    workspace_revision: IdentifierString


class CheckpointCoordinator(Protocol):
    """Create, finalize, and restore checkpoints around side-effecting tools."""

    async def create_before_tool(
        self,
        *,
        run_id: uuid.UUID,
        tool_call_id: str,
        messages: Sequence[GatewayMessage],
        task_plan: FrozenJsonObject,
        context_summary: str | None,
    ) -> Checkpoint:
        """Persist the exact pre-tool workspace and conversational state."""

    async def complete_tool(
        self,
        checkpoint: Checkpoint,
        *,
        tool_call_id: str,
    ) -> str:
        """Finalize successful workspace state and return its revision."""

    async def rollback(self, checkpoint: Checkpoint) -> None:
        """Restore the exact pre-tool workspace state after a failure."""

    async def rewind(self, checkpoint_id: uuid.UUID) -> RewindState:
        """Restore and return the complete state represented by a checkpoint."""


__all__ = ["CheckpointCoordinator", "RewindState"]
