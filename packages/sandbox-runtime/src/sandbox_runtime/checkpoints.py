"""In-memory conversation checkpoints backed by isolated Git revisions."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from agent_core.checkpoints import RewindState
from agent_core.domain.base import (  # noqa: TC001 - protocol annotations require runtime type
    FrozenJsonObject,
)
from agent_core.domain.errors import DomainOperationError
from agent_core.domain.models import Checkpoint

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from agent_core.gateway import GatewayMessage
    from agent_core.loop import Clock
    from sandbox_runtime.git_workspace import GitWorktreeWorkspace


@dataclass(frozen=True, slots=True)
class _StoredCheckpoint:
    checkpoint: Checkpoint
    messages: tuple[GatewayMessage, ...]


class InMemoryCheckpointCoordinator:
    """Checkpoint one active run without claiming cross-process durability."""

    def __init__(
        self,
        *,
        run_id: uuid.UUID,
        session_id: uuid.UUID,
        workspace: GitWorktreeWorkspace,
        clock: Clock,
        id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
        cancel_active: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._run_id = run_id
        self._session_id = session_id
        self._workspace = workspace
        self._clock = clock
        self._id_factory = id_factory
        self._cancel_active = cancel_active
        self._records: dict[uuid.UUID, _StoredCheckpoint] = {}

    async def create_before_tool(
        self,
        *,
        run_id: uuid.UUID,
        tool_call_id: str,
        messages: Sequence[GatewayMessage],
        task_plan: FrozenJsonObject,
        context_summary: str | None,
    ) -> Checkpoint:
        if run_id != self._run_id:
            raise DomainOperationError(
                code="checkpoint_run_mismatch",
                message="the checkpoint coordinator belongs to a different run",
            )
        snapshot = await asyncio.to_thread(
            self._workspace.create_snapshot,
            label=f"checkpoint before {tool_call_id}",
        )
        checkpoint = Checkpoint(
            id=self._id_factory(),
            run_id=run_id,
            session_id=self._session_id,
            message_sequence=len(messages),
            workspace_snapshot_uri=snapshot.uri,
            workspace_revision=snapshot.revision,
            task_plan=task_plan,
            context_summary=context_summary,
            created_at=self._clock.now(),
        )
        self._records[checkpoint.id] = _StoredCheckpoint(
            checkpoint=checkpoint,
            messages=tuple(messages),
        )
        return checkpoint

    async def complete_tool(
        self,
        checkpoint: Checkpoint,
        *,
        tool_call_id: str,
    ) -> str:
        self._require_known(checkpoint.id)
        return await asyncio.to_thread(
            self._workspace.commit_state,
            label=f"tool {tool_call_id}",
        )

    async def rollback(self, checkpoint: Checkpoint) -> None:
        self._require_known(checkpoint.id)
        if self._cancel_active is not None:
            await self._cancel_active()
        await asyncio.to_thread(
            self._workspace.restore_revision,
            checkpoint.workspace_revision,
        )

    async def rewind(self, checkpoint_id: uuid.UUID) -> RewindState:
        record = self._require_known(checkpoint_id)
        if self._cancel_active is not None:
            await self._cancel_active()
        await asyncio.to_thread(
            self._workspace.restore_revision,
            record.checkpoint.workspace_revision,
        )
        return RewindState(
            checkpoint=record.checkpoint,
            messages=record.messages,
            task_plan=record.checkpoint.task_plan,
            context_summary=record.checkpoint.context_summary,
            workspace_revision=record.checkpoint.workspace_revision,
        )

    def _require_known(self, checkpoint_id: uuid.UUID) -> _StoredCheckpoint:
        record = self._records.get(checkpoint_id)
        if record is None:
            raise DomainOperationError(
                code="checkpoint_not_found",
                message="the requested checkpoint is not available",
                details={"checkpoint_id": str(checkpoint_id)},
            )
        return record


__all__ = ["InMemoryCheckpointCoordinator"]
