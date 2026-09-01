"""Bounded in-memory checkpoints backed by serialized Git workspace operations."""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, replace
from enum import StrEnum
from itertools import islice
from typing import TYPE_CHECKING

from agent_core.checkpoints import RewindState
from agent_core.domain.base import FrozenJsonObject
from agent_core.domain.errors import DomainOperationError
from agent_core.domain.models import Checkpoint
from agent_core.gateway import GatewayMessage

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from pydantic import JsonValue

    from agent_core.loop import Clock
    from agent_core.sandbox import WorkspaceSnapshot
    from sandbox_runtime.git_workspace import GitWorktreeWorkspace

_MAX_TOOL_CALL_ID_CHARACTERS = 255
_DEFAULT_MAX_CHECKPOINTS = 1000
_DEFAULT_MAX_MESSAGES = 4096
_DEFAULT_MAX_STATE_BYTES = 4 * 1024 * 1024


class _CheckpointState(StrEnum):
    OPEN = "open"
    COMPLETED = "completed"
    ROLLED_BACK = "rolled_back"


@dataclass(frozen=True, slots=True)
class _StoredCheckpoint:
    checkpoint: Checkpoint
    messages: tuple[GatewayMessage, ...]
    tool_call_id: str
    state: _CheckpointState = _CheckpointState.OPEN
    completed_revision: str | None = None


def _checkpoint_error(
    code: str,
    message: str,
    *,
    checkpoint_id: uuid.UUID | None = None,
    retryable: bool = False,
) -> DomainOperationError:
    details: dict[str, JsonValue] = {}
    if checkpoint_id is not None:
        details["checkpoint_id"] = str(checkpoint_id)
    if retryable:
        details["retryable"] = True
    return DomainOperationError(code=code, message=message, details=details or None)


def _positive_limit(name: str, value: int) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _validate_tool_call_id(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or "\x00" in value
        or len(value) > _MAX_TOOL_CALL_ID_CHARACTERS
    ):
        raise _checkpoint_error(
            "checkpoint_tool_call_invalid",
            "checkpoint tool-call identity must be non-empty bounded text",
        )
    return value


class InMemoryCheckpointCoordinator:
    """Serialize and bound checkpoints for one active run and workspace."""

    def __init__(
        self,
        *,
        run_id: uuid.UUID,
        session_id: uuid.UUID,
        workspace: GitWorktreeWorkspace,
        clock: Clock,
        id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
        cancel_active: Callable[[], Awaitable[None]] | None = None,
        max_checkpoints: int = _DEFAULT_MAX_CHECKPOINTS,
        max_messages: int = _DEFAULT_MAX_MESSAGES,
        max_state_bytes: int = _DEFAULT_MAX_STATE_BYTES,
    ) -> None:
        if not isinstance(run_id, uuid.UUID) or not isinstance(session_id, uuid.UUID):
            raise TypeError("run_id and session_id must be UUID values")
        if not callable(id_factory):
            raise TypeError("id_factory must be callable")
        if cancel_active is not None and not callable(cancel_active):
            raise TypeError("cancel_active must be callable")
        self._run_id = run_id
        self._session_id = session_id
        self._workspace = workspace
        self._clock = clock
        self._id_factory = id_factory
        self._cancel_active = cancel_active
        self._max_checkpoints = _positive_limit("max_checkpoints", max_checkpoints)
        self._max_messages = _positive_limit("max_messages", max_messages)
        self._max_state_bytes = _positive_limit("max_state_bytes", max_state_bytes)
        self._records: dict[uuid.UUID, _StoredCheckpoint] = {}
        self._order: list[uuid.UUID] = []
        self._lock = asyncio.Lock()

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
            raise _checkpoint_error(
                "checkpoint_run_mismatch",
                "the checkpoint coordinator belongs to a different run",
            )
        validated_tool_call_id = _validate_tool_call_id(tool_call_id)
        stored_messages = self._validate_state(
            messages=messages,
            task_plan=task_plan,
            context_summary=context_summary,
        )
        async with self._lock:
            if len(self._records) >= self._max_checkpoints:
                raise _checkpoint_error(
                    "checkpoint_limit",
                    "the in-memory checkpoint limit has been reached",
                )
            if any(
                record.tool_call_id == validated_tool_call_id for record in self._records.values()
            ):
                raise _checkpoint_error(
                    "checkpoint_tool_call_conflict",
                    "a checkpoint already exists for this tool call",
                )
            checkpoint_id = self._id_factory()
            if not isinstance(checkpoint_id, uuid.UUID):
                raise _checkpoint_error(
                    "checkpoint_id_invalid",
                    "the checkpoint identifier factory returned an invalid value",
                )
            if checkpoint_id in self._records:
                raise _checkpoint_error(
                    "checkpoint_id_conflict",
                    "the checkpoint identifier already exists",
                    checkpoint_id=checkpoint_id,
                )
            snapshot = await self._create_snapshot_cancellation_safe(
                label=f"checkpoint before {validated_tool_call_id}"
            )
            checkpoint = Checkpoint(
                id=checkpoint_id,
                run_id=run_id,
                session_id=self._session_id,
                tool_call_id=validated_tool_call_id,
                message_sequence=len(stored_messages),
                workspace_snapshot_uri=snapshot.uri,
                workspace_revision=snapshot.revision,
                task_plan=task_plan,
                context_summary=context_summary,
                created_at=self._clock.now(),
            )
            self._records[checkpoint.id] = _StoredCheckpoint(
                checkpoint=checkpoint,
                messages=stored_messages,
                tool_call_id=validated_tool_call_id,
            )
            self._order.append(checkpoint.id)
            return checkpoint

    async def complete_tool(
        self,
        checkpoint: Checkpoint,
        *,
        tool_call_id: str,
    ) -> str:
        validated_tool_call_id = _validate_tool_call_id(tool_call_id)
        async with self._lock:
            record = self._require_exact(checkpoint)
            self._require_tool_call(record, validated_tool_call_id)
            if record.state is _CheckpointState.COMPLETED:
                if record.completed_revision is None:
                    raise AssertionError("completed checkpoint must retain its revision")
                return record.completed_revision
            if record.state is _CheckpointState.ROLLED_BACK:
                raise _checkpoint_error(
                    "checkpoint_state_conflict",
                    "a rolled-back checkpoint cannot be completed",
                    checkpoint_id=checkpoint.id,
                )

            commit_task = asyncio.create_task(
                asyncio.to_thread(
                    self._workspace.commit_state,
                    label=f"tool {validated_tool_call_id}",
                )
            )
            try:
                revision = await asyncio.shield(commit_task)
            except asyncio.CancelledError as cancelled:
                commit_error: Exception | None = None
                try:
                    await commit_task
                except Exception as error:  # commit may fail after cancellation
                    commit_error = error
                try:
                    await self._restore_record(record)
                except Exception as restore_error:
                    raise _checkpoint_error(
                        "checkpoint_restore_failed",
                        "workspace restoration failed after checkpoint cancellation",
                        checkpoint_id=checkpoint.id,
                        retryable=True,
                    ) from restore_error
                self._records[checkpoint.id] = replace(
                    record,
                    state=_CheckpointState.ROLLED_BACK,
                )
                if commit_error is not None:
                    raise cancelled from commit_error
                raise
            self._records[checkpoint.id] = replace(
                record,
                state=_CheckpointState.COMPLETED,
                completed_revision=revision,
            )
            return revision

    async def rollback(self, checkpoint: Checkpoint) -> None:
        async with self._lock:
            record = self._require_exact(checkpoint)
            cancelled = await self._restore_record(record)
            self._records[checkpoint.id] = replace(
                record,
                state=_CheckpointState.ROLLED_BACK,
                completed_revision=None,
            )
            if cancelled is not None:
                raise cancelled

    async def rewind(self, checkpoint_id: uuid.UUID) -> RewindState:
        async with self._lock:
            record = self._require_known(checkpoint_id)
            cancelled = await self._restore_record(record)
            self._records[checkpoint_id] = replace(
                record,
                state=_CheckpointState.ROLLED_BACK,
                completed_revision=None,
            )
            self._truncate_after(checkpoint_id)
            state = RewindState(
                checkpoint=record.checkpoint,
                messages=record.messages,
                task_plan=record.checkpoint.task_plan,
                context_summary=record.checkpoint.context_summary,
                workspace_revision=record.checkpoint.workspace_revision,
            )
            if cancelled is not None:
                raise cancelled
            return state

    def _validate_state(
        self,
        *,
        messages: Sequence[GatewayMessage],
        task_plan: FrozenJsonObject,
        context_summary: str | None,
    ) -> tuple[GatewayMessage, ...]:
        if not isinstance(task_plan, FrozenJsonObject):
            raise _checkpoint_error(
                "checkpoint_state_invalid",
                "checkpoint task-plan state must be immutable JSON",
            )
        if context_summary is not None and not isinstance(context_summary, str):
            raise _checkpoint_error(
                "checkpoint_state_invalid",
                "checkpoint context summary must be text",
            )
        if context_summary is not None and (
            len(context_summary.encode("utf-8")) > self._max_state_bytes
        ):
            raise _checkpoint_error(
                "checkpoint_state_limit",
                "checkpoint context summary exceeds the configured byte limit",
            )
        stored_messages = tuple(islice(messages, self._max_messages + 1))
        if len(stored_messages) > self._max_messages:
            raise _checkpoint_error(
                "checkpoint_state_limit",
                "checkpoint messages exceed the configured count limit",
            )
        if not all(isinstance(message, GatewayMessage) for message in stored_messages):
            raise _checkpoint_error(
                "checkpoint_state_invalid",
                "checkpoint messages must use the gateway message contract",
            )
        payload: dict[str, object] = {
            "messages": [message.model_dump(mode="json") for message in stored_messages],
            "task_plan": task_plan.to_json_object(),
            "context_summary": context_summary,
        }
        size = len(
            json.dumps(
                payload,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        )
        if size > self._max_state_bytes:
            raise _checkpoint_error(
                "checkpoint_state_limit",
                "checkpoint state exceeds its configured UTF-8 byte limit",
            )
        return stored_messages

    async def _create_snapshot_cancellation_safe(
        self,
        *,
        label: str,
    ) -> WorkspaceSnapshot:
        task: asyncio.Task[WorkspaceSnapshot] = asyncio.create_task(
            asyncio.to_thread(self._workspace.create_snapshot, label=label)
        )
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def _restore_record(
        self,
        record: _StoredCheckpoint,
    ) -> asyncio.CancelledError | None:
        cancelled: asyncio.CancelledError | None = None
        if self._cancel_active is not None:
            cancel_task: asyncio.Future[None] = asyncio.ensure_future(self._cancel_active())
            try:
                await asyncio.shield(cancel_task)
            except asyncio.CancelledError as error:
                cancelled = error
                await cancel_task
        task = asyncio.create_task(
            asyncio.to_thread(
                self._workspace.restore_revision,
                record.checkpoint.workspace_revision,
            )
        )
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            cancelled = error
            await task
        return cancelled

    def _require_exact(self, checkpoint: Checkpoint) -> _StoredCheckpoint:
        record = self._require_known(checkpoint.id)
        if checkpoint != record.checkpoint:
            raise _checkpoint_error(
                "checkpoint_identity_mismatch",
                "the checkpoint does not match its stored immutable state",
                checkpoint_id=checkpoint.id,
            )
        return record

    @staticmethod
    def _require_tool_call(record: _StoredCheckpoint, tool_call_id: str) -> None:
        if record.tool_call_id != tool_call_id:
            raise _checkpoint_error(
                "checkpoint_tool_call_mismatch",
                "the checkpoint belongs to a different tool call",
                checkpoint_id=record.checkpoint.id,
            )

    def _require_known(self, checkpoint_id: uuid.UUID) -> _StoredCheckpoint:
        record = self._records.get(checkpoint_id)
        if record is None:
            raise _checkpoint_error(
                "checkpoint_not_found",
                "the requested checkpoint is not available",
                checkpoint_id=checkpoint_id,
            )
        return record

    def _truncate_after(self, checkpoint_id: uuid.UUID) -> None:
        index = self._order.index(checkpoint_id)
        removed = self._order[index + 1 :]
        for removed_id in removed:
            self._records.pop(removed_id, None)
        del self._order[index + 1 :]


__all__ = ["InMemoryCheckpointCoordinator"]
