"""Agent-loop execution with idempotent durable event and tool-result writes."""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from agent_core.distributed import RunExecutionResult, RunLease, RunRecoveryState
from agent_core.domain.errors import ErrorDetail
from agent_core.domain.models import ToolCall
from agent_core.domain.status import RunStatus, ToolCallStatus
from agent_core.event_store import EventDraft
from agent_core.events import (
    AnyAgentEvent,
    CheckpointCreatedEvent,
    ModelRequestStartedEvent,
    ModelToolCallReceivedEvent,
    RunCompletedEvent,
    RunFailedEvent,
    RunRetryScheduledEvent,
    ToolApprovalRequiredEvent,
    ToolCompletedEvent,
    ToolStartedEvent,
)
from agent_core.loop import AgentLoop, AgentLoopInput

if TYPE_CHECKING:
    import uuid
    from collections.abc import Awaitable, Callable

    from agent_core.event_store import IdempotentEventStore


class ToolCallStore(Protocol):
    """Minimal durable tool-call boundary required for recovery replay."""

    async def save_tool_call(self, tenant_id: uuid.UUID, tool_call: ToolCall) -> ToolCall:
        """Create or monotonically advance one logical tool invocation."""


type AgentLoopFactory = Callable[[RunLease, RunRecoveryState], AgentLoop | Awaitable[AgentLoop]]


@dataclass(slots=True)
class _ObservedTool:
    call: ToolCall


class AgentLoopRunExecutor:
    """Execute AgentLoop attempts while making worker delivery retry-safe."""

    def __init__(
        self,
        *,
        loop_factory: AgentLoopFactory,
        events: IdempotentEventStore,
        tool_calls: ToolCallStore,
    ) -> None:
        if not callable(loop_factory):
            raise TypeError("loop_factory must be callable")
        self._loop_factory = loop_factory
        self._events = events
        self._tool_calls = tool_calls
        self._active: dict[uuid.UUID, asyncio.Task[object]] = {}
        self._cancel_requested: set[uuid.UUID] = set()
        self._lock = asyncio.Lock()

    async def execute(
        self,
        lease: RunLease,
        recovery: RunRecoveryState,
    ) -> RunExecutionResult:
        current = asyncio.current_task()
        if current is None:
            raise RuntimeError("agent execution requires an asyncio task")
        async with self._lock:
            if lease.lease_token in self._active:
                raise RuntimeError("the lease is already executing in this worker")
            self._active[lease.lease_token] = current
        try:
            loop = self._loop_factory(lease, recovery)
            if inspect.isawaitable(loop):
                loop = await loop
            if not isinstance(loop, AgentLoop):
                raise TypeError("loop_factory must return AgentLoop")
            return await self._run_loop(loop, lease, recovery)
        except asyncio.CancelledError:
            async with self._lock:
                distributed_cancel = lease.lease_token in self._cancel_requested
            if distributed_cancel:
                return RunExecutionResult(status=RunStatus.CANCELLED)
            raise
        finally:
            async with self._lock:
                self._active.pop(lease.lease_token, None)
                self._cancel_requested.discard(lease.lease_token)

    async def cancel(self, lease: RunLease) -> None:
        async with self._lock:
            task = self._active.get(lease.lease_token)
            if task is None:
                return
            self._cancel_requested.add(lease.lease_token)
            task.cancel()

    async def _run_loop(
        self,
        loop: AgentLoop,
        lease: RunLease,
        recovery: RunRecoveryState,
    ) -> RunExecutionResult:
        observed: dict[str, _ObservedTool] = {}
        turn_number = 0
        last_checkpoint_id = recovery.checkpoint.id if recovery.checkpoint is not None else None
        deferred_status: RunStatus | None = None
        loop_input = AgentLoopInput(
            tenant_id=lease.tenant_id,
            session_id=lease.session_id,
            run_id=lease.run_id,
            attempt=lease.attempt,
            worker_id=lease.worker_id,
            route_name=lease.route_name,
            messages=recovery.messages,
            checkpoint_id=last_checkpoint_id,
            task_plan=recovery.task_plan,
            context_summary=recovery.context_summary,
            prior_tool_outcomes=recovery.prior_tool_outcomes,
        )
        async for event in loop.run(loop_input):
            if isinstance(event, ModelRequestStartedEvent):
                turn_number += 1
            await self._persist_tool_state(
                lease,
                event,
                observed=observed,
                turn_number=turn_number,
            )
            await self._events.append_idempotent(
                lease.tenant_id,
                lease.run_id,
                self._delivery_key(lease, event),
                EventDraft(
                    event_type=event.event_type,
                    payload=event.payload.model_dump(mode="json"),
                    created_at=event.created_at,
                ),
            )
            if isinstance(event, CheckpointCreatedEvent):
                last_checkpoint_id = event.payload.checkpoint_id
            elif isinstance(event, ToolApprovalRequiredEvent):
                deferred_status = RunStatus.WAITING_APPROVAL
            elif isinstance(event, RunRetryScheduledEvent):
                deferred_status = RunStatus.RETRY_PENDING
            elif isinstance(event, RunCompletedEvent):
                return RunExecutionResult(
                    status=RunStatus.COMPLETED,
                    last_checkpoint_id=event.payload.checkpoint_id or last_checkpoint_id,
                )
            elif isinstance(event, RunFailedEvent):
                return RunExecutionResult(
                    status=RunStatus.FAILED,
                    last_checkpoint_id=last_checkpoint_id,
                    error=event.payload.error,
                )
        if deferred_status is not None:
            return RunExecutionResult(
                status=deferred_status,
                last_checkpoint_id=last_checkpoint_id,
            )
        return RunExecutionResult(
            status=RunStatus.FAILED,
            last_checkpoint_id=last_checkpoint_id,
            error=ErrorDetail(
                code="agent_loop_incomplete",
                message="the agent loop ended without a terminal result",
                retryable=True,
            ),
        )

    async def _persist_tool_state(
        self,
        lease: RunLease,
        event: AnyAgentEvent,
        *,
        observed: dict[str, _ObservedTool],
        turn_number: int,
    ) -> None:
        if isinstance(event, ModelToolCallReceivedEvent):
            await self._persist_received(
                lease,
                event,
                observed=observed,
                turn_number=turn_number,
            )
        elif isinstance(event, ToolApprovalRequiredEvent):
            await self._persist_approval(lease, event, observed=observed)
        elif isinstance(event, ToolStartedEvent):
            await self._persist_started(lease, event, observed=observed)
        elif isinstance(event, ToolCompletedEvent):
            await self._persist_completed(lease, event, observed=observed)

    async def _persist_received(
        self,
        lease: RunLease,
        event: ModelToolCallReceivedEvent,
        *,
        observed: dict[str, _ObservedTool],
        turn_number: int,
    ) -> None:
        payload = event.payload
        if payload.arguments is None or payload.argument_hash is None or payload.error is not None:
            return
        call = ToolCall(
            id=payload.tool_call_id,
            run_id=lease.run_id,
            turn_number=max(1, turn_number),
            tool_name=payload.tool_name,
            arguments=payload.arguments,
            argument_hash=payload.argument_hash,
            status=ToolCallStatus.RECEIVED,
        )
        durable = await self._tool_calls.save_tool_call(lease.tenant_id, call)
        observed[call.id] = _ObservedTool(call=durable)

    async def _persist_approval(
        self,
        lease: RunLease,
        event: ToolApprovalRequiredEvent,
        *,
        observed: dict[str, _ObservedTool],
    ) -> None:
        item = observed.get(event.payload.tool_call_id)
        if item is None or item.call.status is not ToolCallStatus.RECEIVED:
            return
        call = item.call.model_copy(update={"status": ToolCallStatus.WAITING_APPROVAL})
        item.call = await self._tool_calls.save_tool_call(lease.tenant_id, call)

    async def _persist_started(
        self,
        lease: RunLease,
        event: ToolStartedEvent,
        *,
        observed: dict[str, _ObservedTool],
    ) -> None:
        item = observed.get(event.payload.tool_call_id)
        if item is None or item.call.status not in {
            ToolCallStatus.RECEIVED,
            ToolCallStatus.WAITING_APPROVAL,
        }:
            return
        call = item.call.model_copy(
            update={
                "status": ToolCallStatus.RUNNING,
                "started_at": event.created_at,
            }
        )
        item.call = await self._tool_calls.save_tool_call(lease.tenant_id, call)

    async def _persist_completed(
        self,
        lease: RunLease,
        event: ToolCompletedEvent,
        *,
        observed: dict[str, _ObservedTool],
    ) -> None:
        item = observed.get(event.payload.tool_call_id)
        if item is None:
            return
        error = event.payload.error
        if event.payload.status is ToolCallStatus.CANCELLED and error is None:
            error = ErrorDetail(
                code="tool_cancelled",
                message="tool execution was cancelled",
                retryable=True,
            )
        workspace_version: str | None = None
        if event.payload.result is not None:
            candidate = event.payload.result.get("workspace_revision")
            if isinstance(candidate, str):
                workspace_version = candidate
        call = item.call.model_copy(
            update={
                "status": event.payload.status,
                "workspace_version": workspace_version,
                "result": event.payload.result,
                "error": error,
                "completed_at": event.created_at,
            }
        )
        item.call = await self._tool_calls.save_tool_call(lease.tenant_id, call)

    @staticmethod
    def _delivery_key(lease: RunLease, event: AnyAgentEvent) -> str:
        return f"a{lease.attempt}.g{lease.generation}.e{event.sequence}"


__all__ = ["AgentLoopFactory", "AgentLoopRunExecutor", "ToolCallStore"]
