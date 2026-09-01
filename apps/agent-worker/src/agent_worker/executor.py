"""Agent-loop execution with idempotent durable event and tool-result writes."""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING, NoReturn, Protocol

from agent_core.control import ApprovalStatus, PersistedApproval
from agent_core.distributed import (
    RunExecutionResult,
    RunLease,
    RunRecoveryState,
    WorkspaceWriterLease,
)
from agent_core.domain.errors import DomainOperationError, ErrorDetail
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
from platform_telemetry import PlatformTelemetry, TelemetryContext

if TYPE_CHECKING:
    import uuid
    from collections.abc import Awaitable, Callable

    from agent_core.context import ContextBuildResult
    from agent_core.event_store import IdempotentEventStore


class RunContextBuilder(Protocol):
    """Build one bounded run context from durable recovery and workspace state."""

    async def build(
        self,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        recovery: RunRecoveryState,
    ) -> ContextBuildResult: ...


class ToolCallStore(Protocol):
    """Minimal durable tool-call boundary required for recovery replay."""

    async def save_tool_call_fenced(
        self,
        lease: RunLease,
        tool_call: ToolCall,
    ) -> ToolCall:
        """Create or advance one logical invocation only for the active run lease."""


class ApprovalRequestStore(Protocol):
    """Fenced durable approval requests emitted by an executing attempt."""

    async def create_approval_fenced(
        self,
        lease: RunLease,
        approval: PersistedApproval,
        *,
        tool_call_id: str,
    ) -> PersistedApproval: ...


type AgentLoopFactory = Callable[
    [RunLease, WorkspaceWriterLease, RunRecoveryState],
    AgentLoop | Awaitable[AgentLoop],
]
type AgentLoopCleanup = Callable[[RunLease, AgentLoop], Awaitable[None]]
type AgentLoopFinalizer = Callable[
    [RunLease, AgentLoop, RunExecutionResult],
    Awaitable[None],
]


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
        approvals: ApprovalRequestStore | None = None,
        context_builder: RunContextBuilder | None = None,
        attempt_finalizer: AgentLoopFinalizer | None = None,
        loop_cleanup: AgentLoopCleanup | None = None,
        telemetry: PlatformTelemetry | None = None,
    ) -> None:
        if not callable(loop_factory):
            raise TypeError("loop_factory must be callable")
        self._loop_factory = loop_factory
        self._events = events
        self._tool_calls = tool_calls
        self._approvals = approvals
        self._context_builder = context_builder
        self._attempt_finalizer = attempt_finalizer
        self._loop_cleanup = loop_cleanup
        self._telemetry = telemetry
        self._active: dict[uuid.UUID, asyncio.Task[object]] = {}
        self._cancel_requested: set[uuid.UUID] = set()
        self._lock = asyncio.Lock()

    async def execute(
        self,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        recovery: RunRecoveryState,
    ) -> RunExecutionResult:
        self._require_matching_writer(lease, writer_lease)
        current = asyncio.current_task()
        if current is None:
            raise RuntimeError("agent execution requires an asyncio task")
        async with self._lock:
            if lease.lease_token in self._active:
                raise RuntimeError("the lease is already executing in this worker")
            self._active[lease.lease_token] = current
        loop: AgentLoop | None = None
        primary_failure: BaseException | None = None
        try:
            candidate = self._loop_factory(lease, writer_lease, recovery)
            loop = await _resolve_loop(candidate)
            context = await self._build_context(lease, writer_lease, recovery)
            result = await self._run_loop(loop, lease, recovery, context=context)
            if self._attempt_finalizer is not None:
                await _cancellation_safe_finalization(
                    self._attempt_finalizer,
                    lease,
                    loop,
                    result,
                )
            return result  # noqa: TRY300 - cleanup must wrap a successfully finalized result
        except asyncio.CancelledError:
            async with self._lock:
                distributed_cancel = lease.lease_token in self._cancel_requested
            if distributed_cancel:
                return RunExecutionResult(status=RunStatus.CANCELLED)
            primary_failure = asyncio.CancelledError()
            raise
        except BaseException as error:
            primary_failure = error
            raise
        finally:
            if loop is not None and self._loop_cleanup is not None:
                try:
                    await _cancellation_safe_cleanup(self._loop_cleanup, lease, loop)
                except Exception as error:
                    if primary_failure is None:
                        raise DomainOperationError(
                            code="worker_attempt_cleanup_failed",
                            message="worker attempt resources could not be released",
                            retryable=True,
                        ) from error
            async with self._lock:
                self._active.pop(lease.lease_token, None)
                self._cancel_requested.discard(lease.lease_token)

    async def _build_context(
        self,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        recovery: RunRecoveryState,
    ) -> ContextBuildResult | None:
        if self._context_builder is None:
            return None
        if self._telemetry is None:
            return await self._context_builder.build(lease, writer_lease, recovery)
        started = asyncio.get_running_loop().time()
        with self._telemetry.span(
            "context.build",
            context=TelemetryContext(
                tenant_id=str(lease.tenant_id),
                session_id=str(lease.session_id),
                run_id=str(lease.run_id),
            ),
        ):
            result = await self._context_builder.build(lease, writer_lease, recovery)
        self._telemetry.metrics.context_build.observe(asyncio.get_running_loop().time() - started)
        return result

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
        *,
        context: ContextBuildResult | None,
    ) -> RunExecutionResult:
        observed: dict[str, _ObservedTool] = {}
        turn_number = 0
        last_checkpoint_id = recovery.checkpoint.id if recovery.checkpoint is not None else None
        deferred_status: RunStatus | None = None
        retry_delay_seconds: float | None = None
        loop_input = AgentLoopInput(
            tenant_id=lease.tenant_id,
            session_id=lease.session_id,
            run_id=lease.run_id,
            attempt=lease.attempt,
            execution_epoch=lease.execution_epoch,
            worker_id=lease.worker_id,
            route_name=lease.route_name,
            messages=context.messages if context is not None else recovery.messages,
            checkpoint_id=last_checkpoint_id,
            task_plan=recovery.task_plan,
            context_summary=(
                context.summary
                if context is not None and context.summary is not None
                else recovery.context_summary
            ),
            prior_tool_outcomes=recovery.prior_tool_outcomes,
            approval_mode=recovery.approval_mode,
            pending_tool_calls=recovery.pending_tool_calls,
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
            await self._events.append_idempotent_fenced(
                lease,
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
                retry_delay_seconds = event.payload.delay_seconds
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
                retry_delay_seconds=retry_delay_seconds,
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
            execution_epoch=lease.execution_epoch,
            turn_number=max(1, turn_number),
            tool_name=payload.tool_name,
            arguments=payload.arguments,
            argument_hash=payload.argument_hash,
            status=ToolCallStatus.RECEIVED,
        )
        durable = await self._tool_calls.save_tool_call_fenced(lease, call)
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
        item.call = await self._tool_calls.save_tool_call_fenced(lease, call)
        if self._approvals is None:
            raise DomainOperationError(
                code="approval_store_unavailable",
                message="approval persistence is not configured for this worker",
                retryable=True,
            )
        await self._approvals.create_approval_fenced(
            lease,
            PersistedApproval(
                id=event.payload.approval_id,
                run_id=lease.run_id,
                execution_epoch=lease.execution_epoch,
                status=ApprovalStatus.PENDING,
                reason=event.payload.reason,
                arguments=event.payload.arguments,
                requested_at=event.created_at,
            ),
            tool_call_id=event.payload.tool_call_id,
        )

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
        item.call = await self._tool_calls.save_tool_call_fenced(lease, call)

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
        item.call = await self._tool_calls.save_tool_call_fenced(lease, call)

    @staticmethod
    def _delivery_key(lease: RunLease, event: AnyAgentEvent) -> str:
        return f"a{lease.attempt}.g{lease.generation}.e{event.sequence}"

    @staticmethod
    def _require_matching_writer(
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
    ) -> None:
        if (
            writer_lease.tenant_id != lease.tenant_id
            or writer_lease.workspace_id != lease.workspace_id
            or writer_lease.run_id != lease.run_id
            or writer_lease.worker_id != lease.worker_id
            or writer_lease.run_lease_token != lease.lease_token
            or writer_lease.expires_at > lease.expires_at
        ):
            raise DomainOperationError(
                code="workspace_lease_invalid",
                message="the executor workspace fence does not match the active run lease",
                retryable=True,
                details={"workspace_id": str(lease.workspace_id)},
            )


__all__ = [
    "AgentLoopCleanup",
    "AgentLoopFactory",
    "AgentLoopFinalizer",
    "AgentLoopRunExecutor",
    "ApprovalRequestStore",
    "RunContextBuilder",
    "ToolCallStore",
]


async def _cancellation_safe_cleanup(
    cleanup: AgentLoopCleanup,
    lease: RunLease,
    loop: AgentLoop,
) -> None:
    task: asyncio.Future[None] = asyncio.ensure_future(cleanup(lease, loop))
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


async def _cancellation_safe_finalization(
    finalizer: AgentLoopFinalizer,
    lease: RunLease,
    loop: AgentLoop,
    result: RunExecutionResult,
) -> None:
    task: asyncio.Future[None] = asyncio.ensure_future(finalizer(lease, loop, result))
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


def _raise_invalid_loop() -> NoReturn:
    raise TypeError("loop_factory must return AgentLoop")


async def _resolve_loop(candidate: object) -> AgentLoop:
    if isinstance(candidate, AgentLoop):
        return candidate
    if inspect.isawaitable(candidate):
        resolved = await candidate
        if isinstance(resolved, AgentLoop):
            return resolved
    _raise_invalid_loop()
