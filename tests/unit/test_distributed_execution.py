from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import ValidationError

from agent_core.capacity import QueueDepth, QueueSnapshot
from agent_core.context import (
    ContextBudgetRegistry,
    ContextBuildRequest,
    ContextBuildResult,
    ContextCompressionRequest,
    ContextCompressionResult,
    ContextMemorySnippet,
    ContextPipeline,
    ContextRouteBudget,
)
from agent_core.control import ContextCompactionStatus, PersistedContextCompaction
from agent_core.distributed import (
    DurableToolOutcome,
    RunExecutionResult,
    RunLease,
    RunLeaseHeartbeat,
    RunRecoveryState,
    WorkerRegistration,
    WorkerStatus,
    WorkspaceWriterLease,
)
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.domain.models import Checkpoint, Run, ToolCall, canonical_argument_hash
from agent_core.domain.status import RunStatus, ToolCallStatus
from agent_core.event_store import EventDraft, StoredEvent
from agent_core.events import EventType, RunFailedEvent, ToolCompletedEvent, parse_agent_event
from agent_core.fakes import (
    ScriptedGatewayTurn,
    ScriptedModelGateway,
    SequentialIdGenerator,
    SteppingClock,
)
from agent_core.gateway import GatewayMessage, GatewayToolCall, MessageRole
from agent_core.loop import AgentLoop, AgentLoopInput
from agent_core.tools import (
    RegisteredTool,
    ToolArguments,
    ToolEffect,
    ToolExecutionCompleted,
    ToolExecutionContext,
    ToolExecutionEvent,
    ToolRegistry,
)
from agent_scheduler import SchedulerConfig, SchedulerService
from agent_scheduler.process import load_scheduler_factory
from agent_worker import (
    DEFAULT_LOCAL_WORKER_PROCESSES,
    AgentLoopRunExecutor,
    DurableRunContextBuilder,
    WorkerConfig,
    WorkerService,
)
from agent_worker.process import _wait_for_processes, load_worker_factory, run_worker_fleet
from platform_telemetry import PlatformTelemetry, TelemetrySettings

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from agent_core.domain.base import JsonObject

NOW = datetime(2026, 7, 30, 12, tzinfo=UTC)
TENANT_ID = uuid.UUID("10000000-0000-0000-0000-000000000001")
SESSION_ID = uuid.UUID("20000000-0000-0000-0000-000000000002")
RUN_ID = uuid.UUID("30000000-0000-0000-0000-000000000003")
WORKSPACE_ID = uuid.UUID("40000000-0000-0000-0000-000000000004")
LEASE_TOKEN = uuid.UUID("50000000-0000-0000-0000-000000000005")
WORKSPACE_TOKEN = uuid.UUID("60000000-0000-0000-0000-000000000006")


def run_lease(*, worker_id: str = "worker-1", attempt: int = 1) -> RunLease:
    return RunLease(
        tenant_id=TENANT_ID,
        run_id=RUN_ID,
        session_id=SESSION_ID,
        workspace_id=WORKSPACE_ID,
        worker_id=worker_id,
        route_name="coding-default",
        lease_token=LEASE_TOKEN,
        generation=1,
        attempt=attempt,
        priority=0,
        acquired_at=NOW,
        expires_at=NOW + timedelta(seconds=30),
    )


def writer_lease(lease: RunLease | None = None) -> WorkspaceWriterLease:
    active = lease or run_lease()
    return WorkspaceWriterLease(
        tenant_id=active.tenant_id,
        workspace_id=active.workspace_id,
        run_id=active.run_id,
        worker_id=active.worker_id,
        run_lease_token=active.lease_token,
        lease_token=WORKSPACE_TOKEN,
        generation=1,
        acquired_at=active.acquired_at,
        expires_at=active.expires_at,
    )


def recovery_state(
    *,
    checkpoint: Checkpoint | None = None,
    outcomes: tuple[DurableToolOutcome, ...] = (),
) -> RunRecoveryState:
    return RunRecoveryState(
        checkpoint=checkpoint,
        workspace_restore_revision=(
            checkpoint.workspace_revision if checkpoint is not None else None
        ),
        messages=(GatewayMessage(role=MessageRole.USER, content="finish the task"),),
        prior_tool_outcomes=outcomes,
    )


def test_distributed_contracts_are_closed_fenced_and_bounded() -> None:
    registration = WorkerRegistration(
        worker_id="worker-1",
        supported_sandbox_types=("podman",),
        total_slots=3,
        available_slots=2,
        status=WorkerStatus.ACTIVE,
        registered_at=NOW,
        last_heartbeat_at=NOW,
    )
    assert registration.model_dump(mode="json")["supported_sandbox_types"] == ["podman"]
    with pytest.raises(ValidationError, match="unique"):
        registration.model_copy(update={"supported_sandbox_types": ("podman", "podman")})
    with pytest.raises(ValidationError, match="available_slots"):
        registration.model_copy(update={"available_slots": 4})
    with pytest.raises(ValidationError, match="expiry"):
        run_lease().model_copy(update={"expires_at": NOW})
    with pytest.raises(ValidationError, match="extra_forbidden"):
        RunLease.model_validate({**run_lease().model_dump(), "unknown": True})

    arguments: JsonObject = {"path": "README.md"}
    outcome = DurableToolOutcome(
        tool_call_id="call-1",
        tool_name="read_file",
        turn_number=1,
        argument_hash=canonical_argument_hash(arguments),
        status=ToolCallStatus.COMPLETED,
        result={"content": "stable"},
    )
    assert outcome.result is not None
    with pytest.raises(ValidationError, match="terminal"):
        outcome.model_copy(update={"status": ToolCallStatus.RUNNING})
    with pytest.raises(ValidationError, match="requires only a result"):
        outcome.model_copy(update={"result": None})
    with pytest.raises(ValidationError, match="must be unique"):
        recovery_state(outcomes=(outcome, outcome))
    with pytest.raises(ValidationError, match="exactly when"):
        RunRecoveryState(
            checkpoint=None,
            workspace_restore_revision="orphan-revision",
            messages=recovery_state().messages,
        )


class ReadArguments(ToolArguments):
    path: str


class CountingReadHandler:
    def __init__(self) -> None:
        self.calls = 0

    async def __call__(
        self,
        arguments: ReadArguments,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        del arguments, context
        self.calls += 1
        yield ToolExecutionCompleted(result={"content": "unexpected"})


@pytest.mark.asyncio
async def test_recovered_agent_loop_reuses_terminal_tool_outcome() -> None:
    arguments: JsonObject = {"path": "README.md"}
    call = GatewayToolCall(id="call-1", name="read_file", arguments=arguments)
    prior = DurableToolOutcome(
        tool_call_id=call.id,
        tool_name=call.name,
        turn_number=1,
        argument_hash=canonical_argument_hash(arguments),
        status=ToolCallStatus.COMPLETED,
        result={"content": "already durable"},
    )
    handler = CountingReadHandler()
    tools = ToolRegistry(
        (
            RegisteredTool(
                name="read_file",
                description="Read a file",
                arguments_type=ReadArguments,
                handler=handler,
                effect=ToolEffect.READ_ONLY,
            ),
        )
    )
    loop = AgentLoop(
        gateway=ScriptedModelGateway(
            (
                ScriptedGatewayTurn.tool_calls(call),
                ScriptedGatewayTurn.text("done"),
            )
        ),
        tools=tools,
        clock=SteppingClock(NOW),
        id_generator=SequentialIdGenerator(),
    )

    events = [
        event
        async for event in loop.run(
            AgentLoopInput(
                tenant_id=TENANT_ID,
                session_id=SESSION_ID,
                run_id=RUN_ID,
                attempt=2,
                worker_id="worker-2",
                route_name="coding-default",
                messages=recovery_state().messages,
                prior_tool_outcomes=(prior,),
            )
        )
    ]

    assert handler.calls == 0
    reused = [event for event in events if isinstance(event, ToolCompletedEvent)]
    assert len(reused) == 1
    assert reused[0].payload.result == prior.result


@pytest.mark.asyncio
async def test_recovered_outcome_rejects_same_id_and_hash_for_another_tool() -> None:
    arguments: JsonObject = {"path": "README.md"}
    prior = DurableToolOutcome(
        tool_call_id="call-1",
        tool_name="read_file",
        turn_number=1,
        argument_hash=canonical_argument_hash(arguments),
        status=ToolCallStatus.COMPLETED,
        result={"content": "already durable"},
    )
    handler = CountingReadHandler()
    loop = AgentLoop(
        gateway=ScriptedModelGateway(
            (
                ScriptedGatewayTurn.tool_calls(
                    GatewayToolCall(
                        id=prior.tool_call_id,
                        name="search_workspace",
                        arguments=arguments,
                    )
                ),
            )
        ),
        tools=ToolRegistry(
            (
                RegisteredTool(
                    name="search_workspace",
                    description="Search files",
                    arguments_type=ReadArguments,
                    handler=handler,
                    effect=ToolEffect.READ_ONLY,
                ),
            )
        ),
        clock=SteppingClock(NOW),
        id_generator=SequentialIdGenerator(),
    )

    events = [
        event
        async for event in loop.run(
            AgentLoopInput(
                tenant_id=TENANT_ID,
                session_id=SESSION_ID,
                run_id=RUN_ID,
                attempt=2,
                worker_id="worker-2",
                route_name="coding-default",
                messages=recovery_state().messages,
                prior_tool_outcomes=(prior,),
            )
        )
    ]

    assert handler.calls == 0
    failure = next(event for event in events if isinstance(event, RunFailedEvent))
    assert failure.payload.error.code == "tool_call_id_conflict"


class MemoryEventStore:
    def __init__(self) -> None:
        self.events: dict[str, StoredEvent] = {}

    async def append_idempotent_fenced(
        self,
        lease: RunLease,
        delivery_key: str,
        draft: EventDraft,
    ) -> StoredEvent:
        existing = self.events.get(delivery_key)
        if existing is not None:
            assert existing.event_type is draft.event_type
            assert existing.payload == draft.payload
            return existing
        stored = StoredEvent(
            run_id=lease.run_id,
            sequence=len(self.events) + 1,
            event_type=draft.event_type,
            payload=draft.payload,
            created_at=draft.created_at,
        )
        self.events[delivery_key] = stored
        return stored


class MemoryToolStore:
    def __init__(self) -> None:
        self.calls: list[ToolCall] = []

    async def save_tool_call_fenced(
        self,
        lease: RunLease,
        tool_call: ToolCall,
    ) -> ToolCall:
        assert tool_call.run_id == lease.run_id
        self.calls.append(tool_call)
        return tool_call


class PreloadedToolStore(MemoryToolStore):
    def __init__(self, durable: ToolCall) -> None:
        super().__init__()
        self.durable = durable

    async def save_tool_call_fenced(
        self,
        lease: RunLease,
        tool_call: ToolCall,
    ) -> ToolCall:
        assert tool_call.run_id == lease.run_id
        self.calls.append(tool_call)
        return self.durable


class ContextBuilderFake:
    def __init__(self) -> None:
        self.calls = 0

    async def build(
        self,
        lease: RunLease,
        workspace_lease: WorkspaceWriterLease,
        recovery: RunRecoveryState,
    ) -> ContextBuildResult:
        assert workspace_lease.run_id == lease.run_id
        assert recovery.messages
        self.calls += 1
        return ContextBuildResult(
            messages=(GatewayMessage(role=MessageRole.USER, content="bounded context"),),
            estimated_tokens=20,
            budget_tokens=100,
            compressed=True,
            summary="durable compacted context",
            compression_input_tokens=10,
            compression_output_tokens=4,
        )


class WorkerContextSourceFake:
    def __init__(self) -> None:
        self.watermark: int | None = None
        self.previous_summary: str | None = None

    async def load(
        self,
        lease: RunLease,
        workspace_lease: WorkspaceWriterLease,
        recovery: RunRecoveryState,
        *,
        source_message_sequence: int | None,
        previous_summary: str | None,
        force_compaction: bool,
    ) -> ContextBuildRequest:
        assert workspace_lease.run_id == lease.run_id
        self.watermark = source_message_sequence
        self.previous_summary = previous_summary
        return ContextBuildRequest(
            tenant_id=lease.tenant_id,
            session_id=lease.session_id,
            run_id=lease.run_id,
            route_name=lease.route_name,
            system_instructions="preserve active work",
            conversation=recovery.messages,
            memories=(ContextMemorySnippet(memory_id=uuid.uuid4(), content="historical detail"),),
            previous_summary=previous_summary,
            force_compaction=force_compaction,
        )


class WorkerContextCompressorFake:
    async def compress(self, request: ContextCompressionRequest) -> ContextCompressionResult:
        assert "historical detail" in request.source_text
        return ContextCompressionResult(
            summary="new durable summary",
            input_tokens=8,
            output_tokens=3,
        )


class WorkerCompactionStoreFake:
    def __init__(self) -> None:
        self.pending = PersistedContextCompaction(
            id=uuid.uuid4(),
            session_id=SESSION_ID,
            status=ContextCompactionStatus.PENDING,
            idempotency_key="compact-1",
            source_message_sequence=7,
            route_name="coding-default",
            requested_at=NOW,
        )
        self.completed: tuple[str, int, int] | None = None

    async def pending_for_session(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
    ) -> PersistedContextCompaction | None:
        assert tenant_id == TENANT_ID and session_id == SESSION_ID
        return self.pending

    async def latest_completed(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
    ) -> PersistedContextCompaction | None:
        assert tenant_id == TENANT_ID and session_id == SESSION_ID
        return None

    async def complete(
        self,
        tenant_id: uuid.UUID,
        compaction_id: uuid.UUID,
        *,
        summary: str,
        input_tokens: int,
        output_tokens: int,
        completed_at: datetime,
    ) -> PersistedContextCompaction | None:
        assert tenant_id == TENANT_ID and compaction_id == self.pending.id
        assert completed_at >= NOW
        self.completed = (summary, input_tokens, output_tokens)
        return self.pending.model_copy(
            update={
                "status": ContextCompactionStatus.COMPLETED,
                "summary": summary,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "completed_at": completed_at,
            }
        )

    async def fail(
        self,
        tenant_id: uuid.UUID,
        compaction_id: uuid.UUID,
        *,
        error: ErrorDetail,
        completed_at: datetime,
    ) -> PersistedContextCompaction | None:
        raise AssertionError((tenant_id, compaction_id, error, completed_at))


@pytest.mark.asyncio
async def test_agent_loop_executor_idempotently_persists_attempt_events() -> None:
    event_store = MemoryEventStore()
    tool_store = MemoryToolStore()

    def loop_factory(
        lease: RunLease,
        workspace_lease: WorkspaceWriterLease,
        recovery: RunRecoveryState,
    ) -> AgentLoop:
        del lease, workspace_lease, recovery
        return AgentLoop(
            gateway=ScriptedModelGateway((ScriptedGatewayTurn.text("complete"),)),
            tools=ToolRegistry(()),
            clock=SteppingClock(NOW),
            id_generator=SequentialIdGenerator(),
        )

    executor = AgentLoopRunExecutor(
        loop_factory=loop_factory,
        events=event_store,
        tool_calls=tool_store,
    )
    first = await executor.execute(run_lease(), writer_lease(), recovery_state())
    event_count = len(event_store.events)
    second = await executor.execute(run_lease(), writer_lease(), recovery_state())

    assert first.status is RunStatus.COMPLETED
    assert second == first
    assert len(event_store.events) == event_count
    assert set(event_store.events).issuperset({"a1.g1.e1", "a1.g1.e2"})


@pytest.mark.asyncio
async def test_executor_uses_bounded_context_without_precommit_side_effects() -> None:
    gateway = ScriptedModelGateway((ScriptedGatewayTurn.text("complete"),))
    context = ContextBuilderFake()

    executor = AgentLoopRunExecutor(
        loop_factory=lambda _lease, _writer, _recovery: AgentLoop(
            gateway=gateway,
            tools=ToolRegistry(()),
            clock=SteppingClock(NOW),
            id_generator=SequentialIdGenerator(),
        ),
        events=MemoryEventStore(),
        tool_calls=MemoryToolStore(),
        context_builder=context,
    )
    result = await executor.execute(run_lease(), writer_lease(), recovery_state())

    assert result.status is RunStatus.COMPLETED
    assert context.calls == 1
    assert gateway.requests[0].messages[0].content == "bounded context"


@pytest.mark.asyncio
async def test_durable_context_builder_honors_watermark_and_completes_request() -> None:
    source = WorkerContextSourceFake()
    compactions = WorkerCompactionStoreFake()
    builder = DurableRunContextBuilder(
        pipeline=ContextPipeline(
            budgets=ContextBudgetRegistry(
                (
                    ContextRouteBudget(
                        route_name="coding-default",
                        max_context_tokens=10_000,
                        reserved_output_tokens=1_000,
                    ),
                )
            ),
            compressor=WorkerContextCompressorFake(),
        ),
        source=source,
        compactions=compactions,
        clock=SteppingClock(NOW + timedelta(seconds=1)),
    )
    result = await builder.build(run_lease(), writer_lease(), recovery_state())

    assert source.watermark == 7
    assert result.summary == "new durable summary"
    assert compactions.completed == ("new durable summary", 8, 3)


@pytest.mark.asyncio
async def test_agent_loop_executor_rejects_a_mismatched_workspace_fence() -> None:
    executor = AgentLoopRunExecutor(
        loop_factory=lambda _lease, _writer, _recovery: cast("Any", None),
        events=MemoryEventStore(),
        tool_calls=MemoryToolStore(),
    )

    with pytest.raises(DomainOperationError) as error:
        await executor.execute(
            run_lease(),
            writer_lease().model_copy(update={"workspace_id": uuid.uuid4()}),
            recovery_state(),
        )

    assert error.value.code == "workspace_lease_invalid"


@pytest.mark.asyncio
async def test_agent_loop_executor_uses_later_durable_tool_state_on_replay() -> None:
    arguments: JsonObject = {"path": "README.md"}
    durable = ToolCall(
        id="call-1",
        run_id=RUN_ID,
        turn_number=1,
        tool_name="read_file",
        arguments=arguments,
        argument_hash=canonical_argument_hash(arguments),
        status=ToolCallStatus.COMPLETED,
        result={"content": "already durable"},
        started_at=NOW,
        completed_at=NOW + timedelta(seconds=1),
    )
    store = PreloadedToolStore(durable)
    executor = AgentLoopRunExecutor(
        loop_factory=lambda _lease, _writer, _recovery: cast("Any", None),
        events=MemoryEventStore(),
        tool_calls=store,
    )
    received = parse_agent_event(
        {
            "run_id": RUN_ID,
            "sequence": 1,
            "event_type": EventType.MODEL_TOOL_CALL_RECEIVED,
            "payload": {
                "model_call_id": "model-1",
                "tool_call_id": durable.id,
                "tool_name": durable.tool_name,
                "arguments": arguments,
                "argument_hash": durable.argument_hash,
            },
            "created_at": NOW,
        }
    )
    started = parse_agent_event(
        {
            "run_id": RUN_ID,
            "sequence": 2,
            "event_type": EventType.TOOL_STARTED,
            "payload": {
                "tool_call_id": durable.id,
                "tool_name": durable.tool_name,
            },
            "created_at": NOW + timedelta(seconds=2),
        }
    )
    observed: dict[str, Any] = {}

    await executor._persist_tool_state(
        run_lease(),
        received,
        observed=observed,
        turn_number=1,
    )
    await executor._persist_tool_state(
        run_lease(),
        started,
        observed=observed,
        turn_number=1,
    )

    assert [call.status for call in store.calls] == [ToolCallStatus.RECEIVED]


@pytest.mark.asyncio
async def test_agent_loop_executor_persists_waiting_approval_state() -> None:
    arguments: JsonObject = {"path": "README.md"}
    argument_hash = canonical_argument_hash(arguments)
    store = MemoryToolStore()
    executor = AgentLoopRunExecutor(
        loop_factory=lambda _lease, _writer, _recovery: cast("Any", None),
        events=MemoryEventStore(),
        tool_calls=store,
    )
    common = {
        "run_id": RUN_ID,
        "created_at": NOW,
    }
    received = parse_agent_event(
        {
            **common,
            "sequence": 1,
            "event_type": EventType.MODEL_TOOL_CALL_RECEIVED,
            "payload": {
                "model_call_id": "model-1",
                "tool_call_id": "call-approval",
                "tool_name": "read_file",
                "arguments": arguments,
                "argument_hash": argument_hash,
            },
        }
    )
    approval = parse_agent_event(
        {
            **common,
            "sequence": 2,
            "event_type": EventType.TOOL_APPROVAL_REQUIRED,
            "payload": {
                "tool_call_id": "call-approval",
                "tool_name": "read_file",
                "arguments": arguments,
                "argument_hash": argument_hash,
                "reason": "approval policy requires confirmation",
            },
        }
    )
    observed: dict[str, Any] = {}

    for event in (received, approval):
        await executor._persist_tool_state(
            run_lease(),
            event,
            observed=observed,
            turn_number=1,
        )

    assert [call.status for call in store.calls] == [
        ToolCallStatus.RECEIVED,
        ToolCallStatus.WAITING_APPROVAL,
    ]


class MutableClock:
    def __init__(self) -> None:
        self.value = NOW

    def now(self) -> datetime:
        self.value += timedelta(milliseconds=10)
        return self.value


class FakeQueue:
    def __init__(self, lease: RunLease | None) -> None:
        self.lease = lease
        self.claims = 0
        self.finished: list[RunExecutionResult] = []
        self.draining = False
        self.recoveries = 0
        self.cancellation_requested = False

    async def register_worker(
        self,
        registration: WorkerRegistration,
    ) -> WorkerRegistration:
        return registration

    async def heartbeat_worker(
        self,
        worker_id: str,
        *,
        occurred_at: datetime,
        available_slots: int,
    ) -> WorkerRegistration:
        return WorkerRegistration(
            worker_id=worker_id,
            supported_sandbox_types=("podman",),
            total_slots=1,
            available_slots=available_slots,
            status=WorkerStatus.DRAINING if self.draining else WorkerStatus.ACTIVE,
            registered_at=NOW,
            last_heartbeat_at=occurred_at,
        )

    async def set_worker_draining(
        self,
        worker_id: str,
        *,
        draining: bool,
        occurred_at: datetime,
    ) -> WorkerRegistration:
        self.draining = draining
        return await self.heartbeat_worker(
            worker_id,
            occurred_at=occurred_at,
            available_slots=1,
        )

    async def claim(
        self,
        worker_id: str,
        *,
        occurred_at: datetime,
        lease_duration: timedelta,
    ) -> RunLease | None:
        del worker_id, occurred_at, lease_duration
        self.claims += 1
        claimed, self.lease = self.lease, None
        return claimed

    async def start(self, lease: RunLease, *, occurred_at: datetime) -> RunLease:
        del occurred_at
        return lease.model_copy(update={"cancellation_requested": self.cancellation_requested})

    async def heartbeat(
        self,
        lease: RunLease,
        *,
        occurred_at: datetime,
        lease_duration: timedelta,
    ) -> RunLeaseHeartbeat:
        return RunLeaseHeartbeat(
            lease_token=lease.lease_token,
            generation=lease.generation,
            expires_at=occurred_at + lease_duration,
            cancellation_requested=self.cancellation_requested,
        )

    async def finish(
        self,
        lease: RunLease,
        result: RunExecutionResult,
        *,
        occurred_at: datetime,
    ) -> Run:
        del occurred_at
        self.finished.append(result)
        return Run(
            id=lease.run_id,
            session_id=lease.session_id,
            workspace_id=lease.workspace_id,
            status=result.status,
            priority=lease.priority,
            attempt=lease.attempt,
            last_checkpoint_id=result.last_checkpoint_id,
            cancellation_requested=result.status is RunStatus.CANCELLED,
            created_at=NOW,
            started_at=NOW,
            completed_at=NOW + timedelta(seconds=1),
        )

    async def recover_expired(
        self,
        *,
        occurred_at: datetime,
        limit: int,
    ) -> tuple[Run, ...]:
        del occurred_at, limit
        self.recoveries += 1
        return ()


class MultiRunQueue(FakeQueue):
    def __init__(self, leases: tuple[RunLease, ...]) -> None:
        super().__init__(None)
        self._leases = list(leases)

    async def heartbeat_worker(
        self,
        worker_id: str,
        *,
        occurred_at: datetime,
        available_slots: int,
    ) -> WorkerRegistration:
        return WorkerRegistration(
            worker_id=worker_id,
            supported_sandbox_types=("podman",),
            total_slots=2,
            available_slots=available_slots,
            status=WorkerStatus.ACTIVE,
            registered_at=NOW,
            last_heartbeat_at=occurred_at,
        )

    async def claim(
        self,
        worker_id: str,
        *,
        occurred_at: datetime,
        lease_duration: timedelta,
    ) -> RunLease | None:
        del worker_id, occurred_at, lease_duration
        self.claims += 1
        return self._leases.pop(0) if self._leases else None


class FakeWorkspaceLeases:
    def __init__(self) -> None:
        self.released = False

    async def acquire(
        self,
        lease: RunLease,
        *,
        occurred_at: datetime,
        lease_duration: timedelta,
    ) -> WorkspaceWriterLease:
        acquired = writer_lease(lease)
        return acquired.model_copy(
            update={
                "acquired_at": occurred_at,
                "expires_at": min(occurred_at + lease_duration, lease.expires_at),
            }
        )

    async def heartbeat(
        self,
        lease: WorkspaceWriterLease,
        *,
        occurred_at: datetime,
        lease_duration: timedelta,
    ) -> WorkspaceWriterLease:
        return lease.model_copy(update={"expires_at": occurred_at + lease_duration})

    async def release(self, lease: WorkspaceWriterLease) -> None:
        del lease
        self.released = True


class FakeRecovery:
    def __init__(self, state: RunRecoveryState | None = None) -> None:
        self.state = state or recovery_state()

    async def load(self, lease: RunLease) -> RunRecoveryState:
        del lease
        return self.state


class BlockingRecovery(FakeRecovery):
    def __init__(self, state: RunRecoveryState | None = None) -> None:
        super().__init__(state)
        self.started = asyncio.Event()
        self.cancelled = False

    async def load(self, lease: RunLease) -> RunRecoveryState:
        del lease
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        raise AssertionError("blocking recovery unexpectedly resumed")


class FakeRestorer:
    def __init__(self) -> None:
        self.restored: list[tuple[uuid.UUID, uuid.UUID, str]] = []

    async def restore(
        self,
        lease: RunLease,
        checkpoint: Checkpoint,
        *,
        writer_lease: WorkspaceWriterLease,
        workspace_revision: str,
    ) -> None:
        del lease
        self.restored.append((checkpoint.id, writer_lease.lease_token, workspace_revision))


class BlockingRestorer(FakeRestorer):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.cancelled = False

    async def restore(
        self,
        lease: RunLease,
        checkpoint: Checkpoint,
        *,
        writer_lease: WorkspaceWriterLease,
        workspace_revision: str,
    ) -> None:
        del lease, checkpoint, writer_lease, workspace_revision
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise


class FailingFinishQueue(FakeQueue):
    async def finish(
        self,
        lease: RunLease,
        result: RunExecutionResult,
        *,
        occurred_at: datetime,
    ) -> Run:
        del lease, result, occurred_at
        raise RuntimeError("durable finish unavailable")


class LeaseLosingQueue(FakeQueue):
    async def heartbeat(
        self,
        lease: RunLease,
        *,
        occurred_at: datetime,
        lease_duration: timedelta,
    ) -> RunLeaseHeartbeat:
        del lease, occurred_at, lease_duration
        raise DomainOperationError(
            code="run_lease_lost",
            message="the run was reassigned",
            retryable=True,
        )


class FakeExecutor:
    def __init__(self, result: RunExecutionResult) -> None:
        self.result = result
        self.cancelled = False
        self.executions = 0
        self.writer_tokens: list[uuid.UUID] = []

    async def execute(
        self,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        recovery: RunRecoveryState,
    ) -> RunExecutionResult:
        del lease, recovery
        self.executions += 1
        self.writer_tokens.append(writer_lease.lease_token)
        return self.result

    async def cancel(self, lease: RunLease) -> None:
        del lease
        self.cancelled = True


class ExplodingExecutor(FakeExecutor):
    async def execute(
        self,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        recovery: RunRecoveryState,
    ) -> RunExecutionResult:
        del lease, writer_lease, recovery
        raise RuntimeError("sensitive implementation detail")


class BlockingExecutor:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = False

    async def execute(
        self,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        recovery: RunRecoveryState,
    ) -> RunExecutionResult:
        del lease, writer_lease, recovery
        self.started.set()
        await self.release.wait()
        return RunExecutionResult(status=RunStatus.CANCELLED)

    async def cancel(self, lease: RunLease) -> None:
        del lease
        self.cancelled = True
        self.release.set()


class ConcurrencyExecutor:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0
        self.active = 0
        self.max_active = 0

    async def execute(
        self,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        recovery: RunRecoveryState,
    ) -> RunExecutionResult:
        del lease, writer_lease, recovery
        self.calls += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.started.set()
        if self.calls == 1:
            await self.release.wait()
        self.active -= 1
        return RunExecutionResult(status=RunStatus.COMPLETED)

    async def cancel(self, lease: RunLease) -> None:
        del lease
        self.release.set()


async def wait_for_idle(worker: WorkerService) -> None:
    for _ in range(100):
        if worker.active_count == 0:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("worker did not become idle")


@pytest.mark.asyncio
async def test_worker_claims_restores_completes_and_releases_workspace() -> None:
    queue = FakeQueue(run_lease())
    workspace = FakeWorkspaceLeases()
    executor = FakeExecutor(RunExecutionResult(status=RunStatus.COMPLETED))
    worker = WorkerService(
        config=WorkerConfig(worker_id="worker-1"),
        queue=queue,
        workspace_leases=workspace,
        recovery=FakeRecovery(),
        restorer=FakeRestorer(),
        executor=executor,
        clock=MutableClock(),
    )

    assert await worker.run_once() is True
    await wait_for_idle(worker)

    assert [result.status for result in queue.finished] == [RunStatus.COMPLETED]
    assert workspace.released is True
    assert executor.cancelled is False
    assert executor.writer_tokens == [WORKSPACE_TOKEN]


@pytest.mark.asyncio
async def test_worker_records_correlated_run_queue_and_capacity_telemetry() -> None:
    exporter = InMemorySpanExporter()
    telemetry = PlatformTelemetry(
        TelemetrySettings(service_name="agent-worker"),
        span_exporter=exporter,
    )
    traceparent = "00-" + "1" * 32 + "-" + "2" * 16 + "-01"
    lease = run_lease().model_copy(
        update={
            "queued_at": NOW - timedelta(seconds=2),
            "traceparent": traceparent,
        }
    )
    worker = WorkerService(
        config=WorkerConfig(worker_id="worker-1"),
        queue=FakeQueue(lease),
        workspace_leases=FakeWorkspaceLeases(),
        recovery=FakeRecovery(),
        restorer=FakeRestorer(),
        executor=FakeExecutor(RunExecutionResult(status=RunStatus.COMPLETED)),
        clock=MutableClock(),
        telemetry=telemetry,
    )

    assert await worker.run_once() is True
    await wait_for_idle(worker)

    spans = {span.name: span for span in exporter.get_finished_spans()}
    assert {"queue.wait", "worker.run"} <= spans.keys()
    worker_attributes = spans["worker.run"].attributes
    assert worker_attributes is not None
    assert worker_attributes["agent.run.id"] == str(RUN_ID)
    assert worker_attributes["agent.tenant.id"] == str(TENANT_ID)
    assert spans["worker.run"].context.trace_id == int("1" * 32, 16)
    payload = telemetry.metrics.render().decode("utf-8")
    assert "agent_platform_queue_wait_seconds_count 1.0" in payload
    assert "agent_platform_worker_utilization_ratio 0.0" in payload
    assert str(TENANT_ID) not in payload
    telemetry.shutdown()


@pytest.mark.asyncio
async def test_worker_bounds_sandbox_phases_separately_from_agent_run_slots() -> None:
    first = run_lease()
    second = first.model_copy(
        update={
            "run_id": uuid.UUID("30000000-0000-0000-0000-000000000004"),
            "workspace_id": uuid.UUID("40000000-0000-0000-0000-000000000005"),
            "lease_token": uuid.UUID("50000000-0000-0000-0000-000000000006"),
        }
    )
    queue = MultiRunQueue((first, second))
    executor = ConcurrencyExecutor()
    worker = WorkerService(
        config=WorkerConfig(worker_id="worker-1", total_slots=2, sandbox_slots=1),
        queue=queue,
        workspace_leases=FakeWorkspaceLeases(),
        recovery=FakeRecovery(),
        restorer=FakeRestorer(),
        executor=executor,
        clock=MutableClock(),
    )

    assert await worker.run_once() is True
    await asyncio.wait_for(executor.started.wait(), timeout=1)
    assert await worker.run_once() is True
    await asyncio.sleep(0)
    assert worker.active_count == 2
    assert executor.calls == 1

    executor.release.set()
    await asyncio.wait_for(wait_for_idle(worker), timeout=1)
    assert executor.calls == 2
    assert executor.max_active == 1


@pytest.mark.asyncio
async def test_worker_restores_checkpoint_before_execution() -> None:
    checkpoint = Checkpoint(
        id=uuid.uuid4(),
        run_id=RUN_ID,
        session_id=SESSION_ID,
        message_sequence=1,
        workspace_snapshot_uri="s3://agent-platform/checkpoint",
        workspace_revision="revision-1",
        task_plan={},
        created_at=NOW,
    )
    queue = FakeQueue(run_lease())
    restorer = FakeRestorer()
    worker = WorkerService(
        config=WorkerConfig(worker_id="worker-1"),
        queue=queue,
        workspace_leases=FakeWorkspaceLeases(),
        recovery=FakeRecovery(recovery_state(checkpoint=checkpoint)),
        restorer=restorer,
        executor=FakeExecutor(RunExecutionResult(status=RunStatus.COMPLETED)),
        clock=MutableClock(),
    )

    assert await worker.run_once() is True
    await wait_for_idle(worker)

    assert restorer.restored == [(checkpoint.id, WORKSPACE_TOKEN, "revision-1")]


@pytest.mark.asyncio
async def test_worker_surfaces_unrecoverable_background_task_failure() -> None:
    queue = FailingFinishQueue(run_lease())
    worker = WorkerService(
        config=WorkerConfig(worker_id="worker-1"),
        queue=queue,
        workspace_leases=FakeWorkspaceLeases(),
        recovery=FakeRecovery(),
        restorer=FakeRestorer(),
        executor=FakeExecutor(RunExecutionResult(status=RunStatus.COMPLETED)),
        clock=MutableClock(),
    )

    assert await worker.run_once() is True
    await wait_for_idle(worker)

    with pytest.raises(RuntimeError, match="durable finish unavailable"):
        await worker.run_once()
    assert queue.claims == 1


@pytest.mark.asyncio
async def test_worker_persists_and_surfaces_opaque_unexpected_execution_failure() -> None:
    queue = FakeQueue(run_lease())
    worker = WorkerService(
        config=WorkerConfig(worker_id="worker-1"),
        queue=queue,
        workspace_leases=FakeWorkspaceLeases(),
        recovery=FakeRecovery(),
        restorer=FakeRestorer(),
        executor=ExplodingExecutor(RunExecutionResult(status=RunStatus.COMPLETED)),
        clock=MutableClock(),
    )

    assert await worker.run_once() is True
    await wait_for_idle(worker)

    assert len(queue.finished) == 1
    failure = queue.finished[0]
    assert failure.status is RunStatus.FAILED
    assert failure.error is not None
    assert failure.error.code == "worker_execution_failed"
    assert "sensitive implementation detail" not in failure.error.message
    with pytest.raises(DomainOperationError) as error:
        await worker.run_once()
    assert error.value.code == "worker_execution_failed"


@pytest.mark.asyncio
async def test_worker_observes_distributed_cancellation_on_heartbeat() -> None:
    queue = FakeQueue(run_lease())
    executor = BlockingExecutor()
    worker = WorkerService(
        config=WorkerConfig(
            worker_id="worker-1",
            lease_seconds=2,
            heartbeat_seconds=0.2,
        ),
        queue=queue,
        workspace_leases=FakeWorkspaceLeases(),
        recovery=FakeRecovery(),
        restorer=FakeRestorer(),
        executor=executor,
        clock=MutableClock(),
    )

    assert await worker.run_once() is True
    await asyncio.wait_for(executor.started.wait(), timeout=1)
    queue.cancellation_requested = True
    await asyncio.wait_for(wait_for_idle(worker), timeout=2)

    assert executor.cancelled is True
    assert [result.status for result in queue.finished] == [RunStatus.CANCELLED]


@pytest.mark.asyncio
async def test_worker_cancellation_interrupts_recovery_before_execution() -> None:
    queue = FakeQueue(run_lease())
    recovery = BlockingRecovery()
    executor = FakeExecutor(RunExecutionResult(status=RunStatus.COMPLETED))
    worker = WorkerService(
        config=WorkerConfig(
            worker_id="worker-1",
            lease_seconds=2,
            heartbeat_seconds=0.2,
        ),
        queue=queue,
        workspace_leases=FakeWorkspaceLeases(),
        recovery=recovery,
        restorer=FakeRestorer(),
        executor=executor,
        clock=MutableClock(),
    )

    assert await worker.run_once() is True
    await asyncio.wait_for(recovery.started.wait(), timeout=1)
    queue.cancellation_requested = True
    await asyncio.wait_for(wait_for_idle(worker), timeout=2)

    assert recovery.cancelled is True
    assert executor.executions == 0
    assert [result.status for result in queue.finished] == [RunStatus.CANCELLED]
    assert await worker.run_once() is False


@pytest.mark.asyncio
async def test_worker_lease_loss_interrupts_recovery_without_stale_finish() -> None:
    queue = LeaseLosingQueue(run_lease())
    recovery = BlockingRecovery()
    executor = FakeExecutor(RunExecutionResult(status=RunStatus.COMPLETED))
    worker = WorkerService(
        config=WorkerConfig(
            worker_id="worker-1",
            lease_seconds=2,
            heartbeat_seconds=0.2,
        ),
        queue=queue,
        workspace_leases=FakeWorkspaceLeases(),
        recovery=recovery,
        restorer=FakeRestorer(),
        executor=executor,
        clock=MutableClock(),
    )

    assert await worker.run_once() is True
    await asyncio.wait_for(recovery.started.wait(), timeout=1)
    await asyncio.wait_for(wait_for_idle(worker), timeout=2)

    assert recovery.cancelled is True
    assert executor.executions == 0
    assert queue.finished == []
    assert await worker.run_once() is False


@pytest.mark.asyncio
async def test_worker_cancellation_interrupts_restore_before_execution() -> None:
    checkpoint = Checkpoint(
        id=uuid.uuid4(),
        run_id=RUN_ID,
        session_id=SESSION_ID,
        message_sequence=1,
        workspace_snapshot_uri="s3://agent-platform/checkpoint",
        workspace_revision="revision-1",
        task_plan={},
        created_at=NOW,
    )
    queue = FakeQueue(run_lease())
    restorer = BlockingRestorer()
    executor = FakeExecutor(RunExecutionResult(status=RunStatus.COMPLETED))
    worker = WorkerService(
        config=WorkerConfig(
            worker_id="worker-1",
            lease_seconds=2,
            heartbeat_seconds=0.2,
        ),
        queue=queue,
        workspace_leases=FakeWorkspaceLeases(),
        recovery=FakeRecovery(recovery_state(checkpoint=checkpoint)),
        restorer=restorer,
        executor=executor,
        clock=MutableClock(),
    )

    assert await worker.run_once() is True
    await asyncio.wait_for(restorer.started.wait(), timeout=1)
    queue.cancellation_requested = True
    await asyncio.wait_for(wait_for_idle(worker), timeout=2)

    assert restorer.cancelled is True
    assert executor.executions == 0
    assert [result.status for result in queue.finished] == [RunStatus.CANCELLED]


@pytest.mark.asyncio
async def test_worker_cancels_leased_run_before_workspace_or_execution_starts() -> None:
    queue = FakeQueue(run_lease())
    queue.cancellation_requested = True
    workspace = FakeWorkspaceLeases()
    executor = FakeExecutor(RunExecutionResult(status=RunStatus.COMPLETED))
    worker = WorkerService(
        config=WorkerConfig(worker_id="worker-1"),
        queue=queue,
        workspace_leases=workspace,
        recovery=FakeRecovery(),
        restorer=FakeRestorer(),
        executor=executor,
        clock=MutableClock(),
    )

    assert await worker.run_once() is True
    await wait_for_idle(worker)

    assert [result.status for result in queue.finished] == [RunStatus.CANCELLED]
    assert workspace.released is False


@pytest.mark.asyncio
async def test_draining_worker_does_not_claim_new_work() -> None:
    queue = FakeQueue(run_lease())
    worker = WorkerService(
        config=WorkerConfig(worker_id="worker-1"),
        queue=queue,
        workspace_leases=FakeWorkspaceLeases(),
        recovery=FakeRecovery(),
        restorer=FakeRestorer(),
        executor=FakeExecutor(RunExecutionResult(status=RunStatus.COMPLETED)),
        clock=MutableClock(),
    )
    await worker.register()
    await worker.drain()

    assert await worker.run_once() is False
    assert queue.claims == 0
    assert queue.draining is True


@pytest.mark.asyncio
async def test_scheduler_uses_bounded_expiry_recovery() -> None:
    queue = FakeQueue(None)
    scheduler = SchedulerService(
        queue=queue,
        clock=MutableClock(),
        config=SchedulerConfig(recovery_batch_size=7),
    )

    assert await scheduler.recover_once() == 0
    assert queue.recoveries == 1


@pytest.mark.asyncio
async def test_scheduler_exports_bounded_queue_pressure_metrics() -> None:
    class QueueMonitorFake:
        async def snapshot(self, *, occurred_at: datetime) -> QueueSnapshot:
            return QueueSnapshot(
                depth=QueueDepth(interactive=3, background=2, evaluation=1),
                oldest_age_seconds=12.5,
                captured_at=occurred_at,
            )

    telemetry = PlatformTelemetry(TelemetrySettings(service_name="agent-scheduler"))
    scheduler = SchedulerService(
        queue=FakeQueue(None),
        queue_monitor=QueueMonitorFake(),
        clock=MutableClock(),
        telemetry=telemetry,
    )

    assert await scheduler.recover_once() == 0
    payload = telemetry.metrics.render().decode("utf-8")
    assert 'agent_platform_queue_depth{priority="interactive"} 3.0' in payload
    assert 'agent_platform_queue_depth{priority="background"} 2.0' in payload
    assert 'agent_platform_queue_depth{priority="evaluation"} 1.0' in payload
    assert "agent_platform_oldest_queued_seconds 12.5" in payload
    telemetry.shutdown()


def test_worker_and_scheduler_configuration_reject_unsafe_values() -> None:
    assert DEFAULT_LOCAL_WORKER_PROCESSES == 3
    with pytest.raises(ValidationError, match="less than half"):
        WorkerConfig(
            worker_id="worker-1",
            lease_seconds=10,
            heartbeat_seconds=5,
        )
    with pytest.raises(ValidationError):
        WorkerConfig(worker_id="worker-1", total_slots=0)
    with pytest.raises(ValidationError, match="sandbox_slots"):
        WorkerConfig(worker_id="worker-1", total_slots=1, sandbox_slots=2)
    with pytest.raises(ValidationError):
        SchedulerConfig(recovery_batch_size=1001)
    with pytest.raises(ValueError, match="module:attribute"):
        load_worker_factory("../factory")
    with pytest.raises(ValueError, match="module:attribute"):
        load_scheduler_factory("../factory")
    with pytest.raises(ValueError, match="process_count"):
        run_worker_fleet("agent_worker:WorkerService", process_count=0)


class FakeWorkerProcess:
    def __init__(self, exitcode: int | None) -> None:
        self.exitcode = exitcode
        self.joins = 0

    def join(self, timeout: float | None = None) -> None:
        assert timeout == 0.05
        self.joins += 1


def test_worker_fleet_supervisor_observes_any_child_failure_promptly() -> None:
    running = FakeWorkerProcess(None)
    failed = FakeWorkerProcess(7)

    with pytest.raises(RuntimeError, match="exited unsuccessfully"):
        _wait_for_processes(cast("Any", [running, failed]))

    assert running.joins == 1
    assert failed.joins == 1


def test_failed_execution_requires_a_structured_error() -> None:
    with pytest.raises(ValidationError, match="requires an error"):
        RunExecutionResult(status=RunStatus.FAILED)
    failure = RunExecutionResult(
        status=RunStatus.FAILED,
        error=ErrorDetail(code="failed", message="bounded failure"),
    )
    assert failure.error is not None
