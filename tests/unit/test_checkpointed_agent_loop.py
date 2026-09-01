import asyncio
from collections.abc import AsyncGenerator, Sequence
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from agent_core.checkpoints import RewindState
from agent_core.domain import (
    Checkpoint,
    DomainOperationError,
    FrozenJsonObject,
    ToolCallStatus,
)
from agent_core.events import (
    AnyAgentEvent,
    CheckpointCreatedEvent,
    EventType,
    RunCompletedEvent,
    RunFailedEvent,
    ToolCompletedEvent,
)
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
    ToolOutputChannel,
    ToolOutputChunk,
    ToolRegistry,
)
from platform_telemetry import PlatformTelemetry, TelemetrySettings

RUN_ID = UUID("10000000-0000-0000-0000-000000000001")
SESSION_ID = UUID("20000000-0000-0000-0000-000000000002")
CHECKPOINT_ID = UUID("30000000-0000-0000-0000-000000000003")
NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)


class EditArguments(ToolArguments):
    path: str
    content: str


class EditHandler:
    def __init__(self, *, error: DomainOperationError | None = None) -> None:
        self.calls = 0
        self.contexts: list[ToolExecutionContext] = []
        self.error = error

    async def __call__(
        self,
        arguments: EditArguments,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        self.calls += 1
        self.contexts.append(context)
        if self.error is not None:
            raise self.error
        yield ToolExecutionCompleted(
            result={"path": arguments.path, "bytes_written": len(arguments.content)}
        )


class BlockingEditHandler(EditHandler):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()

    async def __call__(
        self,
        arguments: EditArguments,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        del arguments
        self.calls += 1
        self.contexts.append(context)
        self.started.set()
        yield ToolOutputChunk(
            channel=ToolOutputChannel.STDOUT,
            chunk="started",
        )
        await asyncio.Event().wait()


class RecordingCheckpointCoordinator:
    def __init__(self, *, completed_revision: str = "revision-after") -> None:
        self.created: list[
            tuple[UUID, str, tuple[GatewayMessage, ...], FrozenJsonObject, str | None]
        ] = []
        self.completed: list[tuple[Checkpoint, str]] = []
        self.rolled_back: list[Checkpoint] = []
        self.completed_revision = completed_revision

    async def create_before_tool(
        self,
        *,
        run_id: UUID,
        tool_call_id: str,
        messages: Sequence[GatewayMessage],
        task_plan: FrozenJsonObject,
        context_summary: str | None,
    ) -> Checkpoint:
        self.created.append((run_id, tool_call_id, tuple(messages), task_plan, context_summary))
        return Checkpoint(
            id=CHECKPOINT_ID,
            run_id=run_id,
            session_id=SESSION_ID,
            tool_call_id=tool_call_id,
            message_sequence=len(messages),
            workspace_snapshot_uri="memory://checkpoint/pre-tool",
            workspace_revision="revision-before",
            task_plan=task_plan,
            context_summary=context_summary,
            created_at=NOW,
        )

    async def complete_tool(
        self,
        checkpoint: Checkpoint,
        *,
        tool_call_id: str,
    ) -> str:
        self.completed.append((checkpoint, tool_call_id))
        return self.completed_revision

    async def rollback(self, checkpoint: Checkpoint) -> None:
        self.rolled_back.append(checkpoint)

    async def rewind(self, checkpoint_id: UUID) -> RewindState:
        raise NotImplementedError(checkpoint_id)


def loop_input() -> AgentLoopInput:
    return AgentLoopInput(
        tenant_id=UUID("00000000-0000-0000-0000-000000000010"),
        session_id=UUID("00000000-0000-0000-0000-000000000020"),
        run_id=RUN_ID,
        attempt=1,
        worker_id="worker-1",
        route_name="coding-default",
        messages=(GatewayMessage(role=MessageRole.USER, content="edit the file"),),
        task_plan={"steps": [{"title": "edit", "done": False}]},
        context_summary="summary before edit",
    )


def build_loop(
    handler: EditHandler,
    *,
    checkpoints: RecordingCheckpointCoordinator | None,
    turns: list[ScriptedGatewayTurn],
    telemetry: PlatformTelemetry | None = None,
) -> AgentLoop:
    registration = RegisteredTool(
        name="edit_file",
        description="Edit one file",
        arguments_type=EditArguments,
        handler=handler,
        effect=ToolEffect.WORKSPACE_MUTATION,
    )
    return AgentLoop(
        gateway=ScriptedModelGateway(turns),
        tools=ToolRegistry((registration,)),
        clock=SteppingClock(NOW),
        id_generator=SequentialIdGenerator(),
        checkpoints=checkpoints,
        telemetry=telemetry,
    )


async def collect(loop: AgentLoop) -> list[AnyAgentEvent]:
    return [event async for event in loop.run(loop_input())]


async def test_mutation_creates_checkpoint_before_execution_and_records_revision() -> None:
    handler = EditHandler()
    checkpoints = RecordingCheckpointCoordinator()
    tool_call = GatewayToolCall(
        id="edit-1",
        name="edit_file",
        arguments={"path": "main.py", "content": "changed"},
    )
    events = await collect(
        build_loop(
            handler,
            checkpoints=checkpoints,
            turns=[
                ScriptedGatewayTurn.tool_calls(tool_call),
                ScriptedGatewayTurn.text("done"),
            ],
        )
    )

    event_types = [event.event_type for event in events]
    assert event_types.index(EventType.CHECKPOINT_CREATED) < event_types.index(
        EventType.TOOL_STARTED
    )
    checkpoint_event = next(event for event in events if isinstance(event, CheckpointCreatedEvent))
    assert checkpoint_event.payload.checkpoint_id == CHECKPOINT_ID
    assert checkpoints.created[0][0:2] == (RUN_ID, "edit-1")
    assert checkpoints.created[0][3] == loop_input().task_plan
    assert checkpoints.created[0][4] == "summary before edit"
    assert checkpoints.completed[0][1] == "edit-1"
    assert checkpoints.rolled_back == []
    assert handler.calls == 1
    assert handler.contexts[0].checkpoint_id == CHECKPOINT_ID
    assert handler.contexts[0].workspace_revision == "revision-before"

    completed_tool = next(
        event
        for event in events
        if isinstance(event, ToolCompletedEvent)
        and event.payload.status is ToolCallStatus.COMPLETED
    )
    assert completed_tool.payload.result is not None
    assert completed_tool.payload.result["workspace_revision"] == "revision-after"
    completed_run = events[-1]
    assert isinstance(completed_run, RunCompletedEvent)
    assert completed_run.payload.checkpoint_id == CHECKPOINT_ID


async def test_checkpoint_creation_has_correlated_span_and_metrics() -> None:
    exporter = InMemorySpanExporter()
    telemetry = PlatformTelemetry(
        TelemetrySettings(service_name="agent-core"),
        span_exporter=exporter,
    )
    events = await collect(
        build_loop(
            EditHandler(),
            checkpoints=RecordingCheckpointCoordinator(),
            turns=[
                ScriptedGatewayTurn.tool_calls(
                    GatewayToolCall(
                        id="edit-observed",
                        name="edit_file",
                        arguments={"path": "main.py", "content": "changed"},
                    )
                ),
                ScriptedGatewayTurn.text("done"),
            ],
            telemetry=telemetry,
        )
    )

    assert isinstance(events[-1], RunCompletedEvent)
    spans = exporter.get_finished_spans()
    assert [span.name for span in spans] == ["checkpoint.create", "tool.execute"]
    checkpoint_attributes = spans[0].attributes
    assert checkpoint_attributes is not None
    assert checkpoint_attributes["agent.run.id"] == str(RUN_ID)
    assert checkpoint_attributes["agent.tool_call.id"] == "edit-observed"
    payload = telemetry.metrics.render().decode("utf-8")
    assert 'agent_platform_checkpoints_total{outcome="success"} 1.0' in payload
    assert "agent_platform_checkpoint_duration_seconds_count 1.0" in payload
    assert "changed" not in payload
    telemetry.shutdown()


async def test_failed_mutation_rolls_back_and_returns_sanitized_tool_failure() -> None:
    handler = EditHandler(
        error=DomainOperationError(
            code="edit_hash_conflict",
            message="the file changed",
            details={"path": "main.py"},
        )
    )
    checkpoints = RecordingCheckpointCoordinator()
    tool_call = GatewayToolCall(
        id="edit-1",
        name="edit_file",
        arguments={"path": "main.py", "content": "changed"},
    )
    events = await collect(
        build_loop(
            handler,
            checkpoints=checkpoints,
            turns=[
                ScriptedGatewayTurn.tool_calls(tool_call),
                ScriptedGatewayTurn.text("reported failure"),
            ],
        )
    )

    assert len(checkpoints.rolled_back) == 1
    assert checkpoints.completed == []
    failed = next(
        event
        for event in events
        if isinstance(event, ToolCompletedEvent) and event.payload.status is ToolCallStatus.FAILED
    )
    assert failed.payload.error is not None
    assert failed.payload.error.code == "edit_hash_conflict"
    assert isinstance(events[-1], RunCompletedEvent)


async def test_side_effecting_tool_fails_closed_without_checkpoint_coordinator() -> None:
    handler = EditHandler()
    tool_call = GatewayToolCall(
        id="edit-1",
        name="edit_file",
        arguments={"path": "main.py", "content": "changed"},
    )
    events = await collect(
        build_loop(
            handler,
            checkpoints=None,
            turns=[ScriptedGatewayTurn.tool_calls(tool_call)],
        )
    )

    assert handler.calls == 0
    failure = events[-1]
    assert isinstance(failure, RunFailedEvent)
    assert failure.payload.error.code == "checkpoint_required"


async def test_duplicate_mutation_reuses_outcome_without_second_checkpoint() -> None:
    handler = EditHandler()
    checkpoints = RecordingCheckpointCoordinator()
    tool_call = GatewayToolCall(
        id="edit-1",
        name="edit_file",
        arguments={"path": "main.py", "content": "changed"},
    )
    events = await collect(
        build_loop(
            handler,
            checkpoints=checkpoints,
            turns=[
                ScriptedGatewayTurn.tool_calls(tool_call),
                ScriptedGatewayTurn.tool_calls(tool_call),
                ScriptedGatewayTurn.text("done"),
            ],
        )
    )

    assert handler.calls == 1
    assert len(checkpoints.created) == 1
    assert len(checkpoints.completed) == 1
    completed = [
        event
        for event in events
        if isinstance(event, ToolCompletedEvent)
        and event.payload.status is ToolCallStatus.COMPLETED
    ]
    assert len(completed) == 2


async def test_invalid_completed_revision_rolls_back_and_fails_closed() -> None:
    handler = EditHandler()
    checkpoints = RecordingCheckpointCoordinator(completed_revision="x" * 256)
    tool_call = GatewayToolCall(
        id="edit-1",
        name="edit_file",
        arguments={"path": "main.py", "content": "changed"},
    )
    events = await collect(
        build_loop(
            handler,
            checkpoints=checkpoints,
            turns=[ScriptedGatewayTurn.tool_calls(tool_call)],
        )
    )

    assert len(checkpoints.rolled_back) == 1
    failure = events[-1]
    assert isinstance(failure, RunFailedEvent)
    assert failure.payload.error.code == "checkpoint_finalize_failed"


async def test_runtime_invalid_completed_revision_is_structured_and_rolled_back() -> None:
    handler = EditHandler()
    checkpoints = RecordingCheckpointCoordinator(completed_revision=cast("str", 42))
    tool_call = GatewayToolCall(
        id="edit-1",
        name="edit_file",
        arguments={"path": "main.py", "content": "changed"},
    )

    events = await collect(
        build_loop(
            handler,
            checkpoints=checkpoints,
            turns=[ScriptedGatewayTurn.tool_calls(tool_call)],
        )
    )

    assert len(checkpoints.rolled_back) == 1
    failure = events[-1]
    assert isinstance(failure, RunFailedEvent)
    assert failure.payload.error.code == "checkpoint_finalize_failed"


async def test_mismatched_checkpoint_contract_prevents_tool_execution() -> None:
    class MismatchedCoordinator(RecordingCheckpointCoordinator):
        async def create_before_tool(
            self,
            *,
            run_id: UUID,
            tool_call_id: str,
            messages: Sequence[GatewayMessage],
            task_plan: FrozenJsonObject,
            context_summary: str | None,
        ) -> Checkpoint:
            checkpoint = await super().create_before_tool(
                run_id=run_id,
                tool_call_id=tool_call_id,
                messages=messages,
                task_plan=task_plan,
                context_summary=context_summary,
            )
            return checkpoint.model_copy(
                update={"message_sequence": checkpoint.message_sequence + 1}
            )

    handler = EditHandler()
    checkpoints = MismatchedCoordinator()
    tool_call = GatewayToolCall(
        id="edit-1",
        name="edit_file",
        arguments={"path": "main.py", "content": "changed"},
    )

    events = await collect(
        build_loop(
            handler,
            checkpoints=checkpoints,
            turns=[ScriptedGatewayTurn.tool_calls(tool_call)],
        )
    )

    assert handler.calls == 0
    assert checkpoints.completed == []
    assert checkpoints.rolled_back == []
    failure = events[-1]
    assert isinstance(failure, RunFailedEvent)
    assert failure.payload.error.code == "checkpoint_contract_invalid"


async def test_cancelling_a_mutation_waits_for_checkpoint_rollback() -> None:
    handler = BlockingEditHandler()
    checkpoints = RecordingCheckpointCoordinator()
    tool_call = GatewayToolCall(
        id="edit-1",
        name="edit_file",
        arguments={"path": "main.py", "content": "changed"},
    )
    task = asyncio.create_task(
        collect(
            build_loop(
                handler,
                checkpoints=checkpoints,
                turns=[ScriptedGatewayTurn.tool_calls(tool_call)],
            )
        )
    )

    await handler.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert handler.calls == 1
    assert len(checkpoints.rolled_back) == 1
    assert checkpoints.completed == []
