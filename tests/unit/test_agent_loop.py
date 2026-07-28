import asyncio
from datetime import UTC, datetime
from uuid import UUID

import pytest

from agent_core.domain import DomainOperationError, ToolCallStatus
from agent_core.events import (
    AnyAgentEvent,
    EventType,
    ModelToolCallReceivedEvent,
    RunCompletedEvent,
    RunFailedEvent,
    ToolCompletedEvent,
    ToolStderrEvent,
    ToolStdoutEvent,
)
from agent_core.fakes import (
    ScriptedGatewayTurn,
    ScriptedModelGateway,
    SequentialIdGenerator,
    SteppingClock,
)
from agent_core.gateway import (
    GatewayFinishReason,
    GatewayMessage,
    GatewayResponseCompleted,
    GatewayTextDelta,
    GatewayToolCall,
    MessageRole,
)
from agent_core.loop import AgentLoop, AgentLoopConfig, AgentLoopInput
from agent_core.tools import (
    RegisteredTool,
    ToolArguments,
    ToolExecutionResult,
    ToolRegistry,
)

NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)
RUN_ID = UUID("10000000-0000-0000-0000-000000000001")


class ReadArguments(ToolArguments):
    path: str


class ScriptedReadHandler:
    def __init__(
        self,
        *,
        result: ToolExecutionResult | None = None,
        error: Exception | None = None,
        delay_seconds: float = 0,
    ) -> None:
        self.calls: list[ReadArguments] = []
        self.result = result
        self.error = error
        self.delay_seconds = delay_seconds

    async def __call__(self, arguments: ReadArguments) -> ToolExecutionResult:
        self.calls.append(arguments)
        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)
        if self.error is not None:
            raise self.error
        return self.result or ToolExecutionResult(
            result={"path": arguments.path, "content": "source"}
        )


def read_tool(handler: ScriptedReadHandler) -> RegisteredTool[ReadArguments]:
    return RegisteredTool(
        name="read_file",
        description="Read one workspace file",
        arguments_type=ReadArguments,
        handler=handler,
    )


def default_input() -> AgentLoopInput:
    return AgentLoopInput(
        run_id=RUN_ID,
        attempt=1,
        worker_id="worker-1",
        route_name="coding-default",
        messages=(
            GatewayMessage(
                role=MessageRole.USER,
                content="Inspect the repository and report the result.",
            ),
        ),
    )


def make_loop(
    turns: list[ScriptedGatewayTurn],
    *,
    handler: ScriptedReadHandler | None = None,
    config: AgentLoopConfig | None = None,
) -> tuple[AgentLoop, ScriptedModelGateway]:
    gateway = ScriptedModelGateway(turns)
    tools = ToolRegistry((read_tool(handler),)) if handler is not None else ToolRegistry()
    return (
        AgentLoop(
            gateway=gateway,
            tools=tools,
            clock=SteppingClock(NOW),
            id_generator=SequentialIdGenerator(),
            config=config,
        ),
        gateway,
    )


async def collect_events(loop: AgentLoop) -> list[AnyAgentEvent]:
    return [event async for event in loop.run(default_input())]


def event_types(events: list[AnyAgentEvent]) -> list[EventType]:
    return [event.event_type for event in events]


async def test_final_text_stream_emits_contiguous_typed_events() -> None:
    loop, gateway = make_loop([ScriptedGatewayTurn.text("Implemented.", chunk_size=4)])

    events = await collect_events(loop)

    assert event_types(events) == [
        EventType.RUN_STARTED,
        EventType.CONTEXT_BUILD_STARTED,
        EventType.MODEL_REQUEST_STARTED,
        EventType.MODEL_TEXT_DELTA,
        EventType.MODEL_TEXT_DELTA,
        EventType.MODEL_TEXT_DELTA,
        EventType.RUN_COMPLETED,
    ]
    assert [event.sequence for event in events] == list(range(1, len(events) + 1))
    assert [event.created_at for event in events] == sorted(event.created_at for event in events)
    completed = events[-1]
    assert isinstance(completed, RunCompletedEvent)
    assert completed.payload.final_text == "Implemented."
    assert len(gateway.requests) == 1
    assert gateway.requests[0].messages == default_input().messages
    assert gateway.requests[0].tools == ()


async def test_one_tool_call_is_validated_executed_and_returned_to_model() -> None:
    handler = ScriptedReadHandler(
        result=ToolExecutionResult(
            result={"content": "print('hello')"},
            stdout=("read stdout",),
            stderr=("read warning",),
        )
    )
    tool_call = GatewayToolCall(
        id="tool-call-1",
        name="read_file",
        arguments={"path": "src/main.py"},
    )
    loop, gateway = make_loop(
        [
            ScriptedGatewayTurn.tool_calls(tool_call),
            ScriptedGatewayTurn.text("The file is valid."),
        ],
        handler=handler,
    )

    events = await collect_events(loop)

    assert len(handler.calls) == 1
    assert handler.calls[0].path == "src/main.py"
    received = next(event for event in events if isinstance(event, ModelToolCallReceivedEvent))
    assert received.payload.tool_call_id == tool_call.id
    assert len(received.payload.argument_hash) == 64
    completed = next(
        event
        for event in events
        if isinstance(event, ToolCompletedEvent)
        and event.payload.status is ToolCallStatus.COMPLETED
    )
    assert completed.payload.result is not None
    assert completed.payload.result.to_json_object() == {"content": "print('hello')"}
    assert any(isinstance(event, ToolStdoutEvent) for event in events)
    assert any(isinstance(event, ToolStderrEvent) for event in events)
    assert len(gateway.requests) == 2
    assert [message.role for message in gateway.requests[1].messages] == [
        MessageRole.USER,
        MessageRole.ASSISTANT,
        MessageRole.TOOL,
    ]
    assert '"ok":true' in gateway.requests[1].messages[-1].content
    assert events[-1].event_type is EventType.RUN_COMPLETED


async def test_multiple_tool_calls_execute_sequentially_before_next_turn() -> None:
    handler = ScriptedReadHandler()
    calls = (
        GatewayToolCall(
            id="tool-call-1",
            name="read_file",
            arguments={"path": "a.py"},
        ),
        GatewayToolCall(
            id="tool-call-2",
            name="read_file",
            arguments={"path": "b.py"},
        ),
    )
    loop, gateway = make_loop(
        [
            ScriptedGatewayTurn.tool_calls(*calls),
            ScriptedGatewayTurn.text("Both files were inspected."),
        ],
        handler=handler,
    )

    events = await collect_events(loop)

    assert [arguments.path for arguments in handler.calls] == ["a.py", "b.py"]
    assert sum(isinstance(event, ModelToolCallReceivedEvent) for event in events) == 2
    assert (
        sum(
            isinstance(event, ToolCompletedEvent)
            and event.payload.status is ToolCallStatus.COMPLETED
            for event in events
        )
        == 2
    )
    assert len(gateway.requests[1].messages) == 4


async def test_malformed_tool_arguments_never_reach_handler_and_receive_feedback() -> None:
    handler = ScriptedReadHandler()
    malformed_call = GatewayToolCall(
        id="tool-call-malformed",
        name="read_file",
        arguments={"unexpected": "secret-value"},
    )
    loop, gateway = make_loop(
        [
            ScriptedGatewayTurn.tool_calls(malformed_call),
            ScriptedGatewayTurn.text("I corrected the request."),
        ],
        handler=handler,
    )

    events = await collect_events(loop)

    assert handler.calls == []
    failed_tool = next(
        event
        for event in events
        if isinstance(event, ToolCompletedEvent) and event.payload.status is ToolCallStatus.FAILED
    )
    assert failed_tool.payload.error is not None
    assert failed_tool.payload.error.code == "malformed_tool_arguments"
    feedback = gateway.requests[1].messages[-1].content
    assert "malformed_tool_arguments" in feedback
    assert "secret-value" not in feedback
    assert events[-1].event_type is EventType.RUN_COMPLETED


async def test_unknown_tool_counts_against_semantic_retry_budget() -> None:
    unknown_call = GatewayToolCall(
        id="tool-call-unknown",
        name="unknown_tool",
        arguments={},
    )
    loop, _ = make_loop(
        [ScriptedGatewayTurn.tool_calls(unknown_call)],
        config=AgentLoopConfig(max_semantic_retries=0),
    )

    events = await collect_events(loop)

    failed_tool = next(event for event in events if isinstance(event, ToolCompletedEvent))
    assert failed_tool.payload.error is not None
    assert failed_tool.payload.error.code == "unknown_tool"
    failed_run = events[-1]
    assert isinstance(failed_run, RunFailedEvent)
    assert failed_run.payload.error.code == "semantic_retry_limit"


async def test_tool_failure_is_sanitized_and_model_can_react() -> None:
    handler = ScriptedReadHandler(error=RuntimeError("credential=do-not-leak"))
    call = GatewayToolCall(
        id="tool-call-failure",
        name="read_file",
        arguments={"path": "broken.py"},
    )
    loop, gateway = make_loop(
        [
            ScriptedGatewayTurn.tool_calls(call),
            ScriptedGatewayTurn.text("The tool failed safely."),
        ],
        handler=handler,
    )

    events = await collect_events(loop)

    tool_failure = next(
        event
        for event in events
        if isinstance(event, ToolCompletedEvent) and event.payload.status is ToolCallStatus.FAILED
    )
    assert tool_failure.payload.error is not None
    assert tool_failure.payload.error.code == "tool_execution_failed"
    serialized_events = "".join(event.model_dump_json() for event in events)
    assert "do-not-leak" not in serialized_events
    assert "do-not-leak" not in gateway.requests[1].messages[-1].content
    assert events[-1].event_type is EventType.RUN_COMPLETED


async def test_domain_tool_failure_preserves_validated_structured_error() -> None:
    handler = ScriptedReadHandler(
        error=DomainOperationError(
            code="workspace_unavailable",
            message="workspace cannot be read",
            retryable=True,
            details={"workspace_id": "workspace-1"},
        )
    )
    call = GatewayToolCall(
        id="tool-call-domain-failure",
        name="read_file",
        arguments={"path": "README.md"},
    )
    loop, _ = make_loop(
        [
            ScriptedGatewayTurn.tool_calls(call),
            ScriptedGatewayTurn.text("Reported the workspace failure."),
        ],
        handler=handler,
    )

    events = await collect_events(loop)

    failed = next(
        event
        for event in events
        if isinstance(event, ToolCompletedEvent) and event.payload.status is ToolCallStatus.FAILED
    )
    assert failed.payload.error is not None
    assert failed.payload.error.code == "workspace_unavailable"
    assert failed.payload.error.retryable is True


async def test_maximum_turn_limit_terminates_after_exact_number_of_turns() -> None:
    handler = ScriptedReadHandler()
    turns = [
        ScriptedGatewayTurn.tool_calls(
            GatewayToolCall(
                id=f"tool-call-{index}",
                name="read_file",
                arguments={"path": f"{index}.py"},
            )
        )
        for index in range(2)
    ]
    loop, gateway = make_loop(
        turns,
        handler=handler,
        config=AgentLoopConfig(max_turns=2),
    )

    events = await collect_events(loop)

    assert len(gateway.requests) == 2
    assert len(handler.calls) == 2
    failed = events[-1]
    assert isinstance(failed, RunFailedEvent)
    assert failed.payload.error.code == "turn_limit"
    assert failed.payload.error.details["limit"] == 2


async def test_maximum_tool_call_limit_stops_before_excess_execution() -> None:
    handler = ScriptedReadHandler()
    calls = (
        GatewayToolCall(id="call-1", name="read_file", arguments={"path": "a.py"}),
        GatewayToolCall(id="call-2", name="read_file", arguments={"path": "b.py"}),
    )
    loop, _ = make_loop(
        [ScriptedGatewayTurn.tool_calls(*calls)],
        handler=handler,
        config=AgentLoopConfig(max_tool_calls=1),
    )

    events = await collect_events(loop)

    assert handler.calls == []
    assert sum(isinstance(event, ModelToolCallReceivedEvent) for event in events) == 2
    failed = events[-1]
    assert isinstance(failed, RunFailedEvent)
    assert failed.payload.error.code == "tool_call_limit"


async def test_model_and_tool_timeouts_are_structured() -> None:
    model_loop, _ = make_loop(
        [
            ScriptedGatewayTurn(
                events=(GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP),),
                delay_seconds=0.05,
            )
        ],
        config=AgentLoopConfig(model_timeout_seconds=0.001),
    )
    model_events = await collect_events(model_loop)
    model_failure = model_events[-1]
    assert isinstance(model_failure, RunFailedEvent)
    assert model_failure.payload.error.code == "model_timeout"
    assert model_failure.payload.error.retryable is True

    handler = ScriptedReadHandler(delay_seconds=0.05)
    call = GatewayToolCall(
        id="tool-call-timeout",
        name="read_file",
        arguments={"path": "slow.py"},
    )
    tool_loop, _ = make_loop(
        [
            ScriptedGatewayTurn.tool_calls(call),
            ScriptedGatewayTurn.text("The timeout was handled."),
        ],
        handler=handler,
        config=AgentLoopConfig(tool_timeout_seconds=0.001),
    )
    tool_events = await collect_events(tool_loop)
    tool_failure = next(
        event
        for event in tool_events
        if isinstance(event, ToolCompletedEvent) and event.payload.status is ToolCallStatus.FAILED
    )
    assert tool_failure.payload.error is not None
    assert tool_failure.payload.error.code == "tool_timeout"
    assert tool_events[-1].event_type is EventType.RUN_COMPLETED


async def test_model_and_tool_output_limits_are_enforced() -> None:
    model_loop, _ = make_loop(
        [ScriptedGatewayTurn.text("too long")],
        config=AgentLoopConfig(max_model_output_bytes=3),
    )
    model_events = await collect_events(model_loop)
    model_failure = model_events[-1]
    assert isinstance(model_failure, RunFailedEvent)
    assert model_failure.payload.error.code == "model_output_limit"

    handler = ScriptedReadHandler(
        result=ToolExecutionResult(
            result={"content": "ok"},
            stdout=("x" * 60, "y" * 60),
        )
    )
    call = GatewayToolCall(
        id="tool-call-output",
        name="read_file",
        arguments={"path": "output.py"},
    )
    tool_loop, _ = make_loop(
        [
            ScriptedGatewayTurn.tool_calls(call),
            ScriptedGatewayTurn.text("Output was bounded."),
        ],
        handler=handler,
        config=AgentLoopConfig(max_tool_output_bytes=64),
    )
    tool_events = await collect_events(tool_loop)
    output_events = [event for event in tool_events if isinstance(event, ToolStdoutEvent)]
    assert sum(len(event.payload.chunk.encode("utf-8")) for event in output_events) == 64
    assert output_events[-1].payload.truncated is True


async def test_tool_argument_and_result_byte_limits_prevent_oversized_feedback() -> None:
    handler = ScriptedReadHandler(result=ToolExecutionResult(result={"content": "x" * 100}))
    oversized_arguments = GatewayToolCall(
        id="tool-call-large-arguments",
        name="read_file",
        arguments={"path": "x" * 100},
    )
    argument_loop, _ = make_loop(
        [ScriptedGatewayTurn.tool_calls(oversized_arguments)],
        handler=handler,
        config=AgentLoopConfig(max_tool_argument_bytes=10),
    )
    argument_events = await collect_events(argument_loop)
    argument_failure = argument_events[-1]
    assert isinstance(argument_failure, RunFailedEvent)
    assert argument_failure.payload.error.code == "tool_argument_limit"
    assert handler.calls == []

    result_call = GatewayToolCall(
        id="tool-call-large-result",
        name="read_file",
        arguments={"path": "result.py"},
    )
    result_loop, gateway = make_loop(
        [
            ScriptedGatewayTurn.tool_calls(result_call),
            ScriptedGatewayTurn.text("The large result was rejected."),
        ],
        handler=handler,
        config=AgentLoopConfig(max_tool_result_bytes=10),
    )
    result_events = await collect_events(result_loop)
    result_failure = next(
        event
        for event in result_events
        if isinstance(event, ToolCompletedEvent) and event.payload.status is ToolCallStatus.FAILED
    )
    assert result_failure.payload.error is not None
    assert result_failure.payload.error.code == "tool_result_limit"
    assert "x" * 50 not in gateway.requests[1].messages[-1].content


@pytest.mark.parametrize(
    ("turn", "expected_code"),
    [
        (
            ScriptedGatewayTurn(events=(GatewayTextDelta(delta="partial"),)),
            "incomplete_model_stream",
        ),
        (
            ScriptedGatewayTurn(
                events=(
                    GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP),
                    GatewayTextDelta(delta="late"),
                )
            ),
            "invalid_model_stream",
        ),
        (
            ScriptedGatewayTurn(
                events=(
                    GatewayTextDelta(delta="partial"),
                    GatewayResponseCompleted(finish_reason=GatewayFinishReason.LENGTH),
                )
            ),
            "model_response_truncated",
        ),
        (
            ScriptedGatewayTurn(
                events=(GatewayResponseCompleted(finish_reason=GatewayFinishReason.CONTENT_FILTER),)
            ),
            "model_response_blocked",
        ),
        (
            ScriptedGatewayTurn(
                events=(GatewayResponseCompleted(finish_reason=GatewayFinishReason.TOOL_CALLS),)
            ),
            "invalid_model_stream",
        ),
        (
            ScriptedGatewayTurn(
                events=(
                    GatewayTextDelta(delta="text"),
                    GatewayResponseCompleted(finish_reason=GatewayFinishReason.TOOL_CALLS),
                )
            ),
            "invalid_model_stream",
        ),
    ],
)
async def test_malformed_or_incomplete_model_streams_fail_closed(
    turn: ScriptedGatewayTurn,
    expected_code: str,
) -> None:
    loop, _ = make_loop([turn])

    events = await collect_events(loop)

    failed = events[-1]
    assert isinstance(failed, RunFailedEvent)
    assert failed.payload.error.code == expected_code


async def test_gateway_exception_is_sanitized() -> None:
    loop, _ = make_loop([ScriptedGatewayTurn(error=RuntimeError("api_key=do-not-leak"))])

    events = await collect_events(loop)

    failed = events[-1]
    assert isinstance(failed, RunFailedEvent)
    assert failed.payload.error.code == "model_gateway_failure"
    assert failed.payload.error.retryable is True
    assert "do-not-leak" not in failed.model_dump_json()


async def test_empty_model_response_uses_bounded_semantic_retry() -> None:
    empty_turn = ScriptedGatewayTurn(
        events=(GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP),)
    )
    loop, gateway = make_loop(
        [empty_turn],
        config=AgentLoopConfig(max_semantic_retries=0),
    )

    events = await collect_events(loop)

    assert len(gateway.requests) == 1
    failed = events[-1]
    assert isinstance(failed, RunFailedEvent)
    assert failed.payload.error.code == "semantic_retry_limit"


async def test_empty_model_response_can_recover_within_semantic_budget() -> None:
    empty_turn = ScriptedGatewayTurn(
        events=(GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP),)
    )
    loop, gateway = make_loop(
        [empty_turn, ScriptedGatewayTurn.text("Recovered response.")],
        config=AgentLoopConfig(max_semantic_retries=1),
    )

    events = await collect_events(loop)

    assert events[-1].event_type is EventType.RUN_COMPLETED
    assert len(gateway.requests) == 2
    assert gateway.requests[1].messages[-1] == GatewayMessage(
        role=MessageRole.SYSTEM,
        content="The previous response contained neither text nor tool calls.",
    )


async def test_exhausted_fake_gateway_returns_its_structured_failure() -> None:
    loop, gateway = make_loop([])

    events = await collect_events(loop)

    failed = events[-1]
    assert isinstance(failed, RunFailedEvent)
    assert failed.payload.error.code == "fake_gateway_exhausted"
    assert failed.payload.error.details["request_count"] == 1
    assert gateway.remaining_turns == 0


def test_loop_configuration_and_input_reject_unbounded_values() -> None:
    with pytest.raises(ValueError, match="greater than or equal"):
        AgentLoopConfig(max_turns=0)
    with pytest.raises(ValueError, match="less than or equal"):
        AgentLoopConfig(max_model_output_bytes=10**9)
    with pytest.raises(ValueError, match="finite"):
        AgentLoopConfig(model_timeout_seconds=float("inf"))
    with pytest.raises(ValueError, match="at least 1 item"):
        AgentLoopInput(
            run_id=RUN_ID,
            attempt=1,
            worker_id="worker-1",
            route_name="coding-default",
            messages=(),
        )
