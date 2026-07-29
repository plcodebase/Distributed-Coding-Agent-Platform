import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from uuid import UUID

import pytest

from agent_core.domain import DomainOperationError, ErrorDetail, JsonObject, ToolCallStatus
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
    GatewayInvalidToolCallEvent,
    GatewayMessage,
    GatewayResponseCompleted,
    GatewayTextDelta,
    GatewayToolCall,
    GatewayToolCallEvent,
    MessageRole,
)
from agent_core.loop import AgentLoop, AgentLoopConfig, AgentLoopInput
from agent_core.tools import (
    RegisteredTool,
    ToolArguments,
    ToolEffect,
    ToolExecutionCompleted,
    ToolExecutionContext,
    ToolExecutionEvent,
    ToolHandler,
    ToolOutputChannel,
    ToolOutputChunk,
    ToolRegistry,
)
from platform_telemetry import Redactor

NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)
RUN_ID = UUID("10000000-0000-0000-0000-000000000001")


class ReadArguments(ToolArguments):
    path: str


class ScriptedReadHandler:
    def __init__(
        self,
        *,
        result: JsonObject | None = None,
        output: tuple[ToolOutputChunk, ...] = (),
        error: Exception | None = None,
        delay_seconds: float = 0,
    ) -> None:
        self.calls: list[ReadArguments] = []
        self.contexts: list[ToolExecutionContext] = []
        self.result = result
        self.output = output
        self.error = error
        self.delay_seconds = delay_seconds
        self.closed = False

    async def __call__(
        self,
        arguments: ReadArguments,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        self.calls.append(arguments)
        self.contexts.append(context)
        try:
            if self.delay_seconds:
                await asyncio.sleep(self.delay_seconds)
            if self.error is not None:
                raise self.error
            for output in self.output:
                yield output
            yield ToolExecutionCompleted(
                result=self.result
                if self.result is not None
                else {"path": arguments.path, "content": "source"}
            )
        finally:
            self.closed = True


class UnboundedOutputHandler:
    def __init__(self) -> None:
        self.calls: list[ReadArguments] = []
        self.produced = 0
        self.closed = False

    async def __call__(
        self,
        arguments: ReadArguments,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        self.calls.append(arguments)
        assert context.max_output_bytes == 8
        try:
            while True:
                self.produced += 1
                yield ToolOutputChunk(
                    channel=ToolOutputChannel.STDOUT,
                    chunk="🙂",
                )
        finally:
            self.closed = True


class InvalidStreamHandler:
    def __init__(self, *, output_after_completion: bool) -> None:
        self.output_after_completion = output_after_completion

    async def __call__(
        self,
        arguments: ReadArguments,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        assert arguments.path
        assert context.max_result_bytes > 0
        if self.output_after_completion:
            yield ToolExecutionCompleted(result={"content": "premature"})
        yield ToolOutputChunk(channel=ToolOutputChannel.STDOUT, chunk="orphan")


def read_tool(handler: ToolHandler[ReadArguments]) -> RegisteredTool[ReadArguments]:
    return RegisteredTool(
        name="read_file",
        description="Read one workspace file",
        arguments_type=ReadArguments,
        handler=handler,
        effect=ToolEffect.READ_ONLY,
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
    handler: ToolHandler[ReadArguments] | None = None,
    config: AgentLoopConfig | None = None,
    redactor: Redactor | None = None,
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
            redactor=redactor,
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
        result={"content": "print('hello')"},
        output=(
            ToolOutputChunk(channel=ToolOutputChannel.STDOUT, chunk="read stdout"),
            ToolOutputChunk(channel=ToolOutputChannel.STDERR, chunk="read warning"),
        ),
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
    assert received.payload.argument_hash is not None
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


async def test_invalid_json_tool_call_is_recorded_and_retried_without_raw_arguments() -> None:
    known_value = "exact-secret-value"
    handler = ScriptedReadHandler()
    loop, gateway = make_loop(
        [
            ScriptedGatewayTurn.invalid_tool_call(
                tool_call_id="tool-call-invalid-json",
                tool_name="read_file",
                error=ErrorDetail(
                    code="malformed_tool_arguments",
                    message=f"invalid arguments included {known_value}",
                    details={"token": known_value},
                ),
            ),
            ScriptedGatewayTurn.text("I corrected the invalid JSON."),
        ],
        handler=handler,
        redactor=Redactor((known_value,)),
    )

    events = await collect_events(loop)

    assert handler.calls == []
    received = next(event for event in events if isinstance(event, ModelToolCallReceivedEvent))
    assert received.payload.arguments is None
    assert received.payload.argument_hash is None
    assert received.payload.error is not None
    assert received.payload.error.code == "malformed_tool_arguments"
    assert known_value not in received.model_dump_json()
    failed_tool = next(
        event
        for event in events
        if isinstance(event, ToolCompletedEvent) and event.payload.status is ToolCallStatus.FAILED
    )
    assert failed_tool.payload.error is not None
    assert failed_tool.payload.error.code == "malformed_tool_arguments"
    assert gateway.requests[1].messages[-1].role is MessageRole.SYSTEM
    assert "malformed_tool_arguments" in gateway.requests[1].messages[-1].content
    assert known_value not in gateway.requests[1].messages[-1].content
    assert events[-1].event_type is EventType.RUN_COMPLETED

    exhausted_loop, _ = make_loop(
        [
            ScriptedGatewayTurn.invalid_tool_call(
                tool_call_id="tool-call-invalid-json",
                tool_name="read_file",
            )
        ],
        handler=handler,
        config=AgentLoopConfig(max_semantic_retries=0),
    )
    exhausted_events = await collect_events(exhausted_loop)
    exhausted = exhausted_events[-1]
    assert isinstance(exhausted, RunFailedEvent)
    assert exhausted.payload.error.code == "semantic_retry_limit"


async def test_mixed_valid_and_invalid_tool_calls_are_rejected_atomically() -> None:
    handler = ScriptedReadHandler()
    valid_call = GatewayToolCall(
        id="tool-call-valid",
        name="read_file",
        arguments={"path": "safe.py"},
    )
    mixed_turn = ScriptedGatewayTurn(
        events=(
            GatewayToolCallEvent(tool_call=valid_call),
            GatewayInvalidToolCallEvent(
                tool_call_id="tool-call-invalid",
                tool_name="read_file",
                error=ErrorDetail(
                    code="malformed_tool_arguments",
                    message="arguments were not valid JSON",
                ),
            ),
            GatewayResponseCompleted(finish_reason=GatewayFinishReason.TOOL_CALLS),
        )
    )
    loop, gateway = make_loop(
        [mixed_turn, ScriptedGatewayTurn.text("The complete turn was corrected.")],
        handler=handler,
    )

    events = await collect_events(loop)

    assert handler.calls == []
    failures = [
        event.payload.error.code
        for event in events
        if isinstance(event, ToolCompletedEvent) and event.payload.error is not None
    ]
    assert failures == ["tool_turn_rejected", "malformed_tool_arguments"]
    assert [message.role for message in gateway.requests[1].messages] == [
        MessageRole.USER,
        MessageRole.ASSISTANT,
        MessageRole.TOOL,
        MessageRole.SYSTEM,
    ]


async def test_sensitive_tool_arguments_are_rejected_without_event_disclosure() -> None:
    known_value = "exact-secret-value"
    handler = ScriptedReadHandler()
    sensitive_call = GatewayToolCall(
        id="tool-call-sensitive",
        name="read_file",
        arguments={"token": known_value},
    )
    loop, gateway = make_loop(
        [
            ScriptedGatewayTurn.tool_calls(sensitive_call),
            ScriptedGatewayTurn.text("I removed the sensitive argument."),
        ],
        handler=handler,
        redactor=Redactor((known_value,)),
    )

    events = await collect_events(loop)

    assert handler.calls == []
    serialized = "".join(event.model_dump_json() for event in events)
    assert known_value not in serialized
    received = next(event for event in events if isinstance(event, ModelToolCallReceivedEvent))
    assert received.payload.arguments is None
    assert received.payload.error is not None
    assert received.payload.error.code == "sensitive_tool_arguments"
    assert known_value not in gateway.requests[1].messages[-1].content


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


async def test_tool_results_output_and_structured_errors_are_redacted() -> None:
    known_value = "exact-secret-value"
    result_handler = ScriptedReadHandler(
        result={
            "token": known_value,
            "content": "sk-abcdefghijk",
        },
        output=(
            ToolOutputChunk(channel=ToolOutputChannel.STDOUT, chunk="exact-secret-"),
            ToolOutputChunk(channel=ToolOutputChannel.STDOUT, chunk="value"),
            ToolOutputChunk(
                channel=ToolOutputChannel.STDERR,
                chunk="Bearer abcdefghijklmnop",
            ),
        ),
    )
    result_call = GatewayToolCall(
        id="tool-call-redacted",
        name="read_file",
        arguments={"path": "safe.py"},
    )
    result_loop, result_gateway = make_loop(
        [
            ScriptedGatewayTurn.tool_calls(result_call),
            ScriptedGatewayTurn.text("The result was sanitized."),
        ],
        handler=result_handler,
        redactor=Redactor((known_value,)),
    )

    result_events = await collect_events(result_loop)

    serialized = "".join(event.model_dump_json() for event in result_events)
    assert known_value not in serialized
    assert "sk-abcdefghijk" not in serialized
    assert "Bearer abcdefghijklmnop" not in serialized
    assert known_value not in result_gateway.requests[1].messages[-1].content
    completed = next(
        event
        for event in result_events
        if isinstance(event, ToolCompletedEvent)
        and event.payload.status is ToolCallStatus.COMPLETED
    )
    assert completed.payload.result is not None
    assert completed.payload.result["token"] == "[REDACTED]"  # noqa: S105

    error_handler = ScriptedReadHandler(
        error=DomainOperationError(
            code="workspace_unavailable",
            message=f"workspace failed with {known_value}",
            details={"credential": known_value},
        )
    )
    error_call = GatewayToolCall(
        id="tool-call-redacted-error",
        name="read_file",
        arguments={"path": "safe.py"},
    )
    error_loop, error_gateway = make_loop(
        [
            ScriptedGatewayTurn.tool_calls(error_call),
            ScriptedGatewayTurn.text("The error was sanitized."),
        ],
        handler=error_handler,
        redactor=Redactor((known_value,)),
    )

    error_events = await collect_events(error_loop)

    serialized_error = "".join(event.model_dump_json() for event in error_events)
    assert known_value not in serialized_error
    assert known_value not in error_gateway.requests[1].messages[-1].content
    failed = next(
        event
        for event in error_events
        if isinstance(event, ToolCompletedEvent) and event.payload.status is ToolCallStatus.FAILED
    )
    assert failed.payload.error is not None
    assert failed.payload.error.code == "workspace_unavailable"
    assert failed.payload.error.details["credential"] == "[REDACTED]"

    oversized_error_handler = ScriptedReadHandler(
        error=DomainOperationError(
            code="workspace_unavailable",
            message="workspace failed safely",
            details={"diagnostic": "x" * 70_000},
        )
    )
    oversized_error_loop, _ = make_loop(
        [
            ScriptedGatewayTurn.tool_calls(error_call),
            ScriptedGatewayTurn.text("The large error was bounded."),
        ],
        handler=oversized_error_handler,
    )

    oversized_error_events = await collect_events(oversized_error_loop)

    bounded_failure = next(
        event
        for event in oversized_error_events
        if isinstance(event, ToolCompletedEvent) and event.payload.status is ToolCallStatus.FAILED
    )
    assert bounded_failure.payload.error is not None
    assert bounded_failure.payload.error.details.to_json_object() == {"details_truncated": True}


async def test_initial_context_is_redacted_before_gateway_access() -> None:
    known_value = "exact-secret-value"
    loop, gateway = make_loop(
        [ScriptedGatewayTurn.text("The input was sanitized.")],
        redactor=Redactor((known_value,)),
    )
    loop_input = default_input().model_copy(
        update={
            "messages": (
                GatewayMessage(
                    role=MessageRole.USER,
                    content=f"Inspect without exposing {known_value}",
                ),
            )
        }
    )

    events = [event async for event in loop.run(loop_input)]

    assert events[-1].event_type is EventType.RUN_COMPLETED
    assert known_value not in gateway.requests[0].model_dump_json()
    assert "[REDACTED]" in gateway.requests[0].messages[0].content


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


async def test_duplicate_tool_call_reuses_outcome_and_conflicting_hash_fails_closed() -> None:
    handler = ScriptedReadHandler(result={"content": "stable"})
    original = GatewayToolCall(
        id="stable-tool-call",
        name="read_file",
        arguments={"path": "same.py"},
    )
    duplicate_loop, duplicate_gateway = make_loop(
        [
            ScriptedGatewayTurn.tool_calls(original),
            ScriptedGatewayTurn.tool_calls(original),
            ScriptedGatewayTurn.text("The stable outcome was reused."),
        ],
        handler=handler,
    )

    duplicate_events = await collect_events(duplicate_loop)

    assert len(handler.calls) == 1
    assert len(duplicate_gateway.requests) == 3
    assert sum(event.event_type is EventType.TOOL_STARTED for event in duplicate_events) == 1
    assert (
        sum(
            isinstance(event, ToolCompletedEvent)
            and event.payload.status is ToolCallStatus.COMPLETED
            for event in duplicate_events
        )
        == 2
    )

    conflicting = GatewayToolCall(
        id=original.id,
        name=original.name,
        arguments={"path": "different.py"},
    )
    conflict_handler = ScriptedReadHandler()
    conflict_loop, conflict_gateway = make_loop(
        [
            ScriptedGatewayTurn.tool_calls(original),
            ScriptedGatewayTurn.tool_calls(conflicting),
        ],
        handler=conflict_handler,
    )

    conflict_events = await collect_events(conflict_loop)

    assert len(conflict_handler.calls) == 1
    assert len(conflict_gateway.requests) == 2
    conflict_failure = conflict_events[-1]
    assert isinstance(conflict_failure, RunFailedEvent)
    assert conflict_failure.payload.error.code == "tool_call_id_conflict"


async def test_context_message_and_gateway_request_limits_prevent_model_access() -> None:
    message_loop, message_gateway = make_loop(
        [ScriptedGatewayTurn.text("not reached")],
        config=AgentLoopConfig(max_context_messages=1),
    )
    message_input = default_input().model_copy(
        update={
            "messages": (
                GatewayMessage(role=MessageRole.USER, content="first"),
                GatewayMessage(role=MessageRole.USER, content="second"),
            )
        }
    )

    message_events = [event async for event in message_loop.run(message_input)]

    assert message_gateway.requests == ()
    message_failure = message_events[-1]
    assert isinstance(message_failure, RunFailedEvent)
    assert message_failure.payload.error.code == "context_limit"

    byte_loop, byte_gateway = make_loop(
        [ScriptedGatewayTurn.text("not reached")],
        config=AgentLoopConfig(max_gateway_request_bytes=128),
    )
    byte_input = default_input().model_copy(
        update={
            "messages": (
                GatewayMessage(
                    role=MessageRole.USER,
                    content="x" * 1024,
                ),
            )
        }
    )

    byte_events = [event async for event in byte_loop.run(byte_input)]

    assert byte_gateway.requests == ()
    byte_failure = byte_events[-1]
    assert isinstance(byte_failure, RunFailedEvent)
    assert byte_failure.payload.error.code == "context_limit"


async def test_accumulated_context_is_bounded_before_the_next_gateway_call() -> None:
    empty_turn = ScriptedGatewayTurn(
        events=(GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP),)
    )
    measuring_loop, measuring_gateway = make_loop(
        [empty_turn, ScriptedGatewayTurn.text("completed")]
    )
    measuring_events = await collect_events(measuring_loop)
    assert measuring_events[-1].event_type is EventType.RUN_COMPLETED
    request_sizes = [
        len(request.model_dump_json().encode("utf-8")) for request in measuring_gateway.requests
    ]
    assert request_sizes[1] > request_sizes[0]
    boundary = (request_sizes[0] + request_sizes[1]) // 2

    bounded_loop, bounded_gateway = make_loop(
        [empty_turn, ScriptedGatewayTurn.text("not reached")],
        config=AgentLoopConfig(max_gateway_request_bytes=boundary),
    )

    bounded_events = await collect_events(bounded_loop)

    assert len(bounded_gateway.requests) == 1
    bounded_failure = bounded_events[-1]
    assert isinstance(bounded_failure, RunFailedEvent)
    assert bounded_failure.payload.error.code == "context_limit"


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


@pytest.mark.parametrize("output_after_completion", [False, True])
async def test_invalid_tool_streams_fail_safely(output_after_completion: bool) -> None:
    call = GatewayToolCall(
        id="tool-call-invalid-stream",
        name="read_file",
        arguments={"path": "invalid.py"},
    )
    loop, _ = make_loop(
        [
            ScriptedGatewayTurn.tool_calls(call),
            ScriptedGatewayTurn.text("The invalid stream was handled."),
        ],
        handler=InvalidStreamHandler(output_after_completion=output_after_completion),
    )

    events = await collect_events(loop)

    failed_tool = next(
        event
        for event in events
        if isinstance(event, ToolCompletedEvent) and event.payload.status is ToolCallStatus.FAILED
    )
    assert failed_tool.payload.error is not None
    assert failed_tool.payload.error.code == "invalid_tool_stream"
    assert events[-1].event_type is EventType.RUN_COMPLETED


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
        result={"content": "ok"},
        output=(
            ToolOutputChunk(channel=ToolOutputChannel.STDOUT, chunk="x" * 60),
            ToolOutputChunk(channel=ToolOutputChannel.STDOUT, chunk="y" * 60),
        ),
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
    output_failure = next(
        event
        for event in tool_events
        if isinstance(event, ToolCompletedEvent) and event.payload.status is ToolCallStatus.FAILED
    )
    assert output_failure.payload.error is not None
    assert output_failure.payload.error.code == "tool_output_limit"
    assert handler.closed is True
    assert handler.contexts == [
        ToolExecutionContext(
            run_id=RUN_ID,
            tool_call_id="tool-call-output",
            max_output_bytes=64,
            max_result_bytes=256 * 1024,
        )
    ]


async def test_tool_stream_preserves_channel_order_and_cancels_unbounded_producer() -> None:
    ordered_handler = ScriptedReadHandler(
        result={"content": "ok"},
        output=(
            ToolOutputChunk(channel=ToolOutputChannel.STDOUT, chunk="first"),
            ToolOutputChunk(channel=ToolOutputChannel.STDERR, chunk="warning"),
            ToolOutputChunk(channel=ToolOutputChannel.STDOUT, chunk="last"),
        ),
    )
    ordered_call = GatewayToolCall(
        id="tool-call-ordered",
        name="read_file",
        arguments={"path": "ordered.py"},
    )
    ordered_loop, _ = make_loop(
        [
            ScriptedGatewayTurn.tool_calls(ordered_call),
            ScriptedGatewayTurn.text("Ordered output was consumed."),
        ],
        handler=ordered_handler,
    )

    ordered_events = await collect_events(ordered_loop)

    channel_events = [
        event for event in ordered_events if isinstance(event, ToolStdoutEvent | ToolStderrEvent)
    ]
    assert [type(event) for event in channel_events] == [
        ToolStdoutEvent,
        ToolStderrEvent,
        ToolStdoutEvent,
    ]
    assert [event.payload.chunk for event in channel_events] == ["first", "warning", "last"]

    unbounded_handler = UnboundedOutputHandler()
    unbounded_call = GatewayToolCall(
        id="tool-call-unbounded",
        name="read_file",
        arguments={"path": "unbounded.py"},
    )
    unbounded_loop, _ = make_loop(
        [
            ScriptedGatewayTurn.tool_calls(unbounded_call),
            ScriptedGatewayTurn.text("The producer was cancelled."),
        ],
        handler=unbounded_handler,
        config=AgentLoopConfig(max_tool_output_bytes=8),
    )

    unbounded_events = await collect_events(unbounded_loop)

    bounded_output = [event for event in unbounded_events if isinstance(event, ToolStdoutEvent)]
    assert sum(len(event.payload.chunk.encode("utf-8")) for event in bounded_output) == 8
    assert bounded_output[-1].payload.truncated is True
    assert unbounded_handler.produced == 3
    assert unbounded_handler.closed is True


async def test_tool_argument_and_result_byte_limits_prevent_oversized_feedback() -> None:
    handler = ScriptedReadHandler(result={"content": "x" * 100})
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

    expanding_handler = ScriptedReadHandler(result={"token": "x"})
    expanding_call = GatewayToolCall(
        id="tool-call-redaction-expands",
        name="read_file",
        arguments={"path": "result.py"},
    )
    expanding_loop, _ = make_loop(
        [
            ScriptedGatewayTurn.tool_calls(expanding_call),
            ScriptedGatewayTurn.text("The expanded result was bounded."),
        ],
        handler=expanding_handler,
        config=AgentLoopConfig(max_tool_result_bytes=16),
    )
    expanding_events = await collect_events(expanding_loop)
    expanding_failure = next(
        event
        for event in expanding_events
        if isinstance(event, ToolCompletedEvent) and event.payload.status is ToolCallStatus.FAILED
    )
    assert expanding_failure.payload.error is not None
    assert expanding_failure.payload.error.code == "tool_result_limit"


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

    known_value = "exact-secret-value"
    domain_loop, _ = make_loop(
        [
            ScriptedGatewayTurn(
                error=DomainOperationError(
                    code="gateway_unavailable",
                    message=f"gateway rejected {known_value}",
                    details={"api_key": known_value},
                )
            )
        ],
        redactor=Redactor((known_value,)),
    )
    domain_events = await collect_events(domain_loop)
    domain_failure = domain_events[-1]
    assert isinstance(domain_failure, RunFailedEvent)
    assert domain_failure.payload.error.code == "gateway_unavailable"
    assert known_value not in domain_failure.model_dump_json()


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
    with pytest.raises(ValueError, match="less than or equal"):
        AgentLoopConfig(max_gateway_request_bytes=10**9)
    with pytest.raises(ValueError, match="greater than or equal"):
        AgentLoopConfig(max_tool_output_bytes=3)
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
