from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from agent_core.domain import DomainOperationError, FrozenJsonObject
from agent_core.fakes import ScriptedGatewayTurn, SequentialIdGenerator, SteppingClock
from agent_core.gateway import (
    GatewayEventKind,
    GatewayFinishReason,
    GatewayMessage,
    GatewayRequest,
    GatewayResponseCompleted,
    GatewayTextDelta,
    GatewayToolCall,
    GatewayToolCallEvent,
    GatewayToolDefinition,
    MessageRole,
    parse_gateway_event,
)
from agent_core.tools import (
    RegisteredTool,
    ToolArguments,
    ToolExecutionResult,
    ToolRegistry,
)

NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)


class ReadArguments(ToolArguments):
    path: str


class ReadHandler:
    def __init__(self) -> None:
        self.calls: list[ReadArguments] = []

    async def __call__(self, arguments: ReadArguments) -> ToolExecutionResult:
        self.calls.append(arguments)
        return ToolExecutionResult(result={"path": arguments.path})


def read_registration(handler: ReadHandler | None = None) -> RegisteredTool[ReadArguments]:
    return RegisteredTool(
        name="read_file",
        description="Read one workspace file",
        arguments_type=ReadArguments,
        handler=handler or ReadHandler(),
    )


def test_gateway_message_roles_enforce_closed_transcript_shapes() -> None:
    tool_call = GatewayToolCall(
        id="tool-call-1",
        name="read_file",
        arguments={"path": "src/main.py"},
    )
    assistant = GatewayMessage(
        role=MessageRole.ASSISTANT,
        tool_calls=(tool_call,),
    )
    tool = GatewayMessage(
        role=MessageRole.TOOL,
        tool_call_id=tool_call.id,
        content='{"ok":true}',
    )

    assert assistant.tool_calls == (tool_call,)
    assert tool.tool_call_id == tool_call.id
    with pytest.raises(ValidationError, match="must contain content"):
        GatewayMessage(role=MessageRole.USER)
    with pytest.raises(ValidationError, match="may not contain tool-call fields"):
        GatewayMessage(
            role=MessageRole.SYSTEM,
            content="system",
            tool_call_id="unexpected",
        )
    with pytest.raises(ValidationError, match="content or tool calls"):
        GatewayMessage(role=MessageRole.ASSISTANT)
    with pytest.raises(ValidationError, match="must contain tool_call_id"):
        GatewayMessage(role=MessageRole.TOOL, content="result")


def test_gateway_request_rejects_empty_messages_and_duplicate_tools() -> None:
    definition = read_registration().definition
    values = {
        "run_id": "10000000-0000-0000-0000-000000000001",
        "model_call_id": "model-call-1",
        "request_id": "request-1",
        "route_name": "coding-default",
        "messages": [{"role": "user", "content": "Inspect the repository"}],
    }

    request = GatewayRequest.model_validate({**values, "tools": [definition]})
    assert request.tools == (definition,)
    with pytest.raises(ValidationError, match="at least 1 item"):
        GatewayRequest.model_validate({**values, "messages": []})
    with pytest.raises(ValidationError, match="must be unique"):
        GatewayRequest.model_validate({**values, "tools": [definition, definition]})


@pytest.mark.parametrize(
    ("value", "event_type"),
    [
        ({"kind": "text_delta", "delta": "hello"}, GatewayTextDelta),
        (
            {
                "kind": "tool_call",
                "tool_call": {
                    "id": "call-1",
                    "name": "read_file",
                    "arguments": {"path": "README.md"},
                },
            },
            GatewayToolCallEvent,
        ),
        (
            {"kind": "response_completed", "finish_reason": "stop"},
            GatewayResponseCompleted,
        ),
    ],
)
def test_parse_gateway_event_discriminates_untrusted_values(
    value: dict[str, object],
    event_type: type[object],
) -> None:
    event = parse_gateway_event(value)

    assert isinstance(event, event_type)
    assert parse_gateway_event(event.model_dump(mode="json")) == event


def test_gateway_events_reject_unknown_kinds_and_non_finite_usage() -> None:
    with pytest.raises(ValidationError, match="union_tag_invalid"):
        parse_gateway_event({"kind": "provider_specific"})
    with pytest.raises(ValidationError, match="finite"):
        GatewayResponseCompleted(
            finish_reason=GatewayFinishReason.STOP,
            input_tokens=float("inf"),
        )


async def test_tool_registry_validates_before_handler_execution() -> None:
    handler = ReadHandler()
    registry = ToolRegistry((read_registration(handler),))
    raw_arguments = FrozenJsonObject({"path": "src/main.py"})

    prepared = registry.prepare("read_file", raw_arguments)
    assert handler.calls == []
    result = await prepared.execute()

    assert len(handler.calls) == 1
    assert handler.calls[0].path == "src/main.py"
    assert result.result.to_json_object() == {"path": "src/main.py"}
    assert registry.definitions[0].input_schema["additionalProperties"] is False


def test_tool_registry_returns_sanitized_structured_validation_errors() -> None:
    registry = ToolRegistry((read_registration(),))

    with pytest.raises(DomainOperationError) as malformed:
        registry.prepare(
            "read_file",
            FrozenJsonObject({"secret_value": "do-not-reflect"}),
        )
    assert malformed.value.code == "malformed_tool_arguments"
    assert "do-not-reflect" not in malformed.value.as_dict().__repr__()
    assert malformed.value.as_dict()["details"] == {
        "tool_name": "read_file",
        "issues": [
            {"path": "path", "type": "missing"},
            {"path": "secret_value", "type": "extra_forbidden"},
        ],
    }

    with pytest.raises(DomainOperationError) as unknown:
        registry.prepare("unknown_tool", FrozenJsonObject({}))
    assert unknown.value.code == "unknown_tool"


def test_tool_registry_rejects_duplicate_names() -> None:
    with pytest.raises(ValueError, match="must be unique"):
        ToolRegistry((read_registration(), read_registration()))


def test_deterministic_fakes_validate_scripts_time_and_identifiers() -> None:
    text_turn = ScriptedGatewayTurn.text("abcdef", chunk_size=2)
    assert [event.delta for event in text_turn.events if isinstance(event, GatewayTextDelta)] == [
        "ab",
        "cd",
        "ef",
    ]
    assert text_turn.events[-1].kind is GatewayEventKind.RESPONSE_COMPLETED

    tool_call = GatewayToolCall(id="call-1", name="read_file", arguments={"path": "x"})
    tool_turn = ScriptedGatewayTurn.tool_calls(tool_call)
    assert isinstance(tool_turn.events[0], GatewayToolCallEvent)
    with pytest.raises(ValueError, match="positive"):
        ScriptedGatewayTurn.text("text", chunk_size=0)
    with pytest.raises(ValueError, match="negative"):
        ScriptedGatewayTurn(delay_seconds=-1)

    clock = SteppingClock(NOW, step=timedelta(seconds=1))
    assert clock.now() == NOW
    assert clock.now() == NOW + timedelta(seconds=1)
    with pytest.raises(ValueError, match="negative"):
        SteppingClock(NOW, step=timedelta(seconds=-1))

    ids = SequentialIdGenerator()
    assert ids.new_id("request") == "request-1"
    assert ids.new_id("request") == "request-2"
    with pytest.raises(ValueError, match="may not be empty"):
        ids.new_id("")


def test_gateway_tool_definition_requires_normalized_name_and_schema() -> None:
    with pytest.raises(ValidationError, match="string_pattern_mismatch"):
        GatewayToolDefinition(
            name="../read",
            description="Read a file",
            input_schema={},
        )
