from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any, Literal, cast
from uuid import UUID

import pytest

from agent_core.domain.status import ToolCallStatus
from agent_core.events import EventType, ModelToolCallReceivedEvent, ToolCompletedEvent
from agent_core.fakes import SequentialIdGenerator, SteppingClock
from agent_core.gateway import GatewayMessage, MessageRole
from agent_core.loop import AgentLoop, AgentLoopInput
from agent_core.settings import PlatformSettings
from agent_core.tools import (
    RegisteredTool,
    ToolArguments,
    ToolEffect,
    ToolExecutionCompleted,
    ToolExecutionContext,
    ToolExecutionEvent,
    ToolRegistry,
)
from agents_sdk_adapter import create_openai_compatible_agents_gateway
from gateway_client import GatewayClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


class EchoArguments(ToolArguments):
    text: str


class EchoTool:
    def __init__(self) -> None:
        self.values: list[str] = []

    async def run(
        self,
        arguments: EchoArguments,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        assert context.max_result_bytes > 0
        self.values.append(arguments.text)
        yield ToolExecutionCompleted(result={"echo": arguments.text})


class FakeOpenAIServer(ThreadingHTTPServer):
    recorded_requests: list[dict[str, Any]]
    authorization_headers: list[str]
    mode: Literal["normal", "malformed"]

    def __init__(self, *, mode: Literal["normal", "malformed"] = "normal") -> None:
        super().__init__(("127.0.0.1", 0), FakeOpenAIHandler)
        self.recorded_requests = []
        self.authorization_headers = []
        self.mode = mode


class FakeOpenAIHandler(BaseHTTPRequestHandler):
    server_version = "Sequence4FakeOpenAI/1.0"

    def do_POST(self) -> None:
        server = cast("FakeOpenAIServer", self.server)
        if self.path != "/v1/chat/completions":
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        request = json.loads(self.rfile.read(content_length))
        if not isinstance(request, dict):
            self.send_error(HTTPStatus.BAD_REQUEST)
            return
        server.recorded_requests.append(request)
        server.authorization_headers.append(self.headers.get("Authorization", ""))

        messages = request.get("messages", [])
        has_tool_result = any(
            isinstance(message, dict) and message.get("role") == "tool" for message in messages
        )
        if server.mode == "malformed" and len(server.recorded_requests) == 1:
            events = _malformed_tool_call_chunks()
        else:
            events = _final_text_chunks() if has_tool_result else _fragmented_tool_call_chunks()
        payload = b"".join(
            b"data: " + json.dumps(event, ensure_ascii=False).encode() + b"\n\n" for event in events
        )
        payload += b"data: [DONE]\n\n"

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
        self.wfile.flush()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        del format, args


def _chunk(
    *,
    choices: list[dict[str, Any]],
    usage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "id": "chatcmpl-sequence-4",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "coding-default",
        "choices": choices,
    }
    if usage is not None:
        result["usage"] = usage
    return result


def _fragmented_tool_call_chunks() -> tuple[dict[str, Any], ...]:
    return (
        _chunk(
            choices=[
                {
                    "index": 0,
                    "delta": {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "call-sdk-1",
                                "type": "function",
                                "function": {
                                    "name": "echo_text",
                                    "arguments": '{"text":"',
                                },
                            }
                        ]
                    },
                    "finish_reason": None,
                }
            ]
        ),
        _chunk(
            choices=[
                {
                    "index": 0,
                    "delta": {
                        "tool_calls": [
                            {
                                "index": 0,
                                "function": {"arguments": '雪"}'},
                            }
                        ]
                    },
                    "finish_reason": None,
                }
            ]
        ),
        _chunk(
            choices=[
                {
                    "index": 0,
                    "delta": {},
                    "finish_reason": "tool_calls",
                }
            ]
        ),
        _chunk(
            choices=[],
            usage={
                "prompt_tokens": 2,
                "completion_tokens": 1,
                "total_tokens": 3,
                "prompt_tokens_details": {"cached_tokens": 1},
            },
        ),
    )


def _malformed_tool_call_chunks() -> tuple[dict[str, Any], ...]:
    return (
        _chunk(
            choices=[
                {
                    "index": 0,
                    "delta": {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "call-sdk-invalid",
                                "type": "function",
                                "function": {
                                    "name": "echo_text",
                                    "arguments": '{"text":"raw-rejected-sentinel',
                                },
                            }
                        ]
                    },
                    "finish_reason": None,
                }
            ]
        ),
        _chunk(
            choices=[
                {
                    "index": 0,
                    "delta": {},
                    "finish_reason": "tool_calls",
                }
            ]
        ),
        _chunk(
            choices=[],
            usage={
                "prompt_tokens": 2,
                "completion_tokens": 1,
                "total_tokens": 3,
            },
        ),
    )


def _final_text_chunks() -> tuple[dict[str, Any], ...]:
    return (
        _chunk(
            choices=[
                {
                    "index": 0,
                    "delta": {"content": "Tool observed "},
                    "finish_reason": None,
                }
            ]
        ),
        _chunk(
            choices=[
                {
                    "index": 0,
                    "delta": {"content": "雪."},
                    "finish_reason": None,
                }
            ]
        ),
        _chunk(
            choices=[
                {
                    "index": 0,
                    "delta": {},
                    "finish_reason": "stop",
                }
            ]
        ),
        _chunk(
            choices=[],
            usage={
                "prompt_tokens": 4,
                "completion_tokens": 2,
                "total_tokens": 6,
            },
        ),
    )


@pytest.mark.integration
async def test_agents_sdk_streams_fragmented_tool_round_trip_through_agent_loop() -> None:
    server = FakeOpenAIServer()
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    adapter = create_openai_compatible_agents_gateway(
        PlatformSettings(
            gateway_url=f"http://127.0.0.1:{server.server_port}",
            gateway_api_key="sk-sequence-4-test",
        )
    )
    gateway = GatewayClient(adapter, close=adapter.aclose)
    echo_tool = EchoTool()
    registry = ToolRegistry(
        (
            RegisteredTool(
                name="echo_text",
                description="Echo a UTF-8 value through the typed tool boundary",
                arguments_type=EchoArguments,
                handler=echo_tool.run,
                effect=ToolEffect.READ_ONLY,
            ),
        )
    )
    loop = AgentLoop(
        gateway=gateway,
        tools=registry,
        clock=SteppingClock(datetime(2026, 7, 28, 12, tzinfo=UTC)),
        id_generator=SequentialIdGenerator(),
    )
    loop_input = AgentLoopInput(
        tenant_id=UUID("00000000-0000-0000-0000-000000000010"),
        session_id=UUID("00000000-0000-0000-0000-000000000020"),
        run_id=UUID("20000000-0000-0000-0000-000000000001"),
        attempt=1,
        worker_id="sdk-integration-worker",
        route_name="coding-default",
        messages=(
            GatewayMessage(
                role=MessageRole.SYSTEM,
                content="Use the available repository tools.",
            ),
            GatewayMessage(
                role=MessageRole.USER,
                content="Echo the requested UTF-8 value.",
            ),
        ),
    )

    try:
        async with gateway:
            events = [event async for event in loop.run(loop_input)]
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)

    assert server_thread.is_alive() is False
    assert echo_tool.values == ["雪"]
    assert events[-1].event_type is EventType.RUN_COMPLETED
    assert events[-1].payload.final_text == "Tool observed 雪."
    assert any(isinstance(event, ToolCompletedEvent) for event in events)
    assert [event.sequence for event in events] == list(range(1, len(events) + 1))
    assert len(server.recorded_requests) == 2
    assert server.authorization_headers == [
        "Bearer sk-sequence-4-test",
        "Bearer sk-sequence-4-test",
    ]
    assert server.recorded_requests[0]["model"] == "coding-default"
    assert server.recorded_requests[0]["stream"] is True
    assert server.recorded_requests[0]["messages"][0] == {
        "content": "Use the available repository tools.",
        "role": "system",
    }
    assert server.recorded_requests[0]["tools"][0]["function"]["name"] == "echo_text"
    assert any(
        message.get("role") == "tool" and '"echo":"雪"' in message.get("content", "")
        for message in server.recorded_requests[1]["messages"]
    )


@pytest.mark.integration
async def test_agents_sdk_recovers_from_fragmented_malformed_tool_arguments() -> None:
    rejected_raw_value = "raw-rejected-sentinel"
    server = FakeOpenAIServer(mode="malformed")
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    adapter = create_openai_compatible_agents_gateway(
        PlatformSettings(
            gateway_url=f"http://127.0.0.1:{server.server_port}",
            gateway_api_key="sk-sequence-4-test",
        )
    )
    gateway = GatewayClient(adapter, close=adapter.aclose)
    echo_tool = EchoTool()
    registry = ToolRegistry(
        (
            RegisteredTool(
                name="echo_text",
                description="Echo a UTF-8 value through the typed tool boundary",
                arguments_type=EchoArguments,
                handler=echo_tool.run,
                effect=ToolEffect.READ_ONLY,
            ),
        )
    )
    loop = AgentLoop(
        gateway=gateway,
        tools=registry,
        clock=SteppingClock(datetime(2026, 7, 28, 12, tzinfo=UTC)),
        id_generator=SequentialIdGenerator(),
    )
    loop_input = AgentLoopInput(
        tenant_id=UUID("00000000-0000-0000-0000-000000000010"),
        session_id=UUID("00000000-0000-0000-0000-000000000020"),
        run_id=UUID("20000000-0000-0000-0000-000000000002"),
        attempt=1,
        worker_id="sdk-integration-worker",
        route_name="coding-default",
        messages=(
            GatewayMessage(
                role=MessageRole.USER,
                content="Recover from malformed arguments, then echo the requested value.",
            ),
        ),
    )

    try:
        async with gateway:
            events = [event async for event in loop.run(loop_input)]
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)

    assert server_thread.is_alive() is False
    assert echo_tool.values == ["雪"]
    rejected = next(
        event
        for event in events
        if isinstance(event, ModelToolCallReceivedEvent)
        and event.payload.tool_call_id == "call-sdk-invalid"
    )
    assert rejected.payload.arguments is None
    assert rejected.payload.argument_hash is None
    assert rejected.payload.error is not None
    assert rejected.payload.error.code == "malformed_tool_arguments"
    failed = next(
        event
        for event in events
        if isinstance(event, ToolCompletedEvent)
        and event.payload.tool_call_id == "call-sdk-invalid"
    )
    assert failed.payload.status is ToolCallStatus.FAILED
    assert failed.payload.error is not None
    assert failed.payload.error.code == "malformed_tool_arguments"
    assert events[-1].event_type is EventType.RUN_COMPLETED
    assert len(server.recorded_requests) == 3
    correction = json.dumps(server.recorded_requests[1], sort_keys=True)
    later_requests = json.dumps(server.recorded_requests[1:], sort_keys=True)
    serialized_events = "\n".join(event.model_dump_json() for event in events)
    assert "malformed_tool_arguments" in correction
    assert len(correction.encode("utf-8")) < 64 * 1024
    assert rejected_raw_value not in correction
    assert rejected_raw_value not in later_requests
    assert rejected_raw_value not in serialized_events
