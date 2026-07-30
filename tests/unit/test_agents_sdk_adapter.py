from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING, cast

import httpx
import pytest
from agents import FunctionTool, ModelSettings
from agents.models.interface import ModelTracing
from openai.types.responses import (
    Response,
    ResponseCompletedEvent,
    ResponseErrorEvent,
    ResponseFileSearchToolCall,
    ResponseFunctionToolCall,
    ResponseIncompleteEvent,
    ResponseOutputItemDoneEvent,
    ResponseOutputMessage,
    ResponseOutputRefusal,
    ResponseRefusalDeltaEvent,
    ResponseTextDeltaEvent,
    ResponseUsage,
)
from openai.types.responses.response import IncompleteDetails
from openai.types.responses.response_usage import InputTokensDetails, OutputTokensDetails

from agent_core.domain.errors import DomainOperationError
from agent_core.gateway import (
    GatewayEvent,
    GatewayFinishReason,
    GatewayInvalidToolCallEvent,
    GatewayMessage,
    GatewayRequest,
    GatewayResponseCompleted,
    GatewayTextDelta,
    GatewayToolCall,
    GatewayToolCallEvent,
    GatewayToolDefinition,
    MessageRole,
)
from agent_core.settings import PlatformSettings
from agents_sdk_adapter._conversion import convert_request_input, convert_tool_definitions
from agents_sdk_adapter.factory import create_openai_compatible_agents_gateway
from agents_sdk_adapter.gateway import OpenAIAgentsGateway

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator
    from typing import Any

    from agents.items import TResponseStreamEvent
    from agents.models.interface import Model, ModelProvider
    from openai import AsyncOpenAI


class FakeSdkModel:
    def __init__(
        self,
        events: list[TResponseStreamEvent],
        *,
        failure: Exception | None = None,
    ) -> None:
        self.events = events
        self.failure = failure
        self.calls: list[dict[str, object]] = []
        self.closed = False

    def stream_response(self, **kwargs: object) -> AsyncIterator[TResponseStreamEvent]:
        self.calls.append(kwargs)

        async def generate() -> AsyncIterator[TResponseStreamEvent]:
            try:
                for event in self.events:
                    yield event
                if self.failure is not None:
                    raise self.failure
            finally:
                self.closed = True

        return generate()


class FakeSdkProvider:
    def __init__(self, model: FakeSdkModel) -> None:
        self.model = model
        self.model_names: list[str | None] = []
        self.closed = False

    def get_model(self, model_name: str | None) -> Model:
        self.model_names.append(model_name)
        return cast("Model", self.model)

    async def aclose(self) -> None:
        self.closed = True


class FakeOwnedClient:
    def __init__(self) -> None:
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1


class RetryableProvider(FakeSdkProvider):
    def __init__(self, model: FakeSdkModel, known_value: str) -> None:
        super().__init__(model)
        self.known_value = known_value
        self.close_calls = 0

    async def aclose(self) -> None:
        self.close_calls += 1
        if self.close_calls == 1:
            raise RuntimeError(self.known_value)
        await super().aclose()


class RetryableOwnedClient(FakeOwnedClient):
    def __init__(self, known_value: str) -> None:
        super().__init__()
        self.known_value = known_value

    async def close(self) -> None:
        self.close_calls += 1
        if self.close_calls == 1:
            raise RuntimeError(self.known_value)


class BlockingProvider(FakeSdkProvider):
    def __init__(self, model: FakeSdkModel) -> None:
        super().__init__(model)
        self.close_started = asyncio.Event()
        self.close_release = asyncio.Event()

    async def aclose(self) -> None:
        self.close_started.set()
        await self.close_release.wait()
        await super().aclose()


def gateway_request(
    *,
    messages: tuple[GatewayMessage, ...] | None = None,
    tools: tuple[GatewayToolDefinition, ...] = (),
) -> GatewayRequest:
    return GatewayRequest(
        tenant_id=uuid.UUID("00000000-0000-0000-0000-000000000010"),
        session_id=uuid.UUID("00000000-0000-0000-0000-000000000020"),
        run_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
        turn_number=1,
        model_call_id="model-call-1",
        request_id="request-1",
        route_name="coding-default",
        messages=messages
        or (
            GatewayMessage(role=MessageRole.SYSTEM, content="System one"),
            GatewayMessage(role=MessageRole.SYSTEM, content="System two"),
            GatewayMessage(role=MessageRole.USER, content="Inspect the repository"),
        ),
        tools=tools,
    )


def response(
    *,
    status: str | None = "completed",
    incomplete_reason: str | None = None,
    usage: ResponseUsage | None = None,
    model: str = "coding-default",
) -> Response:
    return Response(
        id="response-1",
        created_at=1.0,
        incomplete_details=(
            IncompleteDetails(reason=cast("Any", incomplete_reason))
            if incomplete_reason is not None
            else None
        ),
        model=model,
        object="response",
        output=[],
        parallel_tool_calls=True,
        status=cast("Any", status),
        tool_choice="auto",
        tools=[],
        usage=usage,
    )


def completed_event(
    *,
    status: str | None = "completed",
    usage: ResponseUsage | None = None,
) -> ResponseCompletedEvent:
    return ResponseCompletedEvent(
        response=response(status=status, usage=usage),
        sequence_number=10,
        type="response.completed",
    )


def text_event(delta: str = "done") -> ResponseTextDeltaEvent:
    return ResponseTextDeltaEvent(
        content_index=0,
        delta=delta,
        item_id="message-1",
        output_index=0,
        sequence_number=1,
        type="response.output_text.delta",
        logprobs=[],
    )


def tool_event(
    arguments: str,
    *,
    call_id: str = "call-1",
    name: str = "inspect_repo",
) -> ResponseOutputItemDoneEvent:
    return ResponseOutputItemDoneEvent(
        item=ResponseFunctionToolCall(
            arguments=arguments,
            call_id=call_id,
            name=name,
            type="function_call",
        ),
        output_index=0,
        sequence_number=2,
        type="response.output_item.done",
    )


async def collect_gateway_events(
    sdk_events: list[TResponseStreamEvent],
    *,
    request: GatewayRequest | None = None,
) -> tuple[list[GatewayEvent], FakeSdkModel, FakeSdkProvider]:
    model = FakeSdkModel(sdk_events)
    provider = FakeSdkProvider(model)
    gateway = OpenAIAgentsGateway(cast("ModelProvider", provider))
    events = [event async for event in gateway.stream(request or gateway_request())]
    return events, model, provider


def test_request_conversion_preserves_messages_and_canonical_tool_calls() -> None:
    request = gateway_request(
        messages=(
            GatewayMessage(role=MessageRole.SYSTEM, content="One"),
            GatewayMessage(role=MessageRole.SYSTEM, content="Two"),
            GatewayMessage(role=MessageRole.USER, content="Start"),
            GatewayMessage(
                role=MessageRole.ASSISTANT,
                content="Checking",
                tool_calls=(
                    GatewayToolCall(
                        id="call-1",
                        name="inspect_repo",
                        arguments={"z": "雪", "a": [2, 1]},
                    ),
                ),
            ),
            GatewayMessage(
                role=MessageRole.TOOL,
                tool_call_id="call-1",
                content='{"ok":true}',
            ),
        )
    )

    instructions, input_items = convert_request_input(request)

    assert instructions == "One\n\nTwo"
    assert input_items == [
        {"role": "user", "content": "Start", "type": "message"},
        {
            "id": "message_3",
            "content": [
                {
                    "annotations": [],
                    "text": "Checking",
                    "type": "output_text",
                }
            ],
            "role": "assistant",
            "status": "completed",
            "type": "message",
        },
        {
            "arguments": '{"a":[2,1],"z":"雪"}',
            "call_id": "call-1",
            "name": "inspect_repo",
            "type": "function_call",
        },
        {
            "call_id": "call-1",
            "output": '{"ok":true}',
            "type": "function_call_output",
        },
    ]


def test_request_conversion_rejects_late_system_message() -> None:
    request = gateway_request(
        messages=(
            GatewayMessage(role=MessageRole.USER, content="Start"),
            GatewayMessage(role=MessageRole.SYSTEM, content="Too late"),
        )
    )

    with pytest.raises(DomainOperationError, match="system messages") as caught:
        convert_request_input(request)

    assert caught.value.code == "invalid_gateway_request"
    assert "Too late" not in caught.value.as_dict().__repr__()


def test_tool_conversion_is_schema_only_and_preserves_core_contract() -> None:
    definition = GatewayToolDefinition(
        name="inspect_repo",
        description="Inspect repository metadata",
        input_schema={
            "type": "object",
            "properties": {"depth": {"type": "integer"}},
            "additionalProperties": False,
        },
    )

    tools = convert_tool_definitions((definition,))

    assert len(tools) == 1
    tool = tools[0]
    assert isinstance(tool, FunctionTool)
    assert tool.name == "inspect_repo"
    assert tool.strict_json_schema is False
    assert tool.params_json_schema == definition.input_schema.to_json_object()


@pytest.mark.asyncio
async def test_sdk_tool_callback_is_unreachable() -> None:
    definition = GatewayToolDefinition(
        name="inspect_repo",
        description="Inspect repository metadata",
        input_schema={"type": "object", "additionalProperties": False},
    )
    tool = convert_tool_definitions((definition,))[0]
    assert isinstance(tool, FunctionTool)

    with pytest.raises(RuntimeError, match="disabled"):
        await tool.on_invoke_tool(cast("Any", None), '{"ignored":true}')


@pytest.mark.asyncio
async def test_gateway_normalizes_text_tool_calls_usage_and_route() -> None:
    usage = ResponseUsage(
        input_tokens=7,
        input_tokens_details=InputTokensDetails(cached_tokens=3, cache_write_tokens=0),
        output_tokens=5,
        output_tokens_details=OutputTokensDetails(reasoning_tokens=1),
        total_tokens=12,
    )
    events, model, provider = await collect_gateway_events(
        [
            text_event("checking"),
            tool_event('{"path":"src/雪.py"}'),
            completed_event(usage=usage),
        ],
    )

    assert isinstance(events[0], GatewayTextDelta)
    assert events[0].delta == "checking"
    assert isinstance(events[1], GatewayToolCallEvent)
    assert events[1].tool_call.arguments.to_json_object() == {"path": "src/雪.py"}
    assert events[2] == GatewayResponseCompleted(
        finish_reason=GatewayFinishReason.TOOL_CALLS,
        input_tokens=7,
        output_tokens=5,
        cached_tokens=3,
        provider=None,
        model="coding-default",
    )
    assert provider.model_names == ["coding-default"]
    assert model.calls[0]["tracing"] is ModelTracing.DISABLED
    assert model.calls[0]["previous_response_id"] is None
    request_settings = model.calls[0]["model_settings"]
    assert isinstance(request_settings, ModelSettings)
    assert request_settings.include_usage is True
    assert request_settings.metadata == {
        "tenant_id": "00000000-0000-0000-0000-000000000010",
        "session_id": "00000000-0000-0000-0000-000000000020",
        "run_id": "00000000-0000-0000-0000-000000000001",
        "turn_number": "1",
        "model_call_id": "model-call-1",
        "request_id": "request-1",
        "route_name": "coding-default",
    }
    assert request_settings.extra_headers == {
        "X-Agent-Model-Call-ID": "model-call-1",
        "X-Agent-Run-ID": "00000000-0000-0000-0000-000000000001",
        "X-Agent-Session-ID": "00000000-0000-0000-0000-000000000020",
        "X-Agent-Tenant-ID": "00000000-0000-0000-0000-000000000010",
        "X-Agent-Turn": "1",
        "X-Request-ID": "request-1",
    }
    assert model.closed is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "arguments",
    [
        "{broken",
        "[]",
        "NaN",
        '{"value":Infinity}',
    ],
)
async def test_gateway_sanitizes_malformed_tool_arguments(arguments: str) -> None:
    events, _, _ = await collect_gateway_events(
        [tool_event(arguments), completed_event()],
    )

    invalid = events[0]
    assert isinstance(invalid, GatewayInvalidToolCallEvent)
    assert invalid.error.code == "malformed_tool_arguments"
    assert arguments not in invalid.model_dump_json()
    assert isinstance(events[1], GatewayResponseCompleted)
    assert events[1].finish_reason is GatewayFinishReason.TOOL_CALLS


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        ("max_output_tokens", GatewayFinishReason.LENGTH),
        ("content_filter", GatewayFinishReason.CONTENT_FILTER),
    ],
)
async def test_gateway_maps_incomplete_reasons(
    reason: str,
    expected: GatewayFinishReason,
) -> None:
    incomplete = ResponseIncompleteEvent(
        response=response(status="incomplete", incomplete_reason=reason),
        sequence_number=2,
        type="response.incomplete",
    )

    events, _, _ = await collect_gateway_events([incomplete])

    assert events == [
        GatewayResponseCompleted(
            finish_reason=expected,
            model="coding-default",
        )
    ]


@pytest.mark.asyncio
async def test_gateway_maps_refusal_to_content_filter_without_forwarding_text() -> None:
    refusal = ResponseRefusalDeltaEvent(
        content_index=0,
        delta="sensitive provider refusal",
        item_id="message-1",
        output_index=0,
        sequence_number=1,
        type="response.refusal.delta",
    )

    events, _, _ = await collect_gateway_events([refusal, completed_event()])

    assert events == [
        GatewayResponseCompleted(
            finish_reason=GatewayFinishReason.CONTENT_FILTER,
            model="coding-default",
        )
    ]
    assert "sensitive provider refusal" not in repr(events)


@pytest.mark.asyncio
async def test_gateway_detects_refusal_present_only_in_completed_output() -> None:
    final_response = response()
    final_response.output = [
        ResponseOutputMessage(
            id="message-1",
            content=[
                ResponseOutputRefusal(
                    refusal="completed refusal remains private",
                    type="refusal",
                )
            ],
            role="assistant",
            status="completed",
            type="message",
        )
    ]
    event = ResponseCompletedEvent(
        response=final_response,
        sequence_number=3,
        type="response.completed",
    )

    events, _, _ = await collect_gateway_events([event])

    assert isinstance(events[0], GatewayResponseCompleted)
    assert events[0].finish_reason is GatewayFinishReason.CONTENT_FILTER
    assert "completed refusal" not in repr(events)


@pytest.mark.asyncio
async def test_gateway_rejects_unstreamed_or_incomplete_tool_calls() -> None:
    unstreamed_response = response()
    unstreamed_response.output = [
        ResponseFunctionToolCall(
            arguments="{}",
            call_id="unstreamed-call",
            name="inspect_repo",
            type="function_call",
        )
    ]
    unstreamed = ResponseCompletedEvent(
        response=unstreamed_response,
        sequence_number=3,
        type="response.completed",
    )
    incomplete = tool_event("{}")
    assert isinstance(incomplete.item, ResponseFunctionToolCall)
    incomplete.item.status = "incomplete"

    with pytest.raises(DomainOperationError, match="unstreamed") as unstreamed_error:
        await collect_gateway_events([unstreamed])
    assert unstreamed_error.value.code == "invalid_model_stream"

    with pytest.raises(DomainOperationError, match="unsupported") as incomplete_error:
        await collect_gateway_events([incomplete])
    assert incomplete_error.value.code == "unsupported_model_output"


@pytest.mark.asyncio
async def test_gateway_rejects_invalid_metadata_without_raw_arguments() -> None:
    with pytest.raises(DomainOperationError, match="metadata") as caught:
        await collect_gateway_events(
            [tool_event('{"secret":"do-not-copy"}', name="Invalid-Name")],
        )

    assert caught.value.code == "invalid_tool_call_metadata"
    assert "do-not-copy" not in caught.value.as_dict().__repr__()


@pytest.mark.asyncio
async def test_gateway_ignores_supported_message_items_and_sanitizes_sdk_errors() -> None:
    message_item = ResponseOutputItemDoneEvent(
        item=ResponseOutputMessage(
            id="message-1",
            content=[],
            role="assistant",
            status="completed",
            type="message",
        ),
        output_index=0,
        sequence_number=1,
        type="response.output_item.done",
    )
    error_event = ResponseErrorEvent(
        message="provider secret should remain opaque",
        sequence_number=2,
        type="error",
    )

    events, _, _ = await collect_gateway_events([message_item, text_event(), completed_event()])
    assert isinstance(events[0], GatewayTextDelta)

    with pytest.raises(DomainOperationError, match="failed model response") as caught:
        await collect_gateway_events([error_event])
    assert caught.value.code == "model_gateway_failure"
    assert "provider secret" not in caught.value.as_dict().__repr__()


@pytest.mark.asyncio
async def test_gateway_rejects_unsupported_hosted_tool_items() -> None:
    hosted_tool = ResponseOutputItemDoneEvent(
        item=ResponseFileSearchToolCall(
            id="hosted-1",
            queries=["secret query"],
            status="completed",
            type="file_search_call",
        ),
        output_index=0,
        sequence_number=1,
        type="response.output_item.done",
    )

    with pytest.raises(DomainOperationError, match="unsupported model output") as caught:
        await collect_gateway_events([hosted_tool])

    assert caught.value.code == "unsupported_model_output"
    assert "secret query" not in caught.value.as_dict().__repr__()


@pytest.mark.asyncio
async def test_gateway_rejects_missing_or_duplicate_terminal_events() -> None:
    with pytest.raises(DomainOperationError, match="without a terminal") as missing:
        await collect_gateway_events([text_event()])
    assert missing.value.code == "incomplete_model_stream"

    with pytest.raises(DomainOperationError, match="after a terminal") as duplicate:
        await collect_gateway_events([completed_event(), completed_event()])
    assert duplicate.value.code == "invalid_model_stream"


@pytest.mark.asyncio
async def test_gateway_sanitizes_unexpected_sdk_exception() -> None:
    model = FakeSdkModel([], failure=RuntimeError("sk-provider-secret"))
    provider = FakeSdkProvider(model)
    gateway = OpenAIAgentsGateway(cast("ModelProvider", provider))

    with pytest.raises(DomainOperationError, match="Agents SDK") as caught:
        _ = [event async for event in gateway.stream(gateway_request())]

    assert caught.value.code == "model_gateway_failure"
    assert "sk-provider-secret" not in caught.value.as_dict().__repr__()
    assert model.closed is True


@pytest.mark.asyncio
async def test_gateway_cancellation_closes_sdk_stream() -> None:
    model = FakeSdkModel([text_event(), completed_event()])
    provider = FakeSdkProvider(model)
    gateway = OpenAIAgentsGateway(cast("ModelProvider", provider))
    stream = cast(
        "AsyncGenerator[GatewayEvent, None]",
        gateway.stream(gateway_request()),
    )

    first = await anext(stream)
    assert isinstance(first, GatewayTextDelta)
    await stream.aclose()

    assert model.closed is True


@pytest.mark.asyncio
async def test_gateway_lifecycle_closes_provider_and_rejects_reuse() -> None:
    model = FakeSdkModel([completed_event()])
    provider = FakeSdkProvider(model)
    gateway = OpenAIAgentsGateway(cast("ModelProvider", provider), ModelSettings())

    async with gateway:
        events = [event async for event in gateway.stream(gateway_request())]
        assert len(events) == 1

    assert provider.closed is True
    await gateway.aclose()
    with pytest.raises(DomainOperationError, match="closed") as caught:
        _ = [event async for event in gateway.stream(gateway_request())]
    assert caught.value.code == "gateway_closed"


@pytest.mark.asyncio
async def test_gateway_closes_factory_owned_client_once() -> None:
    model = FakeSdkModel([completed_event()])
    provider = FakeSdkProvider(model)
    client = FakeOwnedClient()
    gateway = OpenAIAgentsGateway._with_owned_client(
        cast("ModelProvider", provider),
        cast("AsyncOpenAI", client),
    )

    await gateway.aclose()
    await gateway.aclose()

    assert provider.closed is True
    assert client.close_calls == 1


@pytest.mark.asyncio
async def test_gateway_cleanup_is_opaque_retryable_and_resumes_partial_cleanup() -> None:
    known_value = "provider-cleanup-secret"
    model = FakeSdkModel([completed_event()])
    provider = RetryableProvider(model, known_value)
    client = FakeOwnedClient()
    gateway = OpenAIAgentsGateway._with_owned_client(
        cast("ModelProvider", provider),
        cast("AsyncOpenAI", client),
    )

    with pytest.raises(DomainOperationError) as first:
        await gateway.aclose()
    assert first.value.code == "gateway_cleanup_failed"
    assert first.value.retryable is True
    assert known_value not in repr(first.value.as_dict())
    assert client.close_calls == 0
    with pytest.raises(DomainOperationError) as unavailable:
        _ = [event async for event in gateway.stream(gateway_request())]
    assert unavailable.value.code == "gateway_cleanup_required"

    await gateway.aclose()
    assert provider.close_calls == 2
    assert client.close_calls == 1

    client_value = "client-cleanup-secret"
    second_provider = FakeSdkProvider(model)
    retryable_client = RetryableOwnedClient(client_value)
    second_gateway = OpenAIAgentsGateway._with_owned_client(
        cast("ModelProvider", second_provider),
        cast("AsyncOpenAI", retryable_client),
    )
    with pytest.raises(DomainOperationError) as client_failure:
        await second_gateway.aclose()
    assert client_failure.value.code == "gateway_cleanup_failed"
    assert client_value not in repr(client_failure.value.as_dict())

    await second_gateway.aclose()
    assert second_provider.closed is True
    assert retryable_client.close_calls == 2


@pytest.mark.asyncio
async def test_gateway_cleanup_completes_before_propagating_cancellation() -> None:
    provider = BlockingProvider(FakeSdkModel([completed_event()]))
    gateway = OpenAIAgentsGateway(cast("ModelProvider", provider))
    closing = asyncio.create_task(gateway.aclose())

    await provider.close_started.wait()
    closing.cancel()
    provider.close_release.set()

    with pytest.raises(asyncio.CancelledError):
        await closing
    assert provider.closed is True
    with pytest.raises(DomainOperationError) as closed:
        _ = [event async for event in gateway.stream(gateway_request())]
    assert closed.value.code == "gateway_closed"


@pytest.mark.asyncio
async def test_factory_uses_versioned_route_auth_and_no_hidden_retry() -> None:
    requests: list[httpx.Request] = []

    def fail_request(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            status_code=500,
            json={"error": {"message": "provider detail must remain opaque"}},
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(fail_request))
    settings = PlatformSettings(
        gateway_url="http://gateway.test",
        gateway_api_key="sk-gateway-secret",
    )
    gateway = create_openai_compatible_agents_gateway(
        settings,
        http_client=http_client,
    )

    with pytest.raises(DomainOperationError) as caught:
        _ = [event async for event in gateway.stream(gateway_request())]

    assert caught.value.code == "model_gateway_failure"
    assert len(requests) == 1
    assert requests[0].url == httpx.URL("http://gateway.test/v1/chat/completions")
    assert requests[0].headers["authorization"] == "Bearer sk-gateway-secret"
    assert requests[0].headers["x-request-id"] == "request-1"
    assert requests[0].headers["x-agent-tenant-id"] == ("00000000-0000-0000-0000-000000000010")
    assert requests[0].headers["x-agent-session-id"] == ("00000000-0000-0000-0000-000000000020")
    assert requests[0].headers["x-agent-run-id"] == ("00000000-0000-0000-0000-000000000001")
    assert requests[0].headers["x-agent-turn"] == "1"
    request_body = requests[0].read().decode()
    assert '"model":"coding-default"' in request_body
    assert '"stream":true' in request_body
    assert '"request_id":"request-1"' in request_body
    assert "provider detail" not in caught.value.as_dict().__repr__()

    await gateway.aclose()
    assert http_client.is_closed is False
    await http_client.aclose()
