from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, cast
from uuid import UUID

import pytest
from pydantic import ValidationError

from agent_core.domain import FrozenJsonObject
from agent_core.domain.errors import DomainOperationError
from agent_core.gateway import (
    MAX_GATEWAY_MESSAGES,
    MAX_GATEWAY_TOOLS,
    GatewayEvent,
    GatewayFinishReason,
    GatewayMessage,
    GatewayRequest,
    GatewayResponseCompleted,
    GatewayTextDelta,
    GatewayToolDefinition,
    MessageRole,
)
from gateway_client import GatewayClient, GatewayClientConfig

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator


class ScriptedGateway:
    def __init__(
        self,
        events: list[object],
        *,
        failure: Exception | None = None,
    ) -> None:
        self.events = events
        self.failure = failure
        self.requests: list[GatewayRequest] = []
        self.stream_closed = False
        self.close_calls = 0

    async def stream(self, request: GatewayRequest) -> AsyncIterator[GatewayEvent]:
        self.requests.append(request)
        try:
            for event in self.events:
                yield cast("GatewayEvent", event)
            if self.failure is not None:
                raise self.failure
        finally:
            self.stream_closed = True

    async def aclose(self) -> None:
        self.close_calls += 1


class RetryableClose:
    def __init__(self, known_value: str) -> None:
        self.known_value = known_value
        self.calls = 0

    async def __call__(self) -> None:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError(self.known_value)


class BlockingClose:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def __call__(self) -> None:
        self.calls += 1
        self.started.set()
        await self.release.wait()


def request(
    *,
    route_name: str = "coding-default",
    content: str = "hello",
) -> GatewayRequest:
    return GatewayRequest(
        tenant_id=UUID("00000000-0000-0000-0000-000000000010"),
        session_id=UUID("00000000-0000-0000-0000-000000000020"),
        run_id=UUID("00000000-0000-0000-0000-000000000030"),
        turn_number=2,
        model_call_id="model-call-2",
        request_id="request-2",
        route_name=route_name,
        messages=(GatewayMessage(role=MessageRole.USER, content=content),),
    )


@pytest.mark.asyncio
async def test_client_returns_only_valid_normalized_events_and_closes_once() -> None:
    delegate = ScriptedGateway(
        [
            GatewayTextDelta(delta="hel"),
            GatewayTextDelta(delta="lo"),
            GatewayResponseCompleted(
                finish_reason=GatewayFinishReason.STOP,
                input_tokens=1,
                output_tokens=2,
                model="coding-default",
            ),
        ]
    )
    client = GatewayClient(delegate, close=delegate.aclose)

    async with client:
        events = [event async for event in client.stream(request())]

    assert events == delegate.events
    assert delegate.requests == [request()]
    assert delegate.stream_closed is True
    assert delegate.close_calls == 1

    await client.aclose()
    assert delegate.close_calls == 1
    with pytest.raises(DomainOperationError) as closed:
        _ = [event async for event in client.stream(request())]
    assert closed.value.code == "gateway_closed"


@pytest.mark.asyncio
async def test_client_rejects_unknown_route_before_gateway_call() -> None:
    delegate = ScriptedGateway([])
    client = GatewayClient(delegate)

    with pytest.raises(DomainOperationError) as caught:
        _ = [event async for event in client.stream(request(route_name="unconfigured"))]

    assert caught.value.code == "gateway_route_not_allowed"
    assert delegate.requests == []


@pytest.mark.asyncio
async def test_client_bounds_multibyte_requests_before_delegate_invocation() -> None:
    bounded_request = request(content="🙂")
    request_bytes = len(bounded_request.model_dump_json().encode("utf-8"))
    accepted_delegate = ScriptedGateway(
        [GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP)]
    )
    accepted = GatewayClient(
        accepted_delegate,
        config=GatewayClientConfig(max_request_bytes=request_bytes),
    )

    events = [event async for event in accepted.stream(bounded_request)]

    assert len(events) == 1
    assert accepted_delegate.requests == [bounded_request]

    rejected_delegate = ScriptedGateway([])
    rejected = GatewayClient(
        rejected_delegate,
        config=GatewayClientConfig(max_request_bytes=request_bytes - 1),
    )
    with pytest.raises(DomainOperationError) as caught:
        _ = [event async for event in rejected.stream(bounded_request)]
    assert caught.value.code == "gateway_request_limit"
    assert caught.value.details["request_bytes"] == request_bytes
    assert rejected_delegate.requests == []


@pytest.mark.asyncio
async def test_client_rejects_invalid_events_and_post_terminal_data() -> None:
    invalid_delegate = ScriptedGateway([{"kind": "text_delta", "delta": ""}])
    invalid_client = GatewayClient(invalid_delegate)

    with pytest.raises(DomainOperationError) as invalid:
        _ = [event async for event in invalid_client.stream(request())]
    assert invalid.value.code == "invalid_gateway_stream"
    assert invalid_delegate.stream_closed is True

    post_terminal_delegate = ScriptedGateway(
        [
            GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP),
            GatewayTextDelta(delta="late"),
        ]
    )
    post_terminal_client = GatewayClient(post_terminal_delegate)
    with pytest.raises(DomainOperationError) as post_terminal:
        _ = [event async for event in post_terminal_client.stream(request())]
    assert post_terminal.value.code == "invalid_gateway_stream"


@pytest.mark.asyncio
async def test_client_requires_terminal_event_and_sanitizes_unexpected_errors() -> None:
    incomplete_client = GatewayClient(ScriptedGateway([GatewayTextDelta(delta="partial")]))
    with pytest.raises(DomainOperationError) as incomplete:
        _ = [event async for event in incomplete_client.stream(request())]
    assert incomplete.value.code == "incomplete_gateway_stream"
    assert incomplete.value.retryable is True

    known_value = "provider-secret-must-not-escape"
    failing_client = GatewayClient(ScriptedGateway([], failure=RuntimeError(known_value)))
    with pytest.raises(DomainOperationError) as failure:
        _ = [event async for event in failing_client.stream(request())]
    assert failure.value.code == "model_gateway_failure"
    assert failure.value.retryable is True
    assert known_value not in repr(failure.value.as_dict())

    structured_client = GatewayClient(
        ScriptedGateway(
            [],
            failure=DomainOperationError(
                code="provider_failure",
                message=known_value,
                details={"provider_body": known_value},
            ),
        )
    )
    with pytest.raises(DomainOperationError) as structured:
        _ = [event async for event in structured_client.stream(request())]
    assert structured.value.code == "model_gateway_failure"
    assert known_value not in repr(structured.value.as_dict())


@pytest.mark.asyncio
async def test_client_enforces_event_and_multibyte_byte_limits() -> None:
    event_limited = GatewayClient(
        ScriptedGateway(
            [
                GatewayTextDelta(delta="one"),
                GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP),
            ]
        ),
        config=GatewayClientConfig(max_stream_events=1),
    )
    with pytest.raises(DomainOperationError) as event_error:
        _ = [event async for event in event_limited.stream(request())]
    assert event_error.value.code == "gateway_stream_limit"

    multibyte_event = GatewayTextDelta(delta="🙂")
    one_event_bytes = len(multibyte_event.model_dump_json().encode("utf-8"))
    byte_limited = GatewayClient(
        ScriptedGateway(
            [
                multibyte_event,
                GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP),
            ]
        ),
        config=GatewayClientConfig(max_stream_bytes=one_event_bytes - 1),
    )
    with pytest.raises(DomainOperationError) as byte_error:
        _ = [event async for event in byte_limited.stream(request())]
    assert byte_error.value.code == "gateway_stream_limit"


@pytest.mark.asyncio
async def test_client_cancellation_closes_delegate_stream() -> None:
    delegate = ScriptedGateway(
        [
            GatewayTextDelta(delta="first"),
            GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP),
        ]
    )
    client = GatewayClient(delegate)
    stream = client.stream(request())

    assert await anext(stream) == GatewayTextDelta(delta="first")
    await cast("AsyncGenerator[GatewayEvent, None]", stream).aclose()

    assert delegate.stream_closed is True


def test_client_configuration_is_closed_and_validated() -> None:
    with pytest.raises(ValidationError):
        GatewayClientConfig(route_names=("coding-default", "coding-default"))
    with pytest.raises(ValidationError):
        GatewayClientConfig(route_names=(" whitespace ",))
    with pytest.raises(ValidationError):
        GatewayClientConfig(max_stream_events=0)
    with pytest.raises(ValidationError):
        GatewayClientConfig(max_request_bytes=0)
    with pytest.raises(ValidationError):
        GatewayClientConfig(max_request_bytes=8 * 1024 * 1024 + 1)
    with pytest.raises(ValidationError):
        GatewayClientConfig.model_validate({"extra": True})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("model_call_id", "model\r\ncall"),
        ("request_id", "request\x00id"),
        ("request_id", "request id"),
    ],
)
def test_gateway_request_rejects_http_unsafe_attribution_ids(
    field: str,
    value: str,
) -> None:
    data = request().model_dump(mode="python")
    data[field] = value

    with pytest.raises(ValidationError):
        GatewayRequest.model_validate(data)


def test_gateway_request_collection_hard_limits() -> None:
    message = GatewayMessage(role=MessageRole.USER, content="bounded")
    data = request().model_dump(mode="python")
    data["messages"] = (message,) * (MAX_GATEWAY_MESSAGES + 1)
    with pytest.raises(ValidationError):
        GatewayRequest.model_validate(data)

    tool = GatewayToolDefinition(
        name="bounded_tool",
        description="Bounded tool",
        input_schema=FrozenJsonObject({"type": "object"}),
    )
    data = request().model_dump(mode="python")
    data["tools"] = (tool,) * (MAX_GATEWAY_TOOLS + 1)
    with pytest.raises(ValidationError):
        GatewayRequest.model_validate(data)


@pytest.mark.asyncio
async def test_client_cleanup_failure_is_opaque_and_retryable() -> None:
    known_value = "gateway-close-sensitive-value"
    close = RetryableClose(known_value)
    client = GatewayClient(ScriptedGateway([]), close=close)

    with pytest.raises(DomainOperationError) as first:
        await client.aclose()
    assert first.value.code == "gateway_cleanup_failed"
    assert first.value.retryable is True
    assert known_value not in repr(first.value.as_dict())

    with pytest.raises(DomainOperationError) as unavailable:
        _ = [event async for event in client.stream(request())]
    assert unavailable.value.code == "gateway_cleanup_required"

    await client.aclose()
    assert close.calls == 2


@pytest.mark.asyncio
async def test_client_cleanup_completes_before_propagating_cancellation() -> None:
    close = BlockingClose()
    client = GatewayClient(ScriptedGateway([]), close=close)
    closing = asyncio.create_task(client.aclose())

    await close.started.wait()
    closing.cancel()
    close.release.set()

    with pytest.raises(asyncio.CancelledError):
        await closing
    assert close.calls == 1
    with pytest.raises(DomainOperationError) as closed:
        _ = [event async for event in client.stream(request())]
    assert closed.value.code == "gateway_closed"
