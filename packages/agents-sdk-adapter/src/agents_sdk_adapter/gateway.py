"""OpenAI Agents SDK implementation of the provider-neutral model gateway."""

from __future__ import annotations

import asyncio
import json
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Self

from agents import ModelSettings
from agents.models.interface import ModelTracing
from openai.types.responses import (
    ResponseCompletedEvent,
    ResponseErrorEvent,
    ResponseFailedEvent,
    ResponseFunctionToolCall,
    ResponseIncompleteEvent,
    ResponseOutputItemDoneEvent,
    ResponseOutputMessage,
    ResponseOutputRefusal,
    ResponseReasoningItem,
    ResponseRefusalDeltaEvent,
    ResponseTextDeltaEvent,
)

from agent_core.domain.base import FrozenJsonObject
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.gateway import (
    GatewayEvent,
    GatewayFinishReason,
    GatewayInvalidToolCallEvent,
    GatewayRequest,
    GatewayResponseCompleted,
    GatewayTextDelta,
    GatewayToolCall,
    GatewayToolCallEvent,
)
from agents_sdk_adapter._conversion import convert_request_input, convert_tool_definitions

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable
    from types import TracebackType

    from agents.items import TResponseStreamEvent
    from agents.models.interface import ModelProvider
    from openai import AsyncOpenAI
    from openai.types.responses import Response


_MAX_IDENTIFIER_LENGTH = 255
_IGNORED_EVENT_TYPES = frozenset(
    {
        "response.content_part.added",
        "response.content_part.done",
        "response.created",
        "response.function_call_arguments.delta",
        "response.function_call_arguments.done",
        "response.in_progress",
        "response.output_item.added",
        "response.output_text.done",
        "response.queued",
        "response.reasoning_summary_part.added",
        "response.reasoning_summary_part.done",
        "response.reasoning_summary_text.delta",
        "response.reasoning_summary_text.done",
        "response.reasoning_text.delta",
        "response.reasoning_text.done",
        "response.refusal.done",
    }
)


class OpenAIAgentsGateway(AbstractAsyncContextManager["OpenAIAgentsGateway"]):
    """Normalize one Agents SDK model stream into core gateway events."""

    def __init__(
        self,
        provider: ModelProvider,
        model_settings: ModelSettings | None = None,
        tracing: ModelTracing = ModelTracing.DISABLED,
    ) -> None:
        self._provider = provider
        self._model_settings = model_settings or ModelSettings()
        self._tracing = tracing
        self._owned_client: AsyncOpenAI | None = None
        self._provider_closed = False
        self._owned_client_closed = False
        self._closing = False
        self._cleanup_required = False
        self._closed = False
        self._close_lock = asyncio.Lock()

    @classmethod
    def _with_owned_client(
        cls,
        provider: ModelProvider,
        client: AsyncOpenAI,
    ) -> Self:
        gateway = cls(provider)
        gateway._owned_client = client
        return gateway

    async def __aenter__(self) -> Self:
        self._require_available()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        await self.aclose()

    async def aclose(self) -> None:
        """Release provider resources exactly once with cancellation-safe retries."""

        task = asyncio.create_task(self._aclose())
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def _aclose(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            self._closing = True
            try:
                if not self._provider_closed:
                    await self._provider.aclose()
                    self._provider_closed = True
                if self._owned_client is not None and not self._owned_client_closed:
                    await self._owned_client.close()
                    self._owned_client_closed = True
            except Exception as error:
                self._closing = False
                self._cleanup_required = True
                raise DomainOperationError(
                    code="gateway_cleanup_failed",
                    message="the model gateway could not be closed",
                    retryable=True,
                ) from error
            self._closing = False
            self._cleanup_required = False
            self._closed = True

    async def stream(self, request: GatewayRequest) -> AsyncIterator[GatewayEvent]:
        """Stream one request through an SDK model selected by route name."""

        self._require_available(request=request)

        try:
            model = self._provider.get_model(request.route_name)
            system_instructions, input_items = convert_request_input(request)
            sdk_tools = convert_tool_definitions(request.tools)
            sdk_stream = model.stream_response(
                system_instructions=system_instructions,
                input=input_items,
                model_settings=_request_model_settings(self._model_settings, request),
                tools=sdk_tools,
                output_schema=None,
                handoffs=[],
                tracing=self._tracing,
                previous_response_id=None,
                conversation_id=None,
                prompt=None,
            )
            normalized_stream = self._normalize_stream(
                sdk_stream,
                request=request,
            )
            try:
                async for event in normalized_stream:
                    yield event
            finally:
                await _close_stream(normalized_stream)
        except DomainOperationError:
            raise
        except Exception as error:
            raise DomainOperationError(
                code="model_gateway_failure",
                message="the Agents SDK model stream failed",
                retryable=True,
                details={
                    "model_call_id": request.model_call_id,
                    "request_id": request.request_id,
                },
            ) from error

    def _require_available(self, *, request: GatewayRequest | None = None) -> None:
        details = (
            FrozenJsonObject({"request_id": request.request_id}) if request is not None else None
        )
        if self._closed:
            raise DomainOperationError(
                code="gateway_closed",
                message="the model gateway is closed",
                details=details,
            )
        if self._closing or self._cleanup_required:
            raise DomainOperationError(
                code="gateway_cleanup_required",
                message="the model gateway requires cleanup before reuse",
                retryable=True,
                details=details,
            )

    async def _normalize_stream(  # noqa: PLR0912 - explicit SDK event allowlist
        self,
        stream: AsyncIterator[TResponseStreamEvent],
        *,
        request: GatewayRequest,
    ) -> AsyncIterator[GatewayEvent]:
        terminal = False
        saw_tool_call = False
        saw_refusal = False
        seen_tool_call_ids: set[str] = set()

        try:
            async for event in stream:
                if terminal:
                    raise DomainOperationError(
                        code="invalid_model_stream",
                        message="the SDK emitted data after a terminal event",
                        details={"request_id": request.request_id},
                    )

                if isinstance(event, ResponseTextDeltaEvent):
                    if event.delta:
                        yield GatewayTextDelta(delta=event.delta)
                    continue

                if isinstance(event, ResponseRefusalDeltaEvent):
                    saw_refusal = True
                    continue

                if isinstance(event, ResponseOutputItemDoneEvent):
                    if isinstance(event.item, ResponseFunctionToolCall):
                        if event.item.status == "incomplete":
                            raise _unsupported_output_error(request)
                        saw_tool_call = True
                        seen_tool_call_ids.add(event.item.call_id)
                        yield _normalize_tool_call(event.item)
                    elif not isinstance(
                        event.item,
                        (ResponseOutputMessage, ResponseReasoningItem),
                    ):
                        raise _unsupported_output_error(request)
                    continue

                if isinstance(event, ResponseIncompleteEvent):
                    terminal = True
                    yield _normalize_incomplete(event.response, request=request)
                    continue

                if isinstance(event, (ResponseFailedEvent, ResponseErrorEvent)):
                    raise DomainOperationError(
                        code="model_gateway_failure",
                        message="the SDK reported a failed model response",
                        retryable=True,
                        details={"request_id": request.request_id},
                    )

                if isinstance(event, ResponseCompletedEvent):
                    terminal = True
                    saw_refusal = (
                        _validate_completed_output(
                            event.response,
                            request=request,
                            seen_tool_call_ids=seen_tool_call_ids,
                        )
                        or saw_refusal
                    )
                    yield _normalize_completion(
                        event.response,
                        request=request,
                        saw_tool_call=saw_tool_call,
                        saw_refusal=saw_refusal,
                    )
                    continue

                if event.type not in _IGNORED_EVENT_TYPES:
                    raise _unsupported_output_error(request)
        finally:
            await _close_stream(stream)

        if not terminal:
            raise DomainOperationError(
                code="incomplete_model_stream",
                message="the SDK model stream ended without a terminal event",
                retryable=True,
                details={"request_id": request.request_id},
            )


def _request_model_settings(
    configured: ModelSettings,
    request: GatewayRequest,
) -> ModelSettings:
    """Attach non-secret, immutable request attribution to the upstream call."""

    attribution = {
        "tenant_id": str(request.tenant_id),
        "session_id": str(request.session_id),
        "run_id": str(request.run_id),
        "turn_number": str(request.turn_number),
        "model_call_id": request.model_call_id,
        "request_id": request.request_id,
        "route_name": request.route_name,
    }
    headers = {
        "X-Agent-Model-Call-ID": request.model_call_id,
        "X-Agent-Run-ID": str(request.run_id),
        "X-Agent-Session-ID": str(request.session_id),
        "X-Agent-Tenant-ID": str(request.tenant_id),
        "X-Agent-Turn": str(request.turn_number),
        "X-Request-ID": request.request_id,
    }
    return replace(
        configured,
        metadata={**(configured.metadata or {}), **attribution},
        extra_headers={**(configured.extra_headers or {}), **headers},
        include_usage=True,
    )


def _normalize_tool_call(
    item: ResponseFunctionToolCall,
) -> GatewayToolCallEvent | GatewayInvalidToolCallEvent:
    try:
        arguments = _parse_tool_arguments(item.arguments)
    except (TypeError, ValueError):
        return GatewayInvalidToolCallEvent(
            tool_call_id=item.call_id,
            tool_name=item.name,
            error=ErrorDetail(
                code="malformed_tool_arguments",
                message="provider tool arguments were not a valid JSON object",
                details={
                    "tool_call_id": item.call_id,
                    "tool_name": item.name,
                },
            ),
        )

    try:
        tool_call = GatewayToolCall(
            id=item.call_id,
            name=item.name,
            arguments=arguments,
        )
    except ValueError as error:
        raise DomainOperationError(
            code="invalid_tool_call_metadata",
            message="provider tool-call metadata was invalid",
        ) from error
    return GatewayToolCallEvent(tool_call=tool_call)


def _reject_json_constant(value: str) -> Any:
    del value
    raise ValueError("tool arguments contained a non-finite JSON number")


def _parse_tool_arguments(arguments_json: str) -> FrozenJsonObject:
    arguments_value = json.loads(
        arguments_json,
        parse_constant=_reject_json_constant,
    )
    if not isinstance(arguments_value, dict):
        raise TypeError("tool arguments must be a JSON object")
    return FrozenJsonObject(arguments_value)


def _normalize_completion(
    response: Response,
    *,
    request: GatewayRequest,
    saw_tool_call: bool,
    saw_refusal: bool,
) -> GatewayResponseCompleted:
    if response.error is not None or response.status in {"failed", "cancelled"}:
        raise DomainOperationError(
            code="model_gateway_failure",
            message="the SDK reported a failed model response",
            retryable=True,
            details={"request_id": request.request_id},
        )
    if response.status == "incomplete":
        return _normalize_incomplete(response, request=request)
    if saw_tool_call and saw_refusal:
        raise _unsupported_output_error(request)

    if saw_tool_call:
        finish_reason = GatewayFinishReason.TOOL_CALLS
    elif saw_refusal:
        finish_reason = GatewayFinishReason.CONTENT_FILTER
    else:
        finish_reason = GatewayFinishReason.STOP
    return _completion_event(
        response,
        request=request,
        finish_reason=finish_reason,
    )


def _validate_completed_output(
    response: Response,
    *,
    request: GatewayRequest,
    seen_tool_call_ids: set[str],
) -> bool:
    saw_refusal = False
    for item in response.output:
        if isinstance(item, ResponseFunctionToolCall):
            if item.status == "incomplete" or item.call_id not in seen_tool_call_ids:
                raise DomainOperationError(
                    code="invalid_model_stream",
                    message="the SDK completion contained an unstreamed tool call",
                    details={"request_id": request.request_id},
                )
        elif isinstance(item, ResponseOutputMessage):
            saw_refusal = saw_refusal or any(
                isinstance(content, ResponseOutputRefusal) for content in item.content
            )
        elif not isinstance(item, ResponseReasoningItem):
            raise _unsupported_output_error(request)
    return saw_refusal


def _normalize_incomplete(
    response: Response,
    *,
    request: GatewayRequest,
) -> GatewayResponseCompleted:
    reason = response.incomplete_details.reason if response.incomplete_details else None
    if reason == "max_output_tokens":
        finish_reason = GatewayFinishReason.LENGTH
    elif reason == "content_filter":
        finish_reason = GatewayFinishReason.CONTENT_FILTER
    else:
        raise DomainOperationError(
            code="unsupported_model_completion",
            message="the SDK returned an unsupported incomplete response",
            details={"request_id": request.request_id},
        )
    return _completion_event(
        response,
        request=request,
        finish_reason=finish_reason,
    )


def _completion_event(
    response: Response,
    *,
    request: GatewayRequest,
    finish_reason: GatewayFinishReason,
) -> GatewayResponseCompleted:
    usage = response.usage
    cached_tokens = (
        usage.input_tokens_details.cached_tokens
        if usage is not None and usage.input_tokens_details is not None
        else 0
    )
    response_model = response.model
    model = (
        response_model
        if isinstance(response_model, str) and 0 < len(response_model) <= _MAX_IDENTIFIER_LENGTH
        else request.route_name
    )
    return GatewayResponseCompleted(
        finish_reason=finish_reason,
        input_tokens=usage.input_tokens if usage is not None else 0,
        output_tokens=usage.output_tokens if usage is not None else 0,
        cached_tokens=cached_tokens,
        provider=None,
        model=model,
    )


def _unsupported_output_error(request: GatewayRequest) -> DomainOperationError:
    return DomainOperationError(
        code="unsupported_model_output",
        message="the SDK emitted an unsupported model output",
        details={"request_id": request.request_id},
    )


async def _close_stream(stream: AsyncIterator[Any]) -> None:
    close = getattr(stream, "aclose", None)
    if close is not None:
        close_call: Callable[[], Awaitable[None]] = close
        await close_call()


__all__ = ["OpenAIAgentsGateway"]
