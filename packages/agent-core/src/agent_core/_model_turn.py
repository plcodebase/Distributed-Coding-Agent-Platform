"""Normalized, bounded consumption of one model gateway stream."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import ValidationError

from agent_core._loop_safety import contains_secret, redact_error
from agent_core._loop_support import json_size, loop_error
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.domain.models import canonical_argument_hash
from agent_core.events import (
    MAX_EVENT_PAYLOAD_BYTES,
    AnyAgentEvent,
    ModelTextDeltaPayload,
)
from agent_core.gateway import (
    GatewayInvalidToolCallEvent,
    GatewayRequest,
    GatewayResponseCompleted,
    GatewayTextDelta,
    GatewayToolCall,
    GatewayToolCallEvent,
    ModelGateway,
    parse_gateway_event,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from agent_core._loop_events import LoopEventFactory
    from agent_core._loop_types import AgentLoopConfig
    from platform_telemetry import Redactor


@dataclass(frozen=True, slots=True)
class InvalidToolCall:
    """Safe metadata for one rejected provider tool call."""

    tool_call_id: str
    tool_name: str
    error: ErrorDetail


@dataclass(frozen=True, slots=True)
class ModelTurnResult:
    """A completely normalized model turn."""

    text: str
    tool_calls: tuple[GatewayToolCall, ...]
    invalid_tool_calls: tuple[InvalidToolCall, ...]
    completion: GatewayResponseCompleted

    @property
    def has_tool_calls(self) -> bool:
        return bool(self.tool_calls or self.invalid_tool_calls)


@dataclass(frozen=True, slots=True)
class ModelTurnFailure:
    """A terminal stream failure translated to a safe domain error."""

    error: ErrorDetail


type ModelStreamItem = AnyAgentEvent | ModelTurnResult | ModelTurnFailure


def _model_text_chunks(value: str, *, model_call_id: str) -> Iterator[str]:
    """Split sanitized text into the largest event-safe Unicode chunks."""

    cursor = 0
    while cursor < len(value):
        remaining = value[cursor:]
        if _model_text_payload_fits(remaining, model_call_id=model_call_id):
            yield remaining
            return
        lower = 1
        upper = len(remaining) - 1
        best = 0
        while lower <= upper:
            middle = (lower + upper) // 2
            if _model_text_payload_fits(
                remaining[:middle],
                model_call_id=model_call_id,
            ):
                best = middle
                lower = middle + 1
            else:
                upper = middle - 1
        if best == 0:
            raise AssertionError("one Unicode character must fit in a model delta event")
        yield remaining[:best]
        cursor += best


def _model_text_payload_fits(value: str, *, model_call_id: str) -> bool:
    payload = ModelTextDeltaPayload(model_call_id=model_call_id, delta=value)
    return len(payload.model_dump_json().encode("utf-8")) <= MAX_EVENT_PAYLOAD_BYTES


class ModelTurnRunner:
    """Consume one gateway stream while enforcing model-side safety bounds."""

    def __init__(
        self,
        *,
        gateway: ModelGateway,
        config: AgentLoopConfig,
        redactor: Redactor,
    ) -> None:
        self._gateway = gateway
        self._config = config
        self._redactor = redactor

    async def stream(  # noqa: PLR0911, PLR0912, PLR0915 - explicit fail-closed paths
        self,
        request: GatewayRequest,
        *,
        model_call_id: str,
        turn_number: int,
        events: LoopEventFactory,
        prior_tool_call_count: int,
    ) -> AsyncIterator[ModelStreamItem]:
        text_parts: list[str] = []
        raw_model_output_bytes = 0
        safe_model_output_bytes = 0
        text_redactor = self._redactor.stream()
        tool_calls: list[GatewayToolCall] = []
        invalid_tool_calls: list[InvalidToolCall] = []
        completion: GatewayResponseCompleted | None = None

        try:
            async with asyncio.timeout(self._config.model_timeout_seconds):
                async for untrusted_event in self._gateway.stream(request):
                    try:
                        gateway_event = parse_gateway_event(untrusted_event)
                    except ValidationError:
                        yield ModelTurnFailure(
                            loop_error(
                                "invalid_model_stream",
                                "gateway emitted an invalid normalized event",
                                details={"turn": turn_number},
                            )
                        )
                        return

                    if completion is not None:
                        yield ModelTurnFailure(
                            loop_error(
                                "invalid_model_stream",
                                "gateway emitted data after the terminal event",
                                details={"turn": turn_number},
                            )
                        )
                        return

                    if isinstance(gateway_event, GatewayTextDelta):
                        raw_model_output_bytes += len(gateway_event.delta.encode("utf-8"))
                        if raw_model_output_bytes > self._config.max_model_output_bytes:
                            yield ModelTurnFailure(
                                loop_error(
                                    "model_output_limit",
                                    "model output exceeded the configured byte limit",
                                    details={
                                        "limit_bytes": self._config.max_model_output_bytes,
                                        "turn": turn_number,
                                    },
                                )
                            )
                            return
                        safe_text = text_redactor.feed(gateway_event.delta)
                        safe_model_output_bytes += len(safe_text.encode("utf-8"))
                        if safe_model_output_bytes > self._config.max_model_output_bytes:
                            yield ModelTurnFailure(
                                loop_error(
                                    "model_output_limit",
                                    "redacted model output exceeded the configured byte limit",
                                    details={
                                        "limit_bytes": self._config.max_model_output_bytes,
                                        "turn": turn_number,
                                    },
                                )
                            )
                            return
                        if safe_text:
                            text_parts.append(safe_text)
                            for chunk in _model_text_chunks(
                                safe_text,
                                model_call_id=model_call_id,
                            ):
                                yield events.model_text(
                                    model_call_id=model_call_id,
                                    delta=chunk,
                                )
                        continue

                    if isinstance(gateway_event, GatewayToolCallEvent):
                        call = gateway_event.tool_call
                        argument_size = json_size(call.arguments)
                        if argument_size > self._config.max_tool_argument_bytes:
                            error = loop_error(
                                "tool_argument_limit",
                                "model-generated tool arguments exceeded the byte limit",
                                details={
                                    "tool_call_id": call.id,
                                    "tool_name": call.name,
                                    "limit_bytes": self._config.max_tool_argument_bytes,
                                },
                            )
                            yield events.tool_rejected(
                                model_call_id=model_call_id,
                                tool_call_id=call.id,
                                tool_name=call.name,
                                error=error,
                            )
                            yield ModelTurnFailure(error)
                            return
                        if contains_secret(call.arguments, self._redactor):
                            error = loop_error(
                                "sensitive_tool_arguments",
                                "model-generated tool arguments contained sensitive data",
                                details={
                                    "tool_call_id": call.id,
                                    "tool_name": call.name,
                                },
                            )
                            invalid_tool_calls.append(
                                InvalidToolCall(
                                    tool_call_id=call.id,
                                    tool_name=call.name,
                                    error=error,
                                )
                            )
                            yield events.tool_rejected(
                                model_call_id=model_call_id,
                                tool_call_id=call.id,
                                tool_name=call.name,
                                error=error,
                            )
                        else:
                            tool_calls.append(call)
                            yield events.tool_received(
                                model_call_id=model_call_id,
                                tool_call=call,
                                argument_hash=canonical_argument_hash(call.arguments),
                            )
                    elif isinstance(gateway_event, GatewayInvalidToolCallEvent):
                        safe_error = redact_error(gateway_event.error, self._redactor)
                        invalid_tool_calls.append(
                            InvalidToolCall(
                                tool_call_id=gateway_event.tool_call_id,
                                tool_name=gateway_event.tool_name,
                                error=safe_error,
                            )
                        )
                        yield events.tool_rejected(
                            model_call_id=model_call_id,
                            tool_call_id=gateway_event.tool_call_id,
                            tool_name=gateway_event.tool_name,
                            error=safe_error,
                        )
                    elif isinstance(gateway_event, GatewayResponseCompleted):
                        completion = gateway_event
                        continue

                    current_calls = len(tool_calls) + len(invalid_tool_calls)
                    if prior_tool_call_count + current_calls > self._config.max_tool_calls:
                        yield ModelTurnFailure(
                            loop_error(
                                "tool_call_limit",
                                "agent reached the maximum number of tool calls",
                                details={"limit": self._config.max_tool_calls},
                            )
                        )
                        return
        except TimeoutError:
            yield ModelTurnFailure(
                loop_error(
                    "model_timeout",
                    "model gateway stream exceeded its timeout",
                    retryable=True,
                    details={"turn": turn_number},
                )
            )
            return
        except DomainOperationError as error:
            yield ModelTurnFailure(redact_error(error.error, self._redactor))
            return
        except Exception:
            yield ModelTurnFailure(
                loop_error(
                    "model_gateway_failure",
                    "model gateway stream failed",
                    retryable=True,
                    details={"turn": turn_number},
                )
            )
            return

        if completion is None:
            yield ModelTurnFailure(
                loop_error(
                    "incomplete_model_stream",
                    "model gateway stream ended without a terminal event",
                    retryable=True,
                    details={"turn": turn_number},
                )
            )
            return
        final_safe_text = text_redactor.finish()
        safe_model_output_bytes += len(final_safe_text.encode("utf-8"))
        if safe_model_output_bytes > self._config.max_model_output_bytes:
            yield ModelTurnFailure(
                loop_error(
                    "model_output_limit",
                    "redacted model output exceeded the configured byte limit",
                    details={
                        "limit_bytes": self._config.max_model_output_bytes,
                        "turn": turn_number,
                    },
                )
            )
            return
        if final_safe_text:
            text_parts.append(final_safe_text)
            for chunk in _model_text_chunks(
                final_safe_text,
                model_call_id=model_call_id,
            ):
                yield events.model_text(
                    model_call_id=model_call_id,
                    delta=chunk,
                )
        yield ModelTurnResult(
            text="".join(text_parts),
            tool_calls=tuple(tool_calls),
            invalid_tool_calls=tuple(invalid_tool_calls),
            completion=completion,
        )


__all__ = [
    "InvalidToolCall",
    "ModelStreamItem",
    "ModelTurnFailure",
    "ModelTurnResult",
    "ModelTurnRunner",
]
