"""Bounded, provider-neutral agent loop with typed event emission."""

from __future__ import annotations

import asyncio
import json
import uuid  # noqa: TC003 - Pydantic resolves this field type at runtime
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from pydantic import Field

from agent_core.domain.base import DomainModel, FrozenJsonObject, JsonObject
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.domain.models import IdentifierString, canonical_argument_hash
from agent_core.domain.status import ToolCallStatus
from agent_core.events import (
    MAX_EVENT_PAYLOAD_BYTES,
    AnyAgentEvent,
    ContextBuildStartedEvent,
    ContextBuildStartedPayload,
    ModelRequestStartedEvent,
    ModelRequestStartedPayload,
    ModelTextDeltaEvent,
    ModelTextDeltaPayload,
    ModelToolCallReceivedEvent,
    ModelToolCallReceivedPayload,
    RunCompletedEvent,
    RunCompletedPayload,
    RunFailedEvent,
    RunFailedPayload,
    RunStartedEvent,
    RunStartedPayload,
    ToolCompletedEvent,
    ToolCompletedPayload,
    ToolOutputPayload,
    ToolStartedEvent,
    ToolStartedPayload,
    ToolStderrEvent,
    ToolStdoutEvent,
)
from agent_core.gateway import (
    GatewayFinishReason,
    GatewayMessage,
    GatewayRequest,
    GatewayResponseCompleted,
    GatewayTextDelta,
    GatewayToolCall,
    GatewayToolCallEvent,
    MessageRole,
    ModelGateway,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable
    from datetime import datetime

    from agent_core.tools import PreparedToolExecution, ToolExecutionResult, ToolRegistry

MAX_LOOP_VALUE_BYTES = MAX_EVENT_PAYLOAD_BYTES // 2


class Clock(Protocol):
    """Injectable aware clock used for deterministic durable events."""

    def now(self) -> datetime:
        """Return the current timezone-aware timestamp."""


class IdGenerator(Protocol):
    """Injectable durable identifier source."""

    def new_id(self, prefix: str) -> str:
        """Return one non-empty identifier for the requested namespace."""


class AgentLoopConfig(DomainModel):
    """Hard bounds applied to every deterministic agent-loop execution."""

    max_turns: int = Field(default=20, ge=1, le=100)
    max_tool_calls: int = Field(default=50, ge=0, le=100)
    max_semantic_retries: int = Field(default=3, ge=0, le=20)
    model_timeout_seconds: float = Field(default=120, gt=0, le=3_600)
    tool_timeout_seconds: float = Field(default=120, gt=0, le=3_600)
    max_model_output_bytes: int = Field(
        default=256 * 1024,
        ge=1,
        le=MAX_LOOP_VALUE_BYTES,
    )
    max_tool_argument_bytes: int = Field(
        default=256 * 1024,
        ge=1,
        le=MAX_LOOP_VALUE_BYTES,
    )
    max_tool_result_bytes: int = Field(
        default=256 * 1024,
        ge=1,
        le=MAX_LOOP_VALUE_BYTES,
    )
    max_tool_output_bytes: int = Field(
        default=256 * 1024,
        ge=64,
        le=MAX_LOOP_VALUE_BYTES,
    )


class AgentLoopInput(DomainModel):
    """Immutable input needed to run one already-leased execution attempt."""

    run_id: uuid.UUID
    attempt: int = Field(ge=1)
    worker_id: IdentifierString
    route_name: IdentifierString
    messages: tuple[GatewayMessage, ...] = Field(min_length=1)
    checkpoint_id: uuid.UUID | None = None


@dataclass(frozen=True, slots=True)
class _ModelTurnResult:
    text: str
    tool_calls: tuple[GatewayToolCall, ...]
    completion: GatewayResponseCompleted


@dataclass(frozen=True, slots=True)
class _ModelStreamFailure:
    error: ErrorDetail


type _ModelStreamItem = AnyAgentEvent | _ModelTurnResult | _ModelStreamFailure


class _EventFactory:
    def __init__(self, *, run_id: uuid.UUID, clock: Clock) -> None:
        self._run_id = run_id
        self._clock = clock
        self._sequence = 0

    def _metadata(self) -> tuple[int, datetime]:
        self._sequence += 1
        return self._sequence, self._clock.now()

    def run_started(self, *, attempt: int, worker_id: str) -> RunStartedEvent:
        sequence, created_at = self._metadata()
        return RunStartedEvent(
            run_id=self._run_id,
            sequence=sequence,
            payload=RunStartedPayload(attempt=attempt, worker_id=worker_id),
            created_at=created_at,
        )

    def context_started(
        self,
        *,
        message_count: int,
        checkpoint_id: uuid.UUID | None,
    ) -> ContextBuildStartedEvent:
        sequence, created_at = self._metadata()
        return ContextBuildStartedEvent(
            run_id=self._run_id,
            sequence=sequence,
            payload=ContextBuildStartedPayload(
                message_count=message_count,
                checkpoint_id=checkpoint_id,
            ),
            created_at=created_at,
        )

    def model_started(
        self,
        *,
        model_call_id: str,
        request_id: str,
        route_name: str,
    ) -> ModelRequestStartedEvent:
        sequence, created_at = self._metadata()
        return ModelRequestStartedEvent(
            run_id=self._run_id,
            sequence=sequence,
            payload=ModelRequestStartedPayload(
                model_call_id=model_call_id,
                request_id=request_id,
                route_name=route_name,
            ),
            created_at=created_at,
        )

    def model_text(self, *, model_call_id: str, delta: str) -> ModelTextDeltaEvent:
        sequence, created_at = self._metadata()
        return ModelTextDeltaEvent(
            run_id=self._run_id,
            sequence=sequence,
            payload=ModelTextDeltaPayload(model_call_id=model_call_id, delta=delta),
            created_at=created_at,
        )

    def tool_received(
        self,
        *,
        model_call_id: str,
        tool_call: GatewayToolCall,
        argument_hash: str,
    ) -> ModelToolCallReceivedEvent:
        sequence, created_at = self._metadata()
        return ModelToolCallReceivedEvent(
            run_id=self._run_id,
            sequence=sequence,
            payload=ModelToolCallReceivedPayload(
                model_call_id=model_call_id,
                tool_call_id=tool_call.id,
                tool_name=tool_call.name,
                arguments=tool_call.arguments,
                argument_hash=argument_hash,
            ),
            created_at=created_at,
        )

    def tool_started(self, *, tool_call: GatewayToolCall) -> ToolStartedEvent:
        sequence, created_at = self._metadata()
        return ToolStartedEvent(
            run_id=self._run_id,
            sequence=sequence,
            payload=ToolStartedPayload(
                tool_call_id=tool_call.id,
                tool_name=tool_call.name,
            ),
            created_at=created_at,
        )

    def tool_output(
        self,
        *,
        tool_call_id: str,
        chunk: str,
        truncated: bool,
        stderr: bool,
    ) -> ToolStdoutEvent | ToolStderrEvent:
        sequence, created_at = self._metadata()
        payload = ToolOutputPayload(
            tool_call_id=tool_call_id,
            chunk=chunk,
            truncated=truncated,
        )
        if stderr:
            return ToolStderrEvent(
                run_id=self._run_id,
                sequence=sequence,
                payload=payload,
                created_at=created_at,
            )
        return ToolStdoutEvent(
            run_id=self._run_id,
            sequence=sequence,
            payload=payload,
            created_at=created_at,
        )

    def tool_completed(
        self,
        *,
        tool_call_id: str,
        status: ToolCallStatus,
        result: FrozenJsonObject | None = None,
        error: ErrorDetail | None = None,
    ) -> ToolCompletedEvent:
        sequence, created_at = self._metadata()
        return ToolCompletedEvent(
            run_id=self._run_id,
            sequence=sequence,
            payload=ToolCompletedPayload(
                tool_call_id=tool_call_id,
                status=status,
                result=result,
                error=error,
            ),
            created_at=created_at,
        )

    def run_completed(self, *, final_text: str) -> RunCompletedEvent:
        sequence, created_at = self._metadata()
        return RunCompletedEvent(
            run_id=self._run_id,
            sequence=sequence,
            payload=RunCompletedPayload(final_text=final_text),
            created_at=created_at,
        )

    def run_failed(self, *, error: ErrorDetail) -> RunFailedEvent:
        sequence, created_at = self._metadata()
        return RunFailedEvent(
            run_id=self._run_id,
            sequence=sequence,
            payload=RunFailedPayload(error=error),
            created_at=created_at,
        )


def _error(
    code: str,
    message: str,
    *,
    retryable: bool = False,
    details: JsonObject | None = None,
) -> ErrorDetail:
    return ErrorDetail(
        code=code,
        message=message,
        retryable=retryable,
        details=details or {},
    )


def _json_size(value: FrozenJsonObject) -> int:
    return len(
        json.dumps(
            value.to_json_object(),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    )


def _tool_message(
    tool_call_id: str,
    *,
    result: FrozenJsonObject | None = None,
    error: ErrorDetail | None = None,
) -> GatewayMessage:
    body: JsonObject
    if error is not None:
        body = {"ok": False, "error": error.model_dump(mode="json")}
    else:
        body = {
            "ok": True,
            "result": result.to_json_object() if result is not None else {},
        }
    return GatewayMessage(
        role=MessageRole.TOOL,
        tool_call_id=tool_call_id,
        content=json.dumps(
            body,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ),
    )


def _bounded_tool_output(
    result: ToolExecutionResult,
    *,
    byte_limit: int,
) -> tuple[tuple[bool, str, bool], ...]:
    chunks = [
        *((False, chunk) for chunk in result.stdout),
        *((True, chunk) for chunk in result.stderr),
    ]
    total_bytes = sum(len(chunk.encode("utf-8")) for _, chunk in chunks)
    remaining = byte_limit
    bounded: list[tuple[bool, str, bool]] = []
    for stderr, source_chunk in chunks:
        if remaining <= 0:
            break
        bounded_chunk = source_chunk
        encoded = bounded_chunk.encode("utf-8")
        truncated = len(encoded) > remaining
        if truncated:
            bounded_chunk = encoded[:remaining].decode("utf-8", errors="ignore")
            encoded = bounded_chunk.encode("utf-8")
        if bounded_chunk:
            bounded.append((stderr, bounded_chunk, truncated))
            remaining -= len(encoded)
        if truncated:
            break
    if total_bytes > byte_limit and bounded:
        stderr, chunk, _ = bounded[-1]
        bounded[-1] = (stderr, chunk, True)
    return tuple(bounded)


class AgentLoop:
    """Execute bounded model/tool turns while yielding only typed agent events."""

    def __init__(
        self,
        *,
        gateway: ModelGateway,
        tools: ToolRegistry,
        clock: Clock,
        id_generator: IdGenerator,
        config: AgentLoopConfig | None = None,
    ) -> None:
        self._gateway = gateway
        self._tools = tools
        self._clock = clock
        self._id_generator = id_generator
        self._config = config or AgentLoopConfig()

    async def run(  # noqa: PLR0911, PLR0912, PLR0915 - explicit safety terminal paths
        self,
        loop_input: AgentLoopInput,
    ) -> AsyncIterator[AnyAgentEvent]:
        """Run until final text or a structured, terminal loop failure is emitted."""

        events = _EventFactory(run_id=loop_input.run_id, clock=self._clock)
        transcript = list(loop_input.messages)
        tool_call_count = 0
        semantic_retry_count = 0

        yield events.run_started(
            attempt=loop_input.attempt,
            worker_id=loop_input.worker_id,
        )
        yield events.context_started(
            message_count=len(transcript),
            checkpoint_id=loop_input.checkpoint_id,
        )

        for turn_number in range(1, self._config.max_turns + 1):
            model_call_id = self._id_generator.new_id("model-call")
            request_id = self._id_generator.new_id("request")
            yield events.model_started(
                model_call_id=model_call_id,
                request_id=request_id,
                route_name=loop_input.route_name,
            )
            request = GatewayRequest(
                run_id=loop_input.run_id,
                model_call_id=model_call_id,
                request_id=request_id,
                route_name=loop_input.route_name,
                messages=tuple(transcript),
                tools=self._tools.definitions,
            )

            turn_result: _ModelTurnResult | None = None
            stream_failure: _ModelStreamFailure | None = None
            async for stream_item in self._stream_model(
                request,
                model_call_id=model_call_id,
                turn_number=turn_number,
                events=events,
                tool_call_count=tool_call_count,
            ):
                if isinstance(stream_item, _ModelTurnResult):
                    turn_result = stream_item
                elif isinstance(stream_item, _ModelStreamFailure):
                    stream_failure = stream_item
                else:
                    if isinstance(stream_item, ModelToolCallReceivedEvent):
                        tool_call_count += 1
                    yield stream_item
            if stream_failure is not None:
                yield events.run_failed(error=stream_failure.error)
                return
            if turn_result is None:
                raise AssertionError("model stream must produce a result or failure")

            declares_tool_calls = (
                turn_result.completion.finish_reason is GatewayFinishReason.TOOL_CALLS
            )
            if bool(turn_result.tool_calls) != declares_tool_calls:
                yield events.run_failed(
                    error=_error(
                        "invalid_model_stream",
                        "gateway finish reason disagreed with emitted tool calls",
                        details={"turn": turn_number},
                    )
                )
                return

            if turn_result.tool_calls:
                transcript.append(
                    GatewayMessage(
                        role=MessageRole.ASSISTANT,
                        content=turn_result.text,
                        tool_calls=turn_result.tool_calls,
                    )
                )
                should_stop = False
                async for event in self._execute_tool_calls(
                    turn_result.tool_calls,
                    events=events,
                    transcript=transcript,
                    semantic_retry_count=semantic_retry_count,
                ):
                    if isinstance(event, RunFailedEvent):
                        should_stop = True
                    elif (
                        isinstance(event, ToolCompletedEvent)
                        and event.payload.status is ToolCallStatus.FAILED
                        and event.payload.error is not None
                        and event.payload.error.code in {"malformed_tool_arguments", "unknown_tool"}
                    ):
                        semantic_retry_count += 1
                    yield event
                if should_stop:
                    return
                continue

            if turn_result.completion.finish_reason is GatewayFinishReason.LENGTH:
                yield events.run_failed(
                    error=_error(
                        "model_response_truncated",
                        "model response ended because its provider length limit was reached",
                        details={"turn": turn_number},
                    )
                )
                return
            if turn_result.completion.finish_reason is GatewayFinishReason.CONTENT_FILTER:
                yield events.run_failed(
                    error=_error(
                        "model_response_blocked",
                        "model response was blocked by a provider content filter",
                        details={"turn": turn_number},
                    )
                )
                return
            if turn_result.text:
                completion_payload = RunCompletedPayload(final_text=turn_result.text)
                if (
                    len(completion_payload.model_dump_json().encode("utf-8"))
                    > MAX_EVENT_PAYLOAD_BYTES
                ):
                    yield events.run_failed(
                        error=_error(
                            "model_output_limit",
                            "final model output exceeded the event payload limit",
                            details={"turn": turn_number},
                        )
                    )
                    return
                yield events.run_completed(final_text=turn_result.text)
                return

            semantic_retry_count += 1
            if semantic_retry_count > self._config.max_semantic_retries:
                yield events.run_failed(
                    error=_error(
                        "semantic_retry_limit",
                        "model exceeded the semantic retry budget",
                        details={
                            "retries": semantic_retry_count,
                            "limit": self._config.max_semantic_retries,
                        },
                    )
                )
                return
            transcript.append(
                GatewayMessage(
                    role=MessageRole.SYSTEM,
                    content="The previous response contained neither text nor tool calls.",
                )
            )

        yield events.run_failed(
            error=_error(
                "turn_limit",
                "agent reached the maximum number of model turns",
                details={"limit": self._config.max_turns},
            )
        )

    async def _stream_model(  # noqa: PLR0911, PLR0912 - stream failures terminate explicitly
        self,
        request: GatewayRequest,
        *,
        model_call_id: str,
        turn_number: int,
        events: _EventFactory,
        tool_call_count: int,
    ) -> AsyncIterator[_ModelStreamItem]:
        text_parts: list[str] = []
        model_output_bytes = 0
        tool_calls: list[GatewayToolCall] = []
        completion: GatewayResponseCompleted | None = None
        try:
            async with asyncio.timeout(self._config.model_timeout_seconds):
                async for normalized_event in self._gateway.stream(request):
                    gateway_event: object = normalized_event
                    if completion is not None:
                        yield _ModelStreamFailure(
                            _error(
                                "invalid_model_stream",
                                "gateway emitted data after the terminal event",
                                details={"turn": turn_number},
                            )
                        )
                        return
                    if isinstance(gateway_event, GatewayTextDelta):
                        model_output_bytes += len(gateway_event.delta.encode("utf-8"))
                        if model_output_bytes > self._config.max_model_output_bytes:
                            yield _ModelStreamFailure(
                                _error(
                                    "model_output_limit",
                                    "model output exceeded the configured byte limit",
                                    details={
                                        "limit_bytes": self._config.max_model_output_bytes,
                                        "turn": turn_number,
                                    },
                                )
                            )
                            return
                        payload = ModelTextDeltaPayload(
                            model_call_id=model_call_id,
                            delta=gateway_event.delta,
                        )
                        if len(payload.model_dump_json().encode("utf-8")) > MAX_EVENT_PAYLOAD_BYTES:
                            yield _ModelStreamFailure(
                                _error(
                                    "model_output_limit",
                                    "model delta exceeded the event payload limit",
                                    details={"turn": turn_number},
                                )
                            )
                            return
                        text_parts.append(gateway_event.delta)
                        yield events.model_text(
                            model_call_id=model_call_id,
                            delta=gateway_event.delta,
                        )
                    elif isinstance(gateway_event, GatewayToolCallEvent):
                        argument_size = _json_size(gateway_event.tool_call.arguments)
                        if argument_size > self._config.max_tool_argument_bytes:
                            yield _ModelStreamFailure(
                                _error(
                                    "tool_argument_limit",
                                    "model-generated tool arguments exceeded the byte limit",
                                    details={
                                        "tool_call_id": gateway_event.tool_call.id,
                                        "tool_name": gateway_event.tool_call.name,
                                        "limit_bytes": self._config.max_tool_argument_bytes,
                                    },
                                )
                            )
                            return
                        yield events.tool_received(
                            model_call_id=model_call_id,
                            tool_call=gateway_event.tool_call,
                            argument_hash=canonical_argument_hash(
                                gateway_event.tool_call.arguments
                            ),
                        )
                        if tool_call_count + len(tool_calls) + 1 > self._config.max_tool_calls:
                            yield _ModelStreamFailure(
                                _error(
                                    "tool_call_limit",
                                    "agent reached the maximum number of tool calls",
                                    details={"limit": self._config.max_tool_calls},
                                )
                            )
                            return
                        tool_calls.append(gateway_event.tool_call)
                    elif isinstance(gateway_event, GatewayResponseCompleted):
                        completion = gateway_event
                    else:
                        yield _ModelStreamFailure(
                            _error(
                                "invalid_model_stream",
                                "gateway emitted an unsupported event",
                                details={"turn": turn_number},
                            )
                        )
                        return
        except TimeoutError:
            yield _ModelStreamFailure(
                _error(
                    "model_timeout",
                    "model gateway stream exceeded its timeout",
                    retryable=True,
                    details={"turn": turn_number},
                )
            )
            return
        except DomainOperationError as error:
            yield _ModelStreamFailure(error.error)
            return
        except Exception:
            yield _ModelStreamFailure(
                _error(
                    "model_gateway_failure",
                    "model gateway stream failed",
                    retryable=True,
                    details={"turn": turn_number},
                )
            )
            return

        if completion is None:
            yield _ModelStreamFailure(
                _error(
                    "incomplete_model_stream",
                    "model gateway stream ended without a terminal event",
                    retryable=True,
                    details={"turn": turn_number},
                )
            )
            return
        yield _ModelTurnResult(
            text="".join(text_parts),
            tool_calls=tuple(tool_calls),
            completion=completion,
        )

    async def _execute_tool_calls(
        self,
        tool_calls: Iterable[GatewayToolCall],
        *,
        events: _EventFactory,
        transcript: list[GatewayMessage],
        semantic_retry_count: int,
    ) -> AsyncIterator[AnyAgentEvent]:
        for tool_call in tool_calls:
            try:
                prepared = self._tools.prepare(tool_call.name, tool_call.arguments)
            except DomainOperationError as error:
                yield events.tool_completed(
                    tool_call_id=tool_call.id,
                    status=ToolCallStatus.FAILED,
                    error=error.error,
                )
                transcript.append(_tool_message(tool_call.id, error=error.error))
                semantic_retry_count += 1
                if semantic_retry_count > self._config.max_semantic_retries:
                    yield events.run_failed(
                        error=_error(
                            "semantic_retry_limit",
                            "model exceeded the semantic retry budget",
                            details={
                                "retries": semantic_retry_count,
                                "limit": self._config.max_semantic_retries,
                            },
                        )
                    )
                    return
                continue

            yield events.tool_started(tool_call=tool_call)
            execution_result, execution_error = await self._run_tool(
                prepared,
                tool_call=tool_call,
            )
            if execution_error is not None:
                yield events.tool_completed(
                    tool_call_id=tool_call.id,
                    status=ToolCallStatus.FAILED,
                    error=execution_error,
                )
                transcript.append(_tool_message(tool_call.id, error=execution_error))
                continue
            if execution_result is None:
                raise AssertionError("tool execution must produce a result or error")

            for stderr, chunk, truncated in _bounded_tool_output(
                execution_result,
                byte_limit=self._config.max_tool_output_bytes,
            ):
                yield events.tool_output(
                    tool_call_id=tool_call.id,
                    chunk=chunk,
                    truncated=truncated,
                    stderr=stderr,
                )

            if _json_size(execution_result.result) > self._config.max_tool_result_bytes:
                result_error = _error(
                    "tool_result_limit",
                    "tool result exceeded the configured byte limit",
                    details={
                        "tool_call_id": tool_call.id,
                        "tool_name": tool_call.name,
                        "limit_bytes": self._config.max_tool_result_bytes,
                    },
                )
                yield events.tool_completed(
                    tool_call_id=tool_call.id,
                    status=ToolCallStatus.FAILED,
                    error=result_error,
                )
                transcript.append(_tool_message(tool_call.id, error=result_error))
                continue

            yield events.tool_completed(
                tool_call_id=tool_call.id,
                status=ToolCallStatus.COMPLETED,
                result=execution_result.result,
            )
            transcript.append(_tool_message(tool_call.id, result=execution_result.result))

    async def _run_tool(
        self,
        prepared: PreparedToolExecution,
        *,
        tool_call: GatewayToolCall,
    ) -> tuple[ToolExecutionResult | None, ErrorDetail | None]:
        try:
            async with asyncio.timeout(self._config.tool_timeout_seconds):
                return await prepared.execute(), None
        except TimeoutError:
            return None, _error(
                "tool_timeout",
                "tool execution exceeded its timeout",
                retryable=True,
                details={
                    "tool_call_id": tool_call.id,
                    "tool_name": tool_call.name,
                },
            )
        except DomainOperationError as error:
            return None, error.error
        except Exception:
            return None, _error(
                "tool_execution_failed",
                "tool execution failed",
                details={
                    "tool_call_id": tool_call.id,
                    "tool_name": tool_call.name,
                },
            )


__all__ = [
    "MAX_LOOP_VALUE_BYTES",
    "AgentLoop",
    "AgentLoopConfig",
    "AgentLoopInput",
    "Clock",
    "IdGenerator",
]
