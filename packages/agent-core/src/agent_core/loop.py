"""Bounded, provider-neutral agent-loop coordinator."""

from __future__ import annotations

from typing import TYPE_CHECKING

from agent_core._loop_events import LoopEventFactory
from agent_core._loop_safety import redact_json
from agent_core._loop_support import loop_error
from agent_core._loop_types import (
    MAX_GATEWAY_REQUEST_BYTES,
    MAX_LOOP_VALUE_BYTES,
    AgentLoopConfig,
    AgentLoopInput,
    Clock,
    IdGenerator,
)
from agent_core._model_turn import ModelTurnFailure, ModelTurnResult, ModelTurnRunner
from agent_core._tool_turn import ToolOutcome, ToolTurnExecutor
from agent_core.events import (
    MAX_EVENT_PAYLOAD_BYTES,
    AnyAgentEvent,
    ModelToolCallReceivedEvent,
    RunCompletedPayload,
)
from agent_core.gateway import (
    GatewayFinishReason,
    GatewayMessage,
    GatewayRequest,
    GatewayToolCall,
    MessageRole,
    ModelGateway,
)
from platform_telemetry import Redactor

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from agent_core.checkpoints import CheckpointCoordinator
    from agent_core.tools import ToolRegistry


def _redact_message(message: GatewayMessage, redactor: Redactor) -> GatewayMessage:
    """Remove configured secrets from caller-supplied initial context."""

    tool_calls = tuple(
        GatewayToolCall(
            id=call.id,
            name=call.name,
            arguments=redact_json(call.arguments, redactor),
        )
        for call in message.tool_calls
    )
    return GatewayMessage(
        role=message.role,
        content=redactor.redact_text(message.content),
        tool_call_id=message.tool_call_id,
        tool_calls=tool_calls,
    )


def _request_size(request: GatewayRequest) -> int:
    return len(request.model_dump_json().encode("utf-8"))


class AgentLoop:
    """Coordinate bounded model/tool turns while yielding only typed events."""

    def __init__(
        self,
        *,
        gateway: ModelGateway,
        tools: ToolRegistry,
        clock: Clock,
        id_generator: IdGenerator,
        config: AgentLoopConfig | None = None,
        redactor: Redactor | None = None,
        checkpoints: CheckpointCoordinator | None = None,
    ) -> None:
        self._tools = tools
        self._clock = clock
        self._id_generator = id_generator
        self._config = config or AgentLoopConfig()
        self._redactor = redactor or Redactor()
        self._model_turns = ModelTurnRunner(
            gateway=gateway,
            config=self._config,
            redactor=self._redactor,
        )
        self._tool_turns = ToolTurnExecutor(
            tools=tools,
            config=self._config,
            redactor=self._redactor,
            checkpoints=checkpoints,
        )

    async def run(  # noqa: PLR0911, PLR0912, PLR0915 - explicit terminal policy paths
        self,
        loop_input: AgentLoopInput,
    ) -> AsyncIterator[AnyAgentEvent]:
        """Run until final text or a structured terminal failure is emitted."""

        events = LoopEventFactory(run_id=loop_input.run_id, clock=self._clock)
        transcript = [_redact_message(message, self._redactor) for message in loop_input.messages]
        tool_call_count = 0
        semantic_retry_count = 0
        outcomes = {
            outcome.tool_call_id: ToolOutcome(
                argument_hash=outcome.argument_hash,
                status=outcome.status,
                result=outcome.result,
                error=outcome.error,
            )
            for outcome in loop_input.prior_tool_outcomes
        }
        last_checkpoint_id = loop_input.checkpoint_id

        yield events.run_started(
            attempt=loop_input.attempt,
            worker_id=loop_input.worker_id,
        )
        yield events.context_started(
            message_count=len(transcript),
            checkpoint_id=loop_input.checkpoint_id,
        )

        for turn_number in range(1, self._config.max_turns + 1):
            if len(transcript) > self._config.max_context_messages:
                yield events.run_failed(
                    error=loop_error(
                        "context_limit",
                        "agent context exceeded the configured message limit",
                        details={
                            "message_count": len(transcript),
                            "limit": self._config.max_context_messages,
                        },
                    )
                )
                return

            model_call_id = self._id_generator.new_id("model-call")
            request_id = self._id_generator.new_id("request")
            request = GatewayRequest(
                tenant_id=loop_input.tenant_id,
                session_id=loop_input.session_id,
                run_id=loop_input.run_id,
                turn_number=turn_number,
                model_call_id=model_call_id,
                request_id=request_id,
                route_name=loop_input.route_name,
                messages=tuple(transcript),
                tools=self._tools.definitions,
            )
            request_size = _request_size(request)
            if request_size > self._config.max_gateway_request_bytes:
                yield events.run_failed(
                    error=loop_error(
                        "context_limit",
                        "gateway request exceeded the configured byte limit",
                        details={
                            "request_bytes": request_size,
                            "limit_bytes": self._config.max_gateway_request_bytes,
                        },
                    )
                )
                return

            yield events.model_started(
                model_call_id=model_call_id,
                request_id=request_id,
                route_name=loop_input.route_name,
            )

            turn_result: ModelTurnResult | None = None
            stream_failure: ModelTurnFailure | None = None
            async for stream_item in self._model_turns.stream(
                request,
                model_call_id=model_call_id,
                turn_number=turn_number,
                events=events,
                prior_tool_call_count=tool_call_count,
            ):
                if isinstance(stream_item, ModelTurnResult):
                    turn_result = stream_item
                elif isinstance(stream_item, ModelTurnFailure):
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
            if turn_result.has_tool_calls != declares_tool_calls:
                yield events.run_failed(
                    error=loop_error(
                        "invalid_model_stream",
                        "gateway finish reason disagreed with emitted tool calls",
                        details={"turn": turn_number},
                    )
                )
                return

            if turn_result.has_tool_calls:
                if turn_result.text or turn_result.tool_calls:
                    transcript.append(
                        GatewayMessage(
                            role=MessageRole.ASSISTANT,
                            content=turn_result.text,
                            tool_calls=turn_result.tool_calls,
                        )
                    )
                report = await self._tool_turns.execute(
                    turn_result.tool_calls,
                    turn_result.invalid_tool_calls,
                    events=events,
                    transcript=transcript,
                    outcomes=outcomes,
                    semantic_retry_count=semantic_retry_count,
                    run_id=loop_input.run_id,
                    task_plan=loop_input.task_plan,
                    context_summary=loop_input.context_summary,
                )
                if report.last_checkpoint_id is not None:
                    last_checkpoint_id = report.last_checkpoint_id
                semantic_retry_count += report.semantic_failures
                for event in report.events:
                    yield event
                if report.terminal:
                    return
                continue

            if turn_result.completion.finish_reason is GatewayFinishReason.LENGTH:
                yield events.run_failed(
                    error=loop_error(
                        "model_response_truncated",
                        "model response ended because its provider length limit was reached",
                        details={"turn": turn_number},
                    )
                )
                return
            if turn_result.completion.finish_reason is GatewayFinishReason.CONTENT_FILTER:
                yield events.run_failed(
                    error=loop_error(
                        "model_response_blocked",
                        "model response was blocked by a provider content filter",
                        details={"turn": turn_number},
                    )
                )
                return
            if turn_result.text:
                completion_payload = RunCompletedPayload(
                    final_text=turn_result.text,
                    checkpoint_id=last_checkpoint_id,
                )
                if (
                    len(completion_payload.model_dump_json().encode("utf-8"))
                    > MAX_EVENT_PAYLOAD_BYTES
                ):
                    yield events.run_failed(
                        error=loop_error(
                            "model_output_limit",
                            "final model output exceeded the event payload limit",
                            details={"turn": turn_number},
                        )
                    )
                    return
                yield events.run_completed(
                    final_text=turn_result.text,
                    checkpoint_id=last_checkpoint_id,
                )
                return

            semantic_retry_count += 1
            if semantic_retry_count > self._config.max_semantic_retries:
                yield events.run_failed(
                    error=loop_error(
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
            error=loop_error(
                "turn_limit",
                "agent reached the maximum number of model turns",
                details={"limit": self._config.max_turns},
            )
        )


__all__ = [
    "MAX_GATEWAY_REQUEST_BYTES",
    "MAX_LOOP_VALUE_BYTES",
    "AgentLoop",
    "AgentLoopConfig",
    "AgentLoopInput",
    "Clock",
    "IdGenerator",
]
