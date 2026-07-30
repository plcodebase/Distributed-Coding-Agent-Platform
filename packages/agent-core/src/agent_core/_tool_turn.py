"""Atomic validation and incrementally bounded execution of one tool-call turn."""

from __future__ import annotations

import asyncio
from contextlib import aclosing
from dataclasses import dataclass
from typing import TYPE_CHECKING

from agent_core._loop_safety import redact_error, redact_json
from agent_core._loop_support import invalid_call_feedback, json_size, loop_error, tool_message
from agent_core.domain.base import FrozenJsonObject
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.domain.models import Checkpoint, canonical_argument_hash
from agent_core.domain.status import ToolCallStatus
from agent_core.tools import (
    PreparedToolExecution,
    ToolEffect,
    ToolExecutionCompleted,
    ToolExecutionContext,
    ToolOutputChannel,
    ToolOutputChunk,
)

_WORKSPACE_RESULT_METADATA_RESERVE_BYTES = 2048
_MAX_WORKSPACE_REVISION_CHARACTERS = 255

if TYPE_CHECKING:
    import uuid

    from agent_core._loop_events import LoopEventFactory
    from agent_core._loop_types import AgentLoopConfig
    from agent_core._model_turn import InvalidToolCall
    from agent_core.checkpoints import CheckpointCoordinator
    from agent_core.events import AnyAgentEvent
    from agent_core.gateway import GatewayMessage, GatewayToolCall
    from agent_core.tools import ToolRegistry
    from platform_telemetry import Redactor


@dataclass(frozen=True, slots=True)
class ToolOutcome:
    """A same-run terminal outcome eligible for safe duplicate reuse."""

    argument_hash: str | None
    status: ToolCallStatus
    result: FrozenJsonObject | None = None
    error: ErrorDetail | None = None


@dataclass(frozen=True, slots=True)
class ToolTurnReport:
    """Events and semantic-budget impact of one atomic tool turn."""

    events: tuple[AnyAgentEvent, ...]
    semantic_failures: int = 0
    terminal: bool = False
    last_checkpoint_id: uuid.UUID | None = None


@dataclass(frozen=True, slots=True)
class _BufferedOutput:
    channel: ToolOutputChannel
    chunk: str
    truncated: bool = False


@dataclass(frozen=True, slots=True)
class _ToolRunResult:
    output: tuple[_BufferedOutput, ...]
    result: FrozenJsonObject | None = None
    error: ErrorDetail | None = None


@dataclass(frozen=True, slots=True)
class _PreparedCall:
    tool_call: GatewayToolCall
    argument_hash: str
    prepared: PreparedToolExecution | None = None
    validation_error: ErrorDetail | None = None


def _checkpoint_matches_request(
    checkpoint: Checkpoint,
    *,
    run_id: uuid.UUID,
    message_sequence: int,
    task_plan: FrozenJsonObject,
    context_summary: str | None,
) -> bool:
    return (
        isinstance(checkpoint, Checkpoint)
        and checkpoint.run_id == run_id
        and checkpoint.message_sequence == message_sequence
        and checkpoint.task_plan == task_plan
        and checkpoint.context_summary == context_summary
    )


def _valid_workspace_revision(value: object) -> bool:
    return (
        isinstance(value, str)
        and value == value.strip()
        and "\x00" not in value
        and 0 < len(value) <= _MAX_WORKSPACE_REVISION_CHARACTERS
    )


def _utf8_prefix(value: str, byte_limit: int) -> str:
    return value.encode("utf-8")[:byte_limit].decode("utf-8", errors="ignore")


def _append_bounded_output(
    output: list[_BufferedOutput],
    *,
    channel: ToolOutputChannel,
    chunk: str,
    remaining_bytes: int,
) -> tuple[int, bool]:
    encoded_size = len(chunk.encode("utf-8"))
    if encoded_size <= remaining_bytes:
        output.append(_BufferedOutput(channel=channel, chunk=chunk))
        return remaining_bytes - encoded_size, False

    prefix = _utf8_prefix(chunk, remaining_bytes)
    if prefix:
        output.append(_BufferedOutput(channel=channel, chunk=prefix, truncated=True))
    elif output:
        previous = output[-1]
        output[-1] = _BufferedOutput(
            channel=previous.channel,
            chunk=previous.chunk,
            truncated=True,
        )
    return 0, True


def _coalesce_and_redact_output(
    output: tuple[_BufferedOutput, ...],
    *,
    byte_limit: int,
    redactor: Redactor,
) -> tuple[_BufferedOutput, ...]:
    coalesced: list[_BufferedOutput] = []
    for item in output:
        if coalesced and coalesced[-1].channel is item.channel:
            previous = coalesced[-1]
            coalesced[-1] = _BufferedOutput(
                channel=item.channel,
                chunk=previous.chunk + item.chunk,
                truncated=previous.truncated or item.truncated,
            )
        else:
            coalesced.append(item)

    bounded: list[_BufferedOutput] = []
    remaining = byte_limit
    for item in coalesced:
        redacted = redactor.redact_text(item.chunk)
        remaining, overflow = _append_bounded_output(
            bounded,
            channel=item.channel,
            chunk=redacted,
            remaining_bytes=remaining,
        )
        if item.truncated and bounded:
            previous = bounded[-1]
            bounded[-1] = _BufferedOutput(
                channel=previous.channel,
                chunk=previous.chunk,
                truncated=True,
            )
        if overflow:
            break
    return tuple(bounded)


class ToolTurnExecutor:
    """Validate a complete tool turn before executing any of its calls."""

    def __init__(
        self,
        *,
        tools: ToolRegistry,
        config: AgentLoopConfig,
        redactor: Redactor,
        checkpoints: CheckpointCoordinator | None = None,
    ) -> None:
        self._tools = tools
        self._config = config
        self._redactor = redactor
        self._checkpoints = checkpoints

    async def execute(  # noqa: PLR0911, PLR0912, PLR0915 - fail-closed orchestration paths
        self,
        tool_calls: tuple[GatewayToolCall, ...],
        invalid_tool_calls: tuple[InvalidToolCall, ...],
        *,
        events: LoopEventFactory,
        transcript: list[GatewayMessage],
        outcomes: dict[str, ToolOutcome],
        semantic_retry_count: int,
        run_id: uuid.UUID,
        task_plan: FrozenJsonObject,
        context_summary: str | None,
    ) -> ToolTurnReport:
        conflict = self._find_id_conflict(tool_calls, invalid_tool_calls, outcomes)
        if conflict is not None:
            return self._reject_conflicting_turn(
                tool_calls,
                invalid_tool_calls,
                conflict=conflict,
                events=events,
                transcript=transcript,
            )

        prepared_calls = self._prepare_calls(tool_calls, outcomes)
        if invalid_tool_calls or any(item.validation_error is not None for item in prepared_calls):
            return self._reject_invalid_turn(
                prepared_calls,
                invalid_tool_calls,
                events=events,
                transcript=transcript,
                outcomes=outcomes,
                semantic_retry_count=semantic_retry_count,
            )

        emitted: list[AnyAgentEvent] = []
        last_checkpoint_id: uuid.UUID | None = None
        for item in prepared_calls:
            prior = outcomes.get(item.tool_call.id)
            if prior is not None:
                emitted.append(self._completion_from_outcome(events, item.tool_call.id, prior))
                transcript.append(
                    tool_message(
                        item.tool_call.id,
                        result=prior.result,
                        error=prior.error,
                    )
                )
                continue
            if item.prepared is None:
                raise AssertionError("new validated call must have a prepared execution")

            checkpoint: Checkpoint | None = None
            if item.prepared.effect in {ToolEffect.WORKSPACE_MUTATION, ToolEffect.COMMAND}:
                if self._checkpoints is None:
                    error = loop_error(
                        "checkpoint_required",
                        "side-effecting tools require a checkpoint coordinator",
                        details={
                            "tool_call_id": item.tool_call.id,
                            "tool_name": item.tool_call.name,
                        },
                    )
                    emitted.append(
                        events.tool_completed(
                            tool_call_id=item.tool_call.id,
                            status=ToolCallStatus.FAILED,
                            error=error,
                        )
                    )
                    emitted.append(events.run_failed(error=error))
                    return ToolTurnReport(events=tuple(emitted), terminal=True)
                try:
                    checkpoint = await self._checkpoints.create_before_tool(
                        run_id=run_id,
                        tool_call_id=item.tool_call.id,
                        messages=tuple(transcript),
                        task_plan=task_plan,
                        context_summary=context_summary,
                    )
                except DomainOperationError as checkpoint_error:
                    error = redact_error(checkpoint_error.error, self._redactor)
                    emitted.append(
                        events.tool_completed(
                            tool_call_id=item.tool_call.id,
                            status=ToolCallStatus.FAILED,
                            error=error,
                        )
                    )
                    emitted.append(events.run_failed(error=error))
                    return ToolTurnReport(events=tuple(emitted), terminal=True)
                except Exception:
                    error = loop_error(
                        "checkpoint_create_failed",
                        "the pre-tool checkpoint could not be created",
                        details={
                            "tool_call_id": item.tool_call.id,
                            "tool_name": item.tool_call.name,
                        },
                    )
                    emitted.append(
                        events.tool_completed(
                            tool_call_id=item.tool_call.id,
                            status=ToolCallStatus.FAILED,
                            error=error,
                        )
                    )
                    emitted.append(events.run_failed(error=error))
                    return ToolTurnReport(events=tuple(emitted), terminal=True)
                if not _checkpoint_matches_request(
                    checkpoint,
                    run_id=run_id,
                    message_sequence=len(transcript),
                    task_plan=task_plan,
                    context_summary=context_summary,
                ):
                    error = loop_error(
                        "checkpoint_contract_invalid",
                        "the checkpoint coordinator returned mismatched state",
                        details={
                            "tool_call_id": item.tool_call.id,
                            "tool_name": item.tool_call.name,
                        },
                    )
                    emitted.append(
                        events.tool_completed(
                            tool_call_id=item.tool_call.id,
                            status=ToolCallStatus.FAILED,
                            error=error,
                        )
                    )
                    emitted.append(events.run_failed(error=error))
                    return ToolTurnReport(events=tuple(emitted), terminal=True)
                last_checkpoint_id = checkpoint.id
                emitted.append(
                    events.checkpoint_created(
                        checkpoint_id=checkpoint.id,
                        message_sequence=checkpoint.message_sequence,
                        workspace_revision=checkpoint.workspace_revision,
                    )
                )

            emitted.append(events.tool_started(tool_call=item.tool_call))
            try:
                run_result = await self._run_tool(
                    item.prepared,
                    tool_call=item.tool_call,
                    run_id=run_id,
                    checkpoint=checkpoint,
                )
            except asyncio.CancelledError as cancelled:
                if checkpoint is not None:
                    coordinator = self._checkpoints
                    if coordinator is None:
                        raise RuntimeError("checkpoint requires a coordinator") from cancelled
                    restore_task = asyncio.create_task(coordinator.rollback(checkpoint))
                    try:
                        await asyncio.shield(restore_task)
                    except asyncio.CancelledError:
                        await restore_task
                    except Exception as cancellation_restore_error:
                        raise DomainOperationError(
                            code="checkpoint_restore_failed",
                            message="workspace restoration failed after tool cancellation",
                            details={
                                "checkpoint_id": str(checkpoint.id),
                                "tool_call_id": item.tool_call.id,
                            },
                        ) from cancellation_restore_error
                raise
            emitted.extend(
                events.tool_output(
                    tool_call_id=item.tool_call.id,
                    chunk=output.chunk,
                    truncated=output.truncated,
                    stderr=output.channel is ToolOutputChannel.STDERR,
                )
                for output in run_result.output
            )

            if run_result.error is not None and checkpoint is not None:
                coordinator = self._checkpoints
                if coordinator is None:
                    raise AssertionError("checkpoint requires a coordinator")
                try:
                    await coordinator.rollback(checkpoint)
                except Exception:
                    restore_error = loop_error(
                        "checkpoint_restore_failed",
                        "the workspace could not be restored after tool failure",
                        details={
                            "checkpoint_id": str(checkpoint.id),
                            "tool_call_id": item.tool_call.id,
                        },
                    )
                    emitted.append(
                        events.tool_completed(
                            tool_call_id=item.tool_call.id,
                            status=ToolCallStatus.FAILED,
                            error=restore_error,
                        )
                    )
                    emitted.append(events.run_failed(error=restore_error))
                    return ToolTurnReport(
                        events=tuple(emitted),
                        terminal=True,
                        last_checkpoint_id=last_checkpoint_id,
                    )
            elif run_result.result is not None and checkpoint is not None:
                coordinator = self._checkpoints
                if coordinator is None:
                    raise AssertionError("checkpoint requires a coordinator")
                try:
                    workspace_revision = await coordinator.complete_tool(
                        checkpoint,
                        tool_call_id=item.tool_call.id,
                    )
                except Exception:
                    try:
                        await coordinator.rollback(checkpoint)
                    except Exception:
                        error_code = "checkpoint_restore_failed"
                        error_message = "workspace restoration failed after checkpoint finalization"
                    else:
                        error_code = "checkpoint_finalize_failed"
                        error_message = "the successful tool state could not be checkpointed"
                    finalize_error = loop_error(
                        error_code,
                        error_message,
                        details={
                            "checkpoint_id": str(checkpoint.id),
                            "tool_call_id": item.tool_call.id,
                        },
                    )
                    emitted.append(
                        events.tool_completed(
                            tool_call_id=item.tool_call.id,
                            status=ToolCallStatus.FAILED,
                            error=finalize_error,
                        )
                    )
                    emitted.append(events.run_failed(error=finalize_error))
                    return ToolTurnReport(
                        events=tuple(emitted),
                        terminal=True,
                        last_checkpoint_id=last_checkpoint_id,
                    )
                result_error: ErrorDetail | None = None
                completed_result: FrozenJsonObject | None = None
                if not _valid_workspace_revision(workspace_revision):
                    result_error = loop_error(
                        "checkpoint_finalize_failed",
                        "the checkpoint coordinator returned an invalid workspace revision",
                        details={
                            "checkpoint_id": str(checkpoint.id),
                            "tool_call_id": item.tool_call.id,
                        },
                    )
                else:
                    result_value = run_result.result.to_json_object()
                    result_value["workspace_revision"] = workspace_revision
                    completed_result = FrozenJsonObject(result_value)
                    if json_size(completed_result) > self._config.max_tool_result_bytes:
                        result_error = loop_error(
                            "tool_result_limit",
                            "the completed tool metadata exceeded the configured byte limit",
                            details={
                                "tool_call_id": item.tool_call.id,
                                "tool_name": item.tool_call.name,
                                "limit_bytes": self._config.max_tool_result_bytes,
                            },
                        )
                if result_error is not None:
                    try:
                        await coordinator.rollback(checkpoint)
                    except Exception:
                        result_error = loop_error(
                            "checkpoint_restore_failed",
                            "workspace restoration failed after invalid completion metadata",
                            details={
                                "checkpoint_id": str(checkpoint.id),
                                "tool_call_id": item.tool_call.id,
                            },
                        )
                    emitted.append(
                        events.tool_completed(
                            tool_call_id=item.tool_call.id,
                            status=ToolCallStatus.FAILED,
                            error=result_error,
                        )
                    )
                    emitted.append(events.run_failed(error=result_error))
                    return ToolTurnReport(
                        events=tuple(emitted),
                        terminal=True,
                        last_checkpoint_id=last_checkpoint_id,
                    )
                if completed_result is None:
                    raise AssertionError("valid completion metadata must produce a result")
                run_result = _ToolRunResult(
                    output=run_result.output,
                    result=completed_result,
                )

            if run_result.error is not None:
                outcome = ToolOutcome(
                    argument_hash=item.argument_hash,
                    status=ToolCallStatus.FAILED,
                    error=run_result.error,
                )
            elif run_result.result is not None:
                outcome = ToolOutcome(
                    argument_hash=item.argument_hash,
                    status=ToolCallStatus.COMPLETED,
                    result=run_result.result,
                )
            else:
                raise AssertionError("tool execution must produce a result or error")

            outcomes[item.tool_call.id] = outcome
            emitted.append(self._completion_from_outcome(events, item.tool_call.id, outcome))
            transcript.append(
                tool_message(
                    item.tool_call.id,
                    result=outcome.result,
                    error=outcome.error,
                )
            )

        return ToolTurnReport(
            events=tuple(emitted),
            last_checkpoint_id=last_checkpoint_id,
        )

    def _find_id_conflict(
        self,
        tool_calls: tuple[GatewayToolCall, ...],
        invalid_tool_calls: tuple[InvalidToolCall, ...],
        outcomes: dict[str, ToolOutcome],
    ) -> tuple[str, str] | None:
        hashes = {tool_call_id: outcome.argument_hash for tool_call_id, outcome in outcomes.items()}
        for call in tool_calls:
            current_hash = canonical_argument_hash(call.arguments)
            if call.id in hashes and hashes[call.id] != current_hash:
                return call.id, call.name
            hashes[call.id] = current_hash
        for invalid_call in invalid_tool_calls:
            if invalid_call.tool_call_id in hashes:
                return invalid_call.tool_call_id, invalid_call.tool_name
            hashes[invalid_call.tool_call_id] = None
        return None

    def _reject_conflicting_turn(
        self,
        tool_calls: tuple[GatewayToolCall, ...],
        invalid_tool_calls: tuple[InvalidToolCall, ...],
        *,
        conflict: tuple[str, str],
        events: LoopEventFactory,
        transcript: list[GatewayMessage],
    ) -> ToolTurnReport:
        conflict_id, conflict_name = conflict
        conflict_error = loop_error(
            "tool_call_id_conflict",
            "a stable tool-call identifier was reused with different arguments",
            details={
                "tool_call_id": conflict_id,
                "tool_name": conflict_name,
            },
        )
        rejected_error = loop_error(
            "tool_turn_rejected",
            "the tool turn was rejected before execution",
        )
        emitted: list[AnyAgentEvent] = []
        for call in tool_calls:
            error = conflict_error if call.id == conflict_id else rejected_error
            emitted.append(
                events.tool_completed(
                    tool_call_id=call.id,
                    status=ToolCallStatus.FAILED,
                    error=error,
                )
            )
            transcript.append(tool_message(call.id, error=error))
        for invalid_call in invalid_tool_calls:
            error = (
                conflict_error if invalid_call.tool_call_id == conflict_id else invalid_call.error
            )
            emitted.append(
                events.tool_completed(
                    tool_call_id=invalid_call.tool_call_id,
                    status=ToolCallStatus.FAILED,
                    error=error,
                )
            )
        if invalid_tool_calls:
            transcript.append(
                invalid_call_feedback(
                    tuple(
                        (call.tool_call_id, call.tool_name, call.error)
                        for call in invalid_tool_calls
                    )
                )
            )
        emitted.append(events.run_failed(error=conflict_error))
        return ToolTurnReport(events=tuple(emitted), terminal=True)

    def _prepare_calls(
        self,
        tool_calls: tuple[GatewayToolCall, ...],
        outcomes: dict[str, ToolOutcome],
    ) -> tuple[_PreparedCall, ...]:
        prepared: list[_PreparedCall] = []
        pending_ids: set[str] = set()
        for call in tool_calls:
            argument_hash = canonical_argument_hash(call.arguments)
            if call.id in outcomes or call.id in pending_ids:
                prepared.append(_PreparedCall(tool_call=call, argument_hash=argument_hash))
                continue
            pending_ids.add(call.id)
            try:
                execution = self._tools.prepare(call.name, call.arguments)
            except DomainOperationError as error:
                prepared.append(
                    _PreparedCall(
                        tool_call=call,
                        argument_hash=argument_hash,
                        validation_error=redact_error(error.error, self._redactor),
                    )
                )
            else:
                prepared.append(
                    _PreparedCall(
                        tool_call=call,
                        argument_hash=argument_hash,
                        prepared=execution,
                    )
                )
        return tuple(prepared)

    def _reject_invalid_turn(
        self,
        prepared_calls: tuple[_PreparedCall, ...],
        invalid_tool_calls: tuple[InvalidToolCall, ...],
        *,
        events: LoopEventFactory,
        transcript: list[GatewayMessage],
        outcomes: dict[str, ToolOutcome],
        semantic_retry_count: int,
    ) -> ToolTurnReport:
        rejected_error = loop_error(
            "tool_turn_rejected",
            "the tool turn contained an invalid call and none were executed",
        )
        emitted: list[AnyAgentEvent] = []
        for item in prepared_calls:
            error = item.validation_error or rejected_error
            outcome = ToolOutcome(
                argument_hash=item.argument_hash,
                status=ToolCallStatus.FAILED,
                error=error,
            )
            outcomes.setdefault(item.tool_call.id, outcome)
            emitted.append(
                events.tool_completed(
                    tool_call_id=item.tool_call.id,
                    status=ToolCallStatus.FAILED,
                    error=error,
                )
            )
            transcript.append(tool_message(item.tool_call.id, error=error))

        for call in invalid_tool_calls:
            emitted.append(
                events.tool_completed(
                    tool_call_id=call.tool_call_id,
                    status=ToolCallStatus.FAILED,
                    error=call.error,
                )
            )
            outcomes.setdefault(
                call.tool_call_id,
                ToolOutcome(
                    argument_hash=None,
                    status=ToolCallStatus.FAILED,
                    error=call.error,
                ),
            )
        if invalid_tool_calls:
            transcript.append(
                invalid_call_feedback(
                    tuple(
                        (call.tool_call_id, call.tool_name, call.error)
                        for call in invalid_tool_calls
                    )
                )
            )

        terminal = semantic_retry_count + 1 > self._config.max_semantic_retries
        if terminal:
            emitted.append(
                events.run_failed(
                    error=loop_error(
                        "semantic_retry_limit",
                        "model exceeded the semantic retry budget",
                        details={
                            "retries": semantic_retry_count + 1,
                            "limit": self._config.max_semantic_retries,
                        },
                    )
                )
            )
        return ToolTurnReport(
            events=tuple(emitted),
            semantic_failures=1,
            terminal=terminal,
        )

    async def _run_tool(  # noqa: PLR0911, PLR0912 - fail-closed stream protocol
        self,
        prepared: PreparedToolExecution,
        *,
        tool_call: GatewayToolCall,
        run_id: uuid.UUID,
        checkpoint: Checkpoint | None,
    ) -> _ToolRunResult:
        result_limit = self._config.max_tool_result_bytes
        if checkpoint is not None:
            result_limit = max(
                1,
                result_limit - _WORKSPACE_RESULT_METADATA_RESERVE_BYTES,
            )
        context = ToolExecutionContext(
            run_id=run_id,
            tool_call_id=tool_call.id,
            checkpoint_id=checkpoint.id if checkpoint is not None else None,
            workspace_revision=(checkpoint.workspace_revision if checkpoint is not None else None),
            max_output_bytes=self._config.max_tool_output_bytes,
            max_result_bytes=result_limit,
        )
        output: list[_BufferedOutput] = []
        remaining = context.max_output_bytes
        completion: ToolExecutionCompleted | None = None
        try:
            async with asyncio.timeout(self._config.tool_timeout_seconds):
                async with aclosing(prepared.stream(context)) as stream:
                    async for item in stream:
                        stream_item: object = item
                        if completion is not None:
                            return self._failed_run(
                                output,
                                loop_error(
                                    "invalid_tool_stream",
                                    "tool emitted data after its terminal result",
                                    details={
                                        "tool_call_id": tool_call.id,
                                        "tool_name": tool_call.name,
                                    },
                                ),
                            )
                        if isinstance(stream_item, ToolOutputChunk):
                            remaining, overflow = _append_bounded_output(
                                output,
                                channel=stream_item.channel,
                                chunk=stream_item.chunk,
                                remaining_bytes=remaining,
                            )
                            if overflow:
                                return self._failed_run(
                                    output,
                                    loop_error(
                                        "tool_output_limit",
                                        "tool output exceeded the configured byte limit",
                                        details={
                                            "tool_call_id": tool_call.id,
                                            "tool_name": tool_call.name,
                                            "limit_bytes": context.max_output_bytes,
                                        },
                                    ),
                                )
                        elif isinstance(stream_item, ToolExecutionCompleted):
                            completion = stream_item
                        else:
                            return self._failed_run(
                                output,
                                loop_error(
                                    "invalid_tool_stream",
                                    "tool emitted an unsupported execution event",
                                    details={
                                        "tool_call_id": tool_call.id,
                                        "tool_name": tool_call.name,
                                    },
                                ),
                            )
        except TimeoutError:
            return self._failed_run(
                output,
                loop_error(
                    "tool_timeout",
                    "tool execution exceeded its timeout",
                    retryable=True,
                    details={
                        "tool_call_id": tool_call.id,
                        "tool_name": tool_call.name,
                    },
                ),
            )
        except DomainOperationError as error:
            return self._failed_run(
                output,
                redact_error(error.error, self._redactor),
            )
        except Exception:
            return self._failed_run(
                output,
                loop_error(
                    "tool_execution_failed",
                    "tool execution failed",
                    details={
                        "tool_call_id": tool_call.id,
                        "tool_name": tool_call.name,
                    },
                ),
            )

        if completion is None:
            return self._failed_run(
                output,
                loop_error(
                    "invalid_tool_stream",
                    "tool stream ended without a terminal result",
                    details={
                        "tool_call_id": tool_call.id,
                        "tool_name": tool_call.name,
                    },
                ),
            )
        if json_size(completion.result) > context.max_result_bytes:
            return self._failed_run(
                output,
                loop_error(
                    "tool_result_limit",
                    "tool result exceeded the configured byte limit",
                    details={
                        "tool_call_id": tool_call.id,
                        "tool_name": tool_call.name,
                        "limit_bytes": context.max_result_bytes,
                    },
                ),
            )
        safe_result = redact_json(completion.result, self._redactor)
        if json_size(safe_result) > context.max_result_bytes:
            return self._failed_run(
                output,
                loop_error(
                    "tool_result_limit",
                    "redacted tool result exceeded the configured byte limit",
                    details={
                        "tool_call_id": tool_call.id,
                        "tool_name": tool_call.name,
                        "limit_bytes": context.max_result_bytes,
                    },
                ),
            )
        return _ToolRunResult(
            output=_coalesce_and_redact_output(
                tuple(output),
                byte_limit=context.max_output_bytes,
                redactor=self._redactor,
            ),
            result=safe_result,
        )

    def _failed_run(
        self,
        output: list[_BufferedOutput],
        error: ErrorDetail,
    ) -> _ToolRunResult:
        return _ToolRunResult(
            output=_coalesce_and_redact_output(
                tuple(output),
                byte_limit=self._config.max_tool_output_bytes,
                redactor=self._redactor,
            ),
            error=error,
        )

    @staticmethod
    def _completion_from_outcome(
        events: LoopEventFactory,
        tool_call_id: str,
        outcome: ToolOutcome,
    ) -> AnyAgentEvent:
        return events.tool_completed(
            tool_call_id=tool_call_id,
            status=outcome.status,
            result=outcome.result,
            error=outcome.error,
        )


__all__ = ["ToolOutcome", "ToolTurnExecutor", "ToolTurnReport"]
