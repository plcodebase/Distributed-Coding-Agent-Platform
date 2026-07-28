"""Contiguous typed event construction for one in-memory loop execution."""

from __future__ import annotations

from typing import TYPE_CHECKING

from agent_core.events import (
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

if TYPE_CHECKING:
    import uuid
    from datetime import datetime

    from agent_core._loop_types import Clock
    from agent_core.domain.base import FrozenJsonObject
    from agent_core.domain.errors import ErrorDetail
    from agent_core.domain.status import ToolCallStatus
    from agent_core.gateway import GatewayToolCall


class LoopEventFactory:
    """Allocate contiguous in-memory event sequence numbers."""

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

    def tool_rejected(
        self,
        *,
        model_call_id: str,
        tool_call_id: str,
        tool_name: str,
        error: ErrorDetail,
    ) -> ModelToolCallReceivedEvent:
        sequence, created_at = self._metadata()
        return ModelToolCallReceivedEvent(
            run_id=self._run_id,
            sequence=sequence,
            payload=ModelToolCallReceivedPayload(
                model_call_id=model_call_id,
                tool_call_id=tool_call_id,
                tool_name=tool_name,
                error=error,
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
        event_type = ToolStderrEvent if stderr else ToolStdoutEvent
        return event_type(
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


__all__ = ["LoopEventFactory"]
