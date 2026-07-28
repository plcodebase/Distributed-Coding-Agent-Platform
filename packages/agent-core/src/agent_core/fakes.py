"""Deterministic in-process fakes for agent-core tests and local development."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from agent_core.domain.base import normalize_timestamp
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

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence


@dataclass(frozen=True, slots=True)
class ScriptedGatewayTurn:
    """One deterministic stream returned for one gateway request."""

    events: tuple[GatewayEvent, ...] = ()
    delay_seconds: float = 0
    error: Exception | None = None

    def __post_init__(self) -> None:
        if self.delay_seconds < 0:
            raise ValueError("delay_seconds may not be negative")

    @classmethod
    def text(
        cls,
        text: str,
        *,
        chunk_size: int | None = None,
    ) -> ScriptedGatewayTurn:
        """Build a successful final-text response with deterministic chunks."""

        if chunk_size is not None and chunk_size < 1:
            raise ValueError("chunk_size must be positive")
        chunks: tuple[str, ...]
        if chunk_size is None:
            chunks = (text,)
        else:
            chunks = tuple(
                text[offset : offset + chunk_size] for offset in range(0, len(text), chunk_size)
            )
        return cls(
            events=(
                *(GatewayTextDelta(delta=chunk) for chunk in chunks if chunk),
                GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP),
            )
        )

    @classmethod
    def tool_calls(
        cls,
        *tool_calls: GatewayToolCall,
        text: str = "",
    ) -> ScriptedGatewayTurn:
        """Build one tool-calling response followed by a terminal stream event."""

        return cls(
            events=(
                *((GatewayTextDelta(delta=text),) if text else ()),
                *(GatewayToolCallEvent(tool_call=tool_call) for tool_call in tool_calls),
                GatewayResponseCompleted(finish_reason=GatewayFinishReason.TOOL_CALLS),
            )
        )

    @classmethod
    def invalid_tool_call(
        cls,
        *,
        tool_call_id: str,
        tool_name: str,
        error: ErrorDetail | None = None,
    ) -> ScriptedGatewayTurn:
        """Build a call whose provider arguments could not be parsed as JSON."""

        return cls(
            events=(
                GatewayInvalidToolCallEvent(
                    tool_call_id=tool_call_id,
                    tool_name=tool_name,
                    error=error
                    or ErrorDetail(
                        code="malformed_tool_arguments",
                        message="model-generated tool arguments were not valid JSON",
                    ),
                ),
                GatewayResponseCompleted(finish_reason=GatewayFinishReason.TOOL_CALLS),
            )
        )


class ScriptedModelGateway:
    """A fake gateway that records requests and consumes a fixed response script."""

    def __init__(self, turns: Sequence[ScriptedGatewayTurn]) -> None:
        self._turns = tuple(turns)
        self._next_turn = 0
        self._requests: list[GatewayRequest] = []

    @property
    def requests(self) -> tuple[GatewayRequest, ...]:
        return tuple(self._requests)

    @property
    def remaining_turns(self) -> int:
        return len(self._turns) - self._next_turn

    async def stream(self, request: GatewayRequest) -> AsyncIterator[GatewayEvent]:
        self._requests.append(request)
        if self._next_turn >= len(self._turns):
            raise DomainOperationError(
                code="fake_gateway_exhausted",
                message="scripted model gateway received an unexpected request",
                details={"request_count": len(self._requests)},
            )
        turn = self._turns[self._next_turn]
        self._next_turn += 1
        if turn.delay_seconds:
            await asyncio.sleep(turn.delay_seconds)
        if turn.error is not None:
            raise turn.error
        for event in turn.events:
            yield event


class SteppingClock:
    """A deterministic aware clock that advances after every observation."""

    def __init__(
        self,
        start: datetime,
        *,
        step: timedelta = timedelta(milliseconds=1),
    ) -> None:
        self._current = normalize_timestamp(start)
        if step < timedelta(0):
            raise ValueError("clock step may not be negative")
        self._step = step

    def now(self) -> datetime:
        value = self._current
        self._current += self._step
        return value


class SequentialIdGenerator:
    """Deterministic per-instance IDs suitable for event and request assertions."""

    def __init__(self) -> None:
        self._next_value = 1

    def new_id(self, prefix: str) -> str:
        if not prefix:
            raise ValueError("identifier prefix may not be empty")
        value = f"{prefix}-{self._next_value}"
        self._next_value += 1
        return value


__all__ = [
    "ScriptedGatewayTurn",
    "ScriptedModelGateway",
    "SequentialIdGenerator",
    "SteppingClock",
]
