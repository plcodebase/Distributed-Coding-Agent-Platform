"""Shared public configuration and dependency protocols for the agent loop."""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime
from typing import Protocol, Self

from pydantic import Field, model_validator

from agent_core.distributed import (  # noqa: TC001 - Pydantic resolves this field at runtime
    DurablePendingToolCall,
    DurableToolOutcome,
)
from agent_core.domain.base import DomainModel, FrozenJsonObject
from agent_core.domain.models import (  # noqa: TC001 - Pydantic resolves this field at runtime
    IdentifierString,
)
from agent_core.domain.status import ApprovalMode
from agent_core.events import MAX_EVENT_PAYLOAD_BYTES
from agent_core.gateway import (  # noqa: TC001 - Pydantic resolves this field at runtime
    GatewayMessage,
)

MAX_LOOP_VALUE_BYTES = MAX_EVENT_PAYLOAD_BYTES // 2
MAX_GATEWAY_REQUEST_BYTES = 8 * MAX_EVENT_PAYLOAD_BYTES


class Clock(Protocol):
    """Injectable aware clock used for deterministic durable events."""

    def now(self) -> datetime:
        """Return the current timezone-aware timestamp."""


class IdGenerator(Protocol):
    """Injectable durable identifier source."""

    def new_id(self, prefix: str) -> str:
        """Return one non-empty identifier for the requested namespace."""


class UtcClock:
    """Production wall clock returning aware UTC timestamps."""

    def now(self) -> datetime:
        return datetime.now(UTC)


class UuidIdGenerator:
    """Production collision-resistant, namespace-prefixed identifier source."""

    def new_id(self, prefix: str) -> str:
        if not re.fullmatch(r"[a-z][a-z0-9-]{0,31}", prefix):
            raise ValueError("identifier prefix must be bounded canonical text")
        return f"{prefix}-{uuid.uuid4()}"


class AgentLoopConfig(DomainModel):
    """Hard bounds applied to every deterministic agent-loop execution."""

    max_turns: int = Field(default=20, ge=1, le=100)
    max_tool_calls: int = Field(default=50, ge=0, le=100)
    max_semantic_retries: int = Field(default=3, ge=0, le=20)
    max_context_messages: int = Field(default=256, ge=1, le=4096)
    max_gateway_request_bytes: int = Field(
        default=MAX_EVENT_PAYLOAD_BYTES,
        ge=1,
        le=MAX_GATEWAY_REQUEST_BYTES,
    )
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
        ge=4,
        le=MAX_LOOP_VALUE_BYTES,
    )


class AgentLoopInput(DomainModel):
    """Immutable input needed to run one already-leased execution attempt."""

    tenant_id: uuid.UUID
    session_id: uuid.UUID
    run_id: uuid.UUID
    attempt: int = Field(ge=1)
    execution_epoch: int = Field(default=1, ge=1)
    worker_id: IdentifierString
    route_name: IdentifierString
    messages: tuple[GatewayMessage, ...] = Field(min_length=1)
    checkpoint_id: uuid.UUID | None = None
    task_plan: FrozenJsonObject = Field(default_factory=lambda: FrozenJsonObject({}))
    context_summary: str | None = None
    prior_tool_outcomes: tuple[DurableToolOutcome, ...] = Field(
        default=(),
        max_length=100,
    )
    approval_mode: ApprovalMode = ApprovalMode.AUTO_APPROVE
    pending_tool_calls: tuple[DurablePendingToolCall, ...] = Field(default=(), max_length=100)

    @model_validator(mode="after")
    def validate_prior_outcomes(self) -> Self:
        identifiers = [outcome.tool_call_id for outcome in self.prior_tool_outcomes]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("prior durable tool-call IDs must be unique")
        pending = [call.tool_call_id for call in self.pending_tool_calls]
        if len(pending) != len(set(pending)) or set(pending).intersection(identifiers):
            raise ValueError(
                "pending and terminal durable tool-call IDs must be disjoint and unique"
            )
        if (
            self.pending_tool_calls
            and len({call.turn_number for call in self.pending_tool_calls}) != 1
        ):
            raise ValueError("pending durable tool calls must belong to one atomic model turn")
        return self


__all__ = [
    "MAX_GATEWAY_REQUEST_BYTES",
    "MAX_LOOP_VALUE_BYTES",
    "AgentLoopConfig",
    "AgentLoopInput",
    "Clock",
    "IdGenerator",
    "UtcClock",
    "UuidIdGenerator",
]
