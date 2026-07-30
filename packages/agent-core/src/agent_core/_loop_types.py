"""Shared public configuration and dependency protocols for the agent loop."""

from __future__ import annotations

import uuid  # noqa: TC003 - Pydantic resolves this field type at runtime
from typing import TYPE_CHECKING, Protocol

from pydantic import Field

from agent_core.domain.base import DomainModel, FrozenJsonObject
from agent_core.domain.models import (  # noqa: TC001 - Pydantic resolves this field at runtime
    IdentifierString,
)
from agent_core.events import MAX_EVENT_PAYLOAD_BYTES
from agent_core.gateway import (  # noqa: TC001 - Pydantic resolves this field at runtime
    GatewayMessage,
)

if TYPE_CHECKING:
    from datetime import datetime

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
    worker_id: IdentifierString
    route_name: IdentifierString
    messages: tuple[GatewayMessage, ...] = Field(min_length=1)
    checkpoint_id: uuid.UUID | None = None
    task_plan: FrozenJsonObject = Field(default_factory=lambda: FrozenJsonObject({}))
    context_summary: str | None = None


__all__ = [
    "MAX_GATEWAY_REQUEST_BYTES",
    "MAX_LOOP_VALUE_BYTES",
    "AgentLoopConfig",
    "AgentLoopInput",
    "Clock",
    "IdGenerator",
]
