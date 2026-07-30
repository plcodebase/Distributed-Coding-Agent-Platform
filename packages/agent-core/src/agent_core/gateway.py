"""Provider-neutral model gateway contracts consumed by the agent loop."""

from __future__ import annotations

import uuid  # noqa: TC003 - Pydantic resolves this field type at runtime
from enum import StrEnum
from typing import TYPE_CHECKING, Annotated, Literal, Protocol, Self

from pydantic import Field, StringConstraints, TypeAdapter, model_validator

from agent_core.domain.base import DomainModel, FrozenJsonObject
from agent_core.domain.errors import ErrorDetail  # noqa: TC001 - Pydantic resolves at runtime
from agent_core.domain.models import (  # noqa: TC001 - Pydantic resolves these at runtime
    IdentifierString,
    NonEmptyString,
    ToolName,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

MAX_GATEWAY_MESSAGES = 4096
MAX_GATEWAY_TOOLS = 1000
type GatewayRequestIdentifier = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=255,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    ),
]


class MessageRole(StrEnum):
    """Normalized conversation roles accepted by every gateway adapter."""

    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"
    TOOL = "tool"


class GatewayToolDefinition(DomainModel):
    """A tool schema advertised to the model without an executable handler."""

    name: ToolName
    description: NonEmptyString
    input_schema: FrozenJsonObject


class GatewayToolCall(DomainModel):
    """One complete, normalized tool call proposed by a model."""

    id: IdentifierString
    name: ToolName
    arguments: FrozenJsonObject


class GatewayMessage(DomainModel):
    """A provider-neutral conversation message used for iterative model turns."""

    role: MessageRole
    content: str = ""
    tool_call_id: IdentifierString | None = None
    tool_calls: tuple[GatewayToolCall, ...] = ()

    @model_validator(mode="after")
    def validate_role_fields(self) -> Self:
        if self.role in {MessageRole.SYSTEM, MessageRole.USER}:
            if not self.content:
                raise ValueError(f"{self.role.value} message must contain content")
            if self.tool_call_id is not None or self.tool_calls:
                raise ValueError(f"{self.role.value} message may not contain tool-call fields")
        elif self.role is MessageRole.ASSISTANT:
            if not self.content and not self.tool_calls:
                raise ValueError("assistant message must contain content or tool calls")
            if self.tool_call_id is not None:
                raise ValueError("assistant message may not contain tool_call_id")
        elif self.role is MessageRole.TOOL:
            if self.tool_call_id is None:
                raise ValueError("tool message must contain tool_call_id")
            if not self.content:
                raise ValueError("tool message must contain content")
            if self.tool_calls:
                raise ValueError("tool message may not contain tool_calls")
        return self


class GatewayRequest(DomainModel):
    """One logical reasoning request routed through the centralized gateway."""

    tenant_id: uuid.UUID
    session_id: uuid.UUID
    run_id: uuid.UUID
    turn_number: int = Field(ge=1, le=100)
    model_call_id: GatewayRequestIdentifier
    request_id: GatewayRequestIdentifier
    route_name: IdentifierString
    messages: tuple[GatewayMessage, ...] = Field(
        min_length=1,
        max_length=MAX_GATEWAY_MESSAGES,
    )
    tools: tuple[GatewayToolDefinition, ...] = Field(
        default=(),
        max_length=MAX_GATEWAY_TOOLS,
    )

    @model_validator(mode="after")
    def validate_unique_tools(self) -> Self:
        names = [tool.name for tool in self.tools]
        if len(names) != len(set(names)):
            raise ValueError("gateway tool names must be unique")
        return self


class GatewayEventKind(StrEnum):
    """Discriminator values for normalized model-stream events."""

    TEXT_DELTA = "text_delta"
    TOOL_CALL = "tool_call"
    INVALID_TOOL_CALL = "invalid_tool_call"
    RESPONSE_COMPLETED = "response_completed"


class GatewayTextDelta(DomainModel):
    """A non-empty assistant text fragment."""

    kind: Literal[GatewayEventKind.TEXT_DELTA] = GatewayEventKind.TEXT_DELTA
    delta: Annotated[str, StringConstraints(min_length=1)]


class GatewayToolCallEvent(DomainModel):
    """A complete model tool call ready for core-level schema validation."""

    kind: Literal[GatewayEventKind.TOOL_CALL] = GatewayEventKind.TOOL_CALL
    tool_call: GatewayToolCall


class GatewayInvalidToolCallEvent(DomainModel):
    """A provider call whose raw arguments could not be normalized safely."""

    kind: Literal[GatewayEventKind.INVALID_TOOL_CALL] = GatewayEventKind.INVALID_TOOL_CALL
    tool_call_id: IdentifierString
    tool_name: ToolName
    error: ErrorDetail


class GatewayFinishReason(StrEnum):
    """Provider-normalized response termination reasons."""

    STOP = "stop"
    TOOL_CALLS = "tool_calls"
    LENGTH = "length"
    CONTENT_FILTER = "content_filter"


class GatewayResponseCompleted(DomainModel):
    """The terminal event for one successful gateway stream."""

    kind: Literal[GatewayEventKind.RESPONSE_COMPLETED] = GatewayEventKind.RESPONSE_COMPLETED
    finish_reason: GatewayFinishReason
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    cached_tokens: int = Field(default=0, ge=0)
    provider: IdentifierString | None = None
    model: IdentifierString | None = None


type GatewayEvent = Annotated[
    GatewayTextDelta
    | GatewayToolCallEvent
    | GatewayInvalidToolCallEvent
    | GatewayResponseCompleted,
    Field(discriminator="kind"),
]

_GATEWAY_EVENT_ADAPTER: TypeAdapter[GatewayEvent] = TypeAdapter(GatewayEvent)


def parse_gateway_event(value: object) -> GatewayEvent:
    """Validate an untrusted adapter value into one normalized stream event."""

    return _GATEWAY_EVENT_ADAPTER.validate_python(value)


class ModelGateway(Protocol):
    """The only model access boundary available to the agent loop."""

    def stream(self, request: GatewayRequest) -> AsyncIterator[GatewayEvent]:
        """Stream one normalized response from the configured model route."""


__all__ = [
    "MAX_GATEWAY_MESSAGES",
    "MAX_GATEWAY_TOOLS",
    "GatewayEvent",
    "GatewayEventKind",
    "GatewayFinishReason",
    "GatewayInvalidToolCallEvent",
    "GatewayMessage",
    "GatewayRequest",
    "GatewayRequestIdentifier",
    "GatewayResponseCompleted",
    "GatewayTextDelta",
    "GatewayToolCall",
    "GatewayToolCallEvent",
    "GatewayToolDefinition",
    "MessageRole",
    "ModelGateway",
    "parse_gateway_event",
]
