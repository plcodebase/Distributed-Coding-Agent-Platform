"""Typed, JSON-serializable events emitted by the agent execution core."""

from __future__ import annotations

import uuid  # noqa: TC003 - Pydantic resolves this field type at runtime
from enum import StrEnum
from typing import Annotated, ClassVar, Literal, Self

from pydantic import Field, StringConstraints, TypeAdapter, model_validator

from agent_core.domain.base import AwareTimestamp, DomainModel, FrozenJsonObject
from agent_core.domain.errors import ErrorDetail
from agent_core.domain.models import IdentifierString, Sha256Hex, ToolName, canonical_argument_hash
from agent_core.domain.status import ToolCallStatus

MAX_EVENT_PAYLOAD_BYTES = 1024 * 1024


class EventType(StrEnum):
    """Stable event names from the design's execution protocol."""

    RUN_STARTED = "run.started"
    CONTEXT_BUILD_STARTED = "context.build_started"
    MODEL_REQUEST_STARTED = "model.request_started"
    MODEL_TEXT_DELTA = "model.text_delta"
    MODEL_TOOL_CALL_RECEIVED = "model.tool_call_received"
    TOOL_APPROVAL_REQUIRED = "tool.approval_required"
    TOOL_STARTED = "tool.started"
    TOOL_STDOUT = "tool.stdout"
    TOOL_STDERR = "tool.stderr"
    TOOL_COMPLETED = "tool.completed"
    CHECKPOINT_CREATED = "checkpoint.created"
    RUN_RETRY_SCHEDULED = "run.retry_scheduled"
    RUN_COMPLETED = "run.completed"
    RUN_FAILED = "run.failed"


class EventPayload(DomainModel):
    """Base for closed-schema event payloads."""


class RunStartedPayload(EventPayload):
    attempt: int = Field(ge=1)
    worker_id: IdentifierString


class ContextBuildStartedPayload(EventPayload):
    message_count: int = Field(ge=0)
    checkpoint_id: uuid.UUID | None = None


class ModelRequestStartedPayload(EventPayload):
    model_call_id: IdentifierString
    request_id: IdentifierString
    route_name: IdentifierString


class ModelTextDeltaPayload(EventPayload):
    model_call_id: IdentifierString
    delta: Annotated[str, StringConstraints(min_length=1)]


class ModelToolCallReceivedPayload(EventPayload):
    model_call_id: IdentifierString
    tool_call_id: IdentifierString
    tool_name: ToolName
    arguments: FrozenJsonObject
    argument_hash: Sha256Hex

    @model_validator(mode="after")
    def validate_argument_hash(self) -> Self:
        if self.argument_hash != canonical_argument_hash(self.arguments):
            raise ValueError("argument_hash does not match canonical arguments")
        return self


class ToolApprovalRequiredPayload(EventPayload):
    tool_call_id: IdentifierString
    tool_name: ToolName
    arguments: FrozenJsonObject
    argument_hash: Sha256Hex
    reason: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]

    @model_validator(mode="after")
    def validate_argument_hash(self) -> Self:
        if self.argument_hash != canonical_argument_hash(self.arguments):
            raise ValueError("argument_hash does not match canonical arguments")
        return self


class ToolStartedPayload(EventPayload):
    tool_call_id: IdentifierString
    tool_name: ToolName


class ToolOutputPayload(EventPayload):
    tool_call_id: IdentifierString
    chunk: Annotated[str, StringConstraints(min_length=1)]
    truncated: bool = False


class ToolCompletedPayload(EventPayload):
    tool_call_id: IdentifierString
    status: Literal[
        ToolCallStatus.COMPLETED,
        ToolCallStatus.FAILED,
        ToolCallStatus.CANCELLED,
    ]
    result: FrozenJsonObject | None = None
    error: ErrorDetail | None = None

    @model_validator(mode="after")
    def validate_outcome(self) -> Self:
        if self.status is ToolCallStatus.COMPLETED and self.error is not None:
            raise ValueError("completed tool event may not contain an error")
        if self.status is ToolCallStatus.FAILED and self.error is None:
            raise ValueError("failed tool event must contain an error")
        return self


class CheckpointCreatedPayload(EventPayload):
    checkpoint_id: uuid.UUID
    message_sequence: int = Field(ge=0)
    workspace_revision: IdentifierString


class RunRetryScheduledPayload(EventPayload):
    attempt: int = Field(ge=1)
    delay_seconds: float = Field(ge=0)
    error: ErrorDetail


class RunCompletedPayload(EventPayload):
    final_text: Annotated[str, StringConstraints(min_length=1)]
    checkpoint_id: uuid.UUID | None = None


class RunFailedPayload(EventPayload):
    error: ErrorDetail


class AgentEvent[PayloadT: EventPayload](DomainModel):
    """Common envelope whose `(run_id, sequence)` pair is the durable key."""

    is_concrete_event: ClassVar[bool] = False

    run_id: uuid.UUID
    sequence: int = Field(ge=1)
    event_type: EventType
    payload: PayloadT
    created_at: AwareTimestamp

    @model_validator(mode="after")
    def validate_concrete_event(self) -> Self:
        if not self.is_concrete_event:
            raise ValueError("AgentEvent must use a concrete event type")
        payload_size = len(self.payload.model_dump_json().encode("utf-8"))
        if payload_size > MAX_EVENT_PAYLOAD_BYTES:
            raise ValueError(
                f"serialized event payload exceeds {MAX_EVENT_PAYLOAD_BYTES}-byte limit"
            )
        return self

    @property
    def event_key(self) -> tuple[uuid.UUID, int]:
        """Return the key later enforced by event-store uniqueness."""

        return self.run_id, self.sequence


class RunStartedEvent(AgentEvent[RunStartedPayload]):
    is_concrete_event = True
    event_type: Literal[EventType.RUN_STARTED] = EventType.RUN_STARTED


class ContextBuildStartedEvent(AgentEvent[ContextBuildStartedPayload]):
    is_concrete_event = True
    event_type: Literal[EventType.CONTEXT_BUILD_STARTED] = EventType.CONTEXT_BUILD_STARTED


class ModelRequestStartedEvent(AgentEvent[ModelRequestStartedPayload]):
    is_concrete_event = True
    event_type: Literal[EventType.MODEL_REQUEST_STARTED] = EventType.MODEL_REQUEST_STARTED


class ModelTextDeltaEvent(AgentEvent[ModelTextDeltaPayload]):
    is_concrete_event = True
    event_type: Literal[EventType.MODEL_TEXT_DELTA] = EventType.MODEL_TEXT_DELTA


class ModelToolCallReceivedEvent(AgentEvent[ModelToolCallReceivedPayload]):
    is_concrete_event = True
    event_type: Literal[EventType.MODEL_TOOL_CALL_RECEIVED] = EventType.MODEL_TOOL_CALL_RECEIVED


class ToolApprovalRequiredEvent(AgentEvent[ToolApprovalRequiredPayload]):
    is_concrete_event = True
    event_type: Literal[EventType.TOOL_APPROVAL_REQUIRED] = EventType.TOOL_APPROVAL_REQUIRED


class ToolStartedEvent(AgentEvent[ToolStartedPayload]):
    is_concrete_event = True
    event_type: Literal[EventType.TOOL_STARTED] = EventType.TOOL_STARTED


class ToolStdoutEvent(AgentEvent[ToolOutputPayload]):
    is_concrete_event = True
    event_type: Literal[EventType.TOOL_STDOUT] = EventType.TOOL_STDOUT


class ToolStderrEvent(AgentEvent[ToolOutputPayload]):
    is_concrete_event = True
    event_type: Literal[EventType.TOOL_STDERR] = EventType.TOOL_STDERR


class ToolCompletedEvent(AgentEvent[ToolCompletedPayload]):
    is_concrete_event = True
    event_type: Literal[EventType.TOOL_COMPLETED] = EventType.TOOL_COMPLETED


class CheckpointCreatedEvent(AgentEvent[CheckpointCreatedPayload]):
    is_concrete_event = True
    event_type: Literal[EventType.CHECKPOINT_CREATED] = EventType.CHECKPOINT_CREATED


class RunRetryScheduledEvent(AgentEvent[RunRetryScheduledPayload]):
    is_concrete_event = True
    event_type: Literal[EventType.RUN_RETRY_SCHEDULED] = EventType.RUN_RETRY_SCHEDULED


class RunCompletedEvent(AgentEvent[RunCompletedPayload]):
    is_concrete_event = True
    event_type: Literal[EventType.RUN_COMPLETED] = EventType.RUN_COMPLETED


class RunFailedEvent(AgentEvent[RunFailedPayload]):
    is_concrete_event = True
    event_type: Literal[EventType.RUN_FAILED] = EventType.RUN_FAILED


type AnyAgentEvent = Annotated[
    RunStartedEvent
    | ContextBuildStartedEvent
    | ModelRequestStartedEvent
    | ModelTextDeltaEvent
    | ModelToolCallReceivedEvent
    | ToolApprovalRequiredEvent
    | ToolStartedEvent
    | ToolStdoutEvent
    | ToolStderrEvent
    | ToolCompletedEvent
    | CheckpointCreatedEvent
    | RunRetryScheduledEvent
    | RunCompletedEvent
    | RunFailedEvent,
    Field(discriminator="event_type"),
]


def parse_agent_event(value: object) -> AnyAgentEvent:
    """Validate an untrusted event mapping into its concrete event class."""

    adapter: TypeAdapter[AnyAgentEvent] = TypeAdapter(AnyAgentEvent)
    return adapter.validate_python(value)


__all__ = [
    "MAX_EVENT_PAYLOAD_BYTES",
    "AgentEvent",
    "AnyAgentEvent",
    "CheckpointCreatedEvent",
    "CheckpointCreatedPayload",
    "ContextBuildStartedEvent",
    "ContextBuildStartedPayload",
    "ErrorDetail",
    "EventPayload",
    "EventType",
    "ModelRequestStartedEvent",
    "ModelRequestStartedPayload",
    "ModelTextDeltaEvent",
    "ModelTextDeltaPayload",
    "ModelToolCallReceivedEvent",
    "ModelToolCallReceivedPayload",
    "RunCompletedEvent",
    "RunCompletedPayload",
    "RunFailedEvent",
    "RunFailedPayload",
    "RunRetryScheduledEvent",
    "RunRetryScheduledPayload",
    "RunStartedEvent",
    "RunStartedPayload",
    "ToolApprovalRequiredEvent",
    "ToolApprovalRequiredPayload",
    "ToolCompletedEvent",
    "ToolCompletedPayload",
    "ToolOutputPayload",
    "ToolStartedEvent",
    "ToolStartedPayload",
    "ToolStderrEvent",
    "ToolStdoutEvent",
    "parse_agent_event",
]
