from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from agent_core.domain import JsonObject, ToolCallStatus, canonical_argument_hash
from agent_core.events import (
    AgentEvent,
    CheckpointCreatedEvent,
    ContextBuildStartedEvent,
    ErrorDetail,
    EventType,
    ModelRequestStartedEvent,
    ModelTextDeltaEvent,
    ModelToolCallReceivedEvent,
    RunCompletedEvent,
    RunFailedEvent,
    RunRetryScheduledEvent,
    RunStartedEvent,
    RunStartedPayload,
    ToolApprovalRequiredEvent,
    ToolCompletedEvent,
    ToolStartedEvent,
    ToolStderrEvent,
    ToolStdoutEvent,
    parse_agent_event,
)

NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)
RUN_ID = UUID("10000000-0000-0000-0000-000000000001")
CHECKPOINT_ID = UUID("50000000-0000-0000-0000-000000000005")
ARGUMENTS: JsonObject = {"path": "src/main.py"}
ARGUMENT_HASH = canonical_argument_hash(ARGUMENTS)


def assign_attribute(target: object, name: str, value: object) -> None:
    setattr(target, name, value)


EVENT_CASES = [
    (
        EventType.RUN_STARTED,
        {"attempt": 1, "worker_id": "worker-1"},
        RunStartedEvent,
    ),
    (
        EventType.CONTEXT_BUILD_STARTED,
        {"message_count": 2, "checkpoint_id": None},
        ContextBuildStartedEvent,
    ),
    (
        EventType.MODEL_REQUEST_STARTED,
        {
            "model_call_id": "model-call-1",
            "request_id": "request-1",
            "route_name": "coding-default",
        },
        ModelRequestStartedEvent,
    ),
    (
        EventType.MODEL_TEXT_DELTA,
        {"model_call_id": "model-call-1", "delta": "hello"},
        ModelTextDeltaEvent,
    ),
    (
        EventType.MODEL_TOOL_CALL_RECEIVED,
        {
            "model_call_id": "model-call-1",
            "tool_call_id": "tool-call-1",
            "tool_name": "read_file",
            "arguments": ARGUMENTS,
            "argument_hash": ARGUMENT_HASH,
        },
        ModelToolCallReceivedEvent,
    ),
    (
        EventType.TOOL_APPROVAL_REQUIRED,
        {
            "tool_call_id": "tool-call-1",
            "tool_name": "read_file",
            "arguments": ARGUMENTS,
            "argument_hash": ARGUMENT_HASH,
            "reason": "Session policy requires approval",
        },
        ToolApprovalRequiredEvent,
    ),
    (
        EventType.TOOL_STARTED,
        {"tool_call_id": "tool-call-1", "tool_name": "read_file"},
        ToolStartedEvent,
    ),
    (
        EventType.TOOL_STDOUT,
        {"tool_call_id": "tool-call-1", "chunk": "output", "truncated": False},
        ToolStdoutEvent,
    ),
    (
        EventType.TOOL_STDERR,
        {"tool_call_id": "tool-call-1", "chunk": "warning", "truncated": False},
        ToolStderrEvent,
    ),
    (
        EventType.TOOL_COMPLETED,
        {
            "tool_call_id": "tool-call-1",
            "status": ToolCallStatus.COMPLETED,
            "result": {"content": "source"},
            "error": None,
        },
        ToolCompletedEvent,
    ),
    (
        EventType.CHECKPOINT_CREATED,
        {
            "checkpoint_id": str(CHECKPOINT_ID),
            "message_sequence": 3,
            "workspace_revision": "abc123",
        },
        CheckpointCreatedEvent,
    ),
    (
        EventType.RUN_RETRY_SCHEDULED,
        {
            "attempt": 2,
            "delay_seconds": 0.5,
            "error": {
                "code": "gateway_timeout",
                "message": "gateway timed out",
                "retryable": True,
            },
        },
        RunRetryScheduledEvent,
    ),
    (
        EventType.RUN_COMPLETED,
        {"final_text": "Implemented the change.", "checkpoint_id": str(CHECKPOINT_ID)},
        RunCompletedEvent,
    ),
    (
        EventType.RUN_FAILED,
        {
            "error": {
                "code": "turn_limit",
                "message": "maximum turn count reached",
                "retryable": False,
            }
        },
        RunFailedEvent,
    ),
]


@pytest.mark.parametrize(
    ("event_type", "payload", "event_class"),
    EVENT_CASES,
    ids=[case[0].value for case in EVENT_CASES],
)
def test_parse_every_design_event_into_concrete_type(
    event_type: EventType,
    payload: dict[str, object],
    event_class: type[object],
) -> None:
    event = parse_agent_event(
        {
            "run_id": str(RUN_ID),
            "sequence": 1,
            "event_type": event_type.value,
            "payload": payload,
            "created_at": "2026-07-28T05:00:00-07:00",
        }
    )

    assert isinstance(event, event_class)
    assert event.event_type is event_type
    assert event.event_key == (RUN_ID, 1)
    assert event.created_at == NOW
    assert event.model_dump(mode="json")["event_type"] == event_type.value
    assert parse_agent_event(event.model_dump(mode="json")) == event


def test_event_envelope_rejects_unknown_type_sequence_and_naive_timestamp() -> None:
    base = {
        "run_id": RUN_ID,
        "sequence": 1,
        "event_type": EventType.RUN_STARTED,
        "payload": {"attempt": 1, "worker_id": "worker-1"},
        "created_at": NOW,
    }

    with pytest.raises(ValidationError, match="union_tag_invalid"):
        parse_agent_event({**base, "event_type": "run.unknown"})
    with pytest.raises(ValidationError, match="greater than or equal"):
        parse_agent_event({**base, "sequence": 0})
    with pytest.raises(ValidationError, match="timezone"):
        parse_agent_event(
            {**base, "created_at": datetime(2026, 7, 28, 12)}  # noqa: DTZ001
        )
    with pytest.raises(ValidationError, match="concrete event type"):
        AgentEvent[RunStartedPayload].model_validate(base)


def test_event_payload_rejects_extra_or_mismatched_fields() -> None:
    payload: dict[str, object] = {
        "model_call_id": "model-call-1",
        "tool_call_id": "tool-call-1",
        "tool_name": "read_file",
        "arguments": ARGUMENTS,
        "argument_hash": ARGUMENT_HASH,
    }
    base = {
        "run_id": RUN_ID,
        "sequence": 1,
        "event_type": EventType.MODEL_TOOL_CALL_RECEIVED,
        "payload": payload,
        "created_at": NOW,
    }

    with pytest.raises(ValidationError, match="Extra inputs"):
        parse_agent_event({**base, "payload": {**payload, "provider_specific": "value"}})
    with pytest.raises(ValidationError, match="does not match"):
        parse_agent_event({**base, "payload": {**payload, "argument_hash": "0" * 64}})

    with pytest.raises(ValidationError, match="does not match"):
        parse_agent_event(
            {
                **base,
                "event_type": EventType.TOOL_APPROVAL_REQUIRED,
                "payload": {
                    "tool_call_id": "tool-call-1",
                    "tool_name": "read_file",
                    "arguments": ARGUMENTS,
                    "argument_hash": "0" * 64,
                    "reason": "Session policy requires approval",
                },
            }
        )


def test_tool_completed_event_enforces_structured_failure() -> None:
    base = {
        "run_id": RUN_ID,
        "sequence": 4,
        "event_type": EventType.TOOL_COMPLETED,
        "created_at": NOW,
    }

    with pytest.raises(ValidationError, match="must contain an error"):
        parse_agent_event(
            {
                **base,
                "payload": {
                    "tool_call_id": "tool-call-1",
                    "status": ToolCallStatus.FAILED,
                    "result": None,
                },
            }
        )
    with pytest.raises(ValidationError, match="may not contain an error"):
        parse_agent_event(
            {
                **base,
                "payload": {
                    "tool_call_id": "tool-call-1",
                    "status": ToolCallStatus.COMPLETED,
                    "error": {
                        "code": "unexpected",
                        "message": "should not be present",
                    },
                },
            }
        )


def test_event_and_nested_error_are_immutable() -> None:
    event = RunFailedEvent(
        run_id=RUN_ID,
        sequence=9,
        payload={
            "error": {
                "code": "turn_limit",
                "message": "maximum turn count reached",
            }
        },
        created_at=NOW,
    )

    assert isinstance(event.payload.error, ErrorDetail)
    with pytest.raises(ValidationError, match="frozen"):
        assign_attribute(event, "sequence", 10)
    with pytest.raises(ValidationError, match="frozen"):
        assign_attribute(event.payload.error, "retryable", True)
