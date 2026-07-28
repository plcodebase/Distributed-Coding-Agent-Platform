from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import ValidationError

from agent_core.domain import (
    ApprovalMode,
    Checkpoint,
    DomainOperationError,
    InvalidRunTransitionError,
    JsonObject,
    ModelCall,
    ModelCallStatus,
    Run,
    RunStatus,
    Session,
    SessionStatus,
    ToolCall,
    ToolCallStatus,
    allowed_run_transitions,
    canonical_argument_hash,
    transition_run,
)

NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)
RUN_ID = UUID("10000000-0000-0000-0000-000000000001")
SESSION_ID = UUID("20000000-0000-0000-0000-000000000002")
TENANT_ID = UUID("30000000-0000-0000-0000-000000000003")
WORKSPACE_ID = UUID("40000000-0000-0000-0000-000000000004")
CHECKPOINT_ID = UUID("50000000-0000-0000-0000-000000000005")


def assign_attribute(target: object, name: str, value: object) -> None:
    setattr(target, name, value)


def queued_run() -> Run:
    return Run(
        id=RUN_ID,
        session_id=SESSION_ID,
        workspace_id=WORKSPACE_ID,
        status=RunStatus.QUEUED,
        priority=10,
        attempt=1,
        created_at=NOW,
    )


def running_run() -> Run:
    leased = transition_run(
        queued_run(),
        RunStatus.LEASED,
        occurred_at=NOW + timedelta(seconds=1),
        worker_id="worker-1",
        lease_expires_at=NOW + timedelta(seconds=30),
    )
    return transition_run(
        leased,
        RunStatus.RUNNING,
        occurred_at=NOW + timedelta(seconds=2),
    )


def test_session_is_closed_immutable_and_normalizes_timestamps() -> None:
    session = Session(
        id=SESSION_ID,
        tenant_id=TENANT_ID,
        workspace_id=WORKSPACE_ID,
        status=SessionStatus.ACTIVE,
        approval_mode=ApprovalMode.ON_REQUEST,
        model_route=" coding-default ",
        created_at="2026-07-28T05:00:00-07:00",
        updated_at="2026-07-28T12:01:00Z",
    )

    assert session.created_at == NOW
    assert session.model_route == "coding-default"
    with pytest.raises(ValidationError, match="frozen"):
        assign_attribute(session, "status", SessionStatus.COMPLETED)
    with pytest.raises(ValidationError, match="Extra inputs"):
        Session.model_validate({**session.model_dump(), "unexpected": True})


def test_session_rejects_naive_and_reversed_timestamps() -> None:
    values = {
        "id": SESSION_ID,
        "tenant_id": TENANT_ID,
        "workspace_id": WORKSPACE_ID,
        "status": SessionStatus.ACTIVE,
        "approval_mode": ApprovalMode.ALWAYS,
        "model_route": "coding-default",
        "created_at": NOW,
        "updated_at": NOW,
    }

    with pytest.raises(ValidationError, match="timezone"):
        Session.model_validate(
            {**values, "created_at": datetime(2026, 7, 28, 12)}  # noqa: DTZ001
        )
    with pytest.raises(ValidationError, match="may not precede"):
        Session.model_validate({**values, "updated_at": NOW - timedelta(seconds=1)})


def test_run_follows_happy_path_and_sets_lifecycle_fields() -> None:
    leased = transition_run(
        queued_run(),
        RunStatus.LEASED,
        occurred_at=NOW + timedelta(seconds=1),
        worker_id=" worker-1 ",
        lease_expires_at=NOW + timedelta(seconds=30),
    )
    running = transition_run(
        leased,
        RunStatus.RUNNING,
        occurred_at=NOW + timedelta(seconds=2),
    )
    waiting = transition_run(
        running,
        RunStatus.WAITING_APPROVAL,
        occurred_at=NOW + timedelta(seconds=3),
    )
    resumed = transition_run(
        waiting,
        RunStatus.RUNNING,
        occurred_at=NOW + timedelta(seconds=4),
    )
    retrying = transition_run(
        resumed,
        RunStatus.RETRY_PENDING,
        occurred_at=NOW + timedelta(seconds=5),
    )
    retried = transition_run(
        retrying,
        RunStatus.RUNNING,
        occurred_at=NOW + timedelta(seconds=6),
    )
    completed = transition_run(
        retried,
        RunStatus.COMPLETED,
        occurred_at=NOW + timedelta(seconds=7),
    )

    assert leased.assigned_worker_id == "worker-1"
    assert running.started_at == NOW + timedelta(seconds=2)
    assert resumed.started_at == running.started_at
    assert completed.completed_at == NOW + timedelta(seconds=7)
    assert completed.assigned_worker_id == "worker-1"
    assert completed.lease_expires_at is None
    assert allowed_run_transitions(completed.status) == frozenset()


def test_invalid_transition_has_stable_structured_error() -> None:
    with pytest.raises(InvalidRunTransitionError) as captured:
        transition_run(
            queued_run(),
            RunStatus.COMPLETED,
            occurred_at=NOW + timedelta(seconds=1),
        )

    error = captured.value
    assert error.code == "invalid_run_transition"
    assert error.as_dict() == {
        "code": "invalid_run_transition",
        "message": "run cannot transition from queued to completed",
        "details": {
            "current_status": "queued",
            "requested_status": "completed",
            "allowed_statuses": ["cancelled", "leased"],
        },
    }


@pytest.mark.parametrize(
    ("worker_id", "lease_expires_at"),
    [
        (None, NOW + timedelta(seconds=30)),
        ("", NOW + timedelta(seconds=30)),
        ("worker-1", None),
        ("worker-1", NOW),
    ],
)
def test_lease_transition_requires_complete_future_lease(
    worker_id: str | None,
    lease_expires_at: datetime | None,
) -> None:
    with pytest.raises(DomainOperationError) as captured:
        transition_run(
            queued_run(),
            RunStatus.LEASED,
            occurred_at=NOW,
            worker_id=worker_id,
            lease_expires_at=lease_expires_at,
        )

    assert captured.value.code == "invalid_run_lease"
    assert captured.value.as_dict()["details"] == {"run_id": str(RUN_ID)}


def test_transition_rejects_invalid_or_reversed_timestamps_with_structured_error() -> None:
    with pytest.raises(DomainOperationError) as naive_transition:
        transition_run(
            queued_run(),
            RunStatus.LEASED,
            occurred_at=datetime(2026, 7, 28, 12),  # noqa: DTZ001
            worker_id="worker-1",
            lease_expires_at=NOW + timedelta(seconds=30),
        )
    assert naive_transition.value.code == "invalid_run_transition_timestamp"

    with pytest.raises(DomainOperationError) as reversed_transition:
        transition_run(
            queued_run(),
            RunStatus.LEASED,
            occurred_at=NOW - timedelta(seconds=1),
            worker_id="worker-1",
            lease_expires_at=NOW + timedelta(seconds=30),
        )
    assert reversed_transition.value.code == "invalid_run_transition_timestamp"

    with pytest.raises(DomainOperationError) as naive_lease:
        transition_run(
            queued_run(),
            RunStatus.LEASED,
            occurred_at=NOW,
            worker_id="worker-1",
            lease_expires_at=datetime(2026, 7, 28, 12, 1),  # noqa: DTZ001
        )
    assert naive_lease.value.code == "invalid_run_lease"


def test_lost_run_requeues_with_new_attempt_and_no_lease() -> None:
    lost = transition_run(
        running_run(),
        RunStatus.LOST,
        occurred_at=NOW + timedelta(seconds=3),
    )
    requeued = transition_run(
        lost,
        RunStatus.QUEUED,
        occurred_at=NOW + timedelta(seconds=4),
    )

    assert lost.assigned_worker_id == "worker-1"
    assert lost.lease_expires_at is None
    assert requeued.attempt == 2
    assert requeued.assigned_worker_id is None
    assert requeued.lease_expires_at is None


def test_run_model_rejects_inconsistent_persisted_state() -> None:
    values = queued_run().model_dump()

    with pytest.raises(ValidationError, match="assigned_worker_id"):
        Run.model_validate({**values, "status": RunStatus.RUNNING, "started_at": NOW})
    with pytest.raises(ValidationError, match="must have completed_at"):
        Run.model_validate(
            {
                **values,
                "status": RunStatus.CANCELLED,
                "started_at": NOW,
            }
        )
    with pytest.raises(ValidationError, match="may not retain"):
        Run.model_validate({**values, "assigned_worker_id": "worker-1"})


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"started_at": NOW - timedelta(seconds=1)}, "started_at may not precede"),
        (
            {
                "status": RunStatus.CANCELLED,
                "completed_at": NOW - timedelta(seconds=1),
            },
            "completed_at may not precede",
        ),
        (
            {
                "status": RunStatus.RUNNING,
                "assigned_worker_id": "worker-1",
                "lease_expires_at": NOW + timedelta(seconds=30),
            },
            "must have started_at",
        ),
        (
            {"status": RunStatus.LEASED, "assigned_worker_id": "worker-1"},
            "must have lease_expires_at",
        ),
        ({"completed_at": NOW + timedelta(seconds=1)}, "non-terminal"),
    ],
)
def test_run_model_rejects_temporal_and_lease_invariants(
    updates: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        Run.model_validate({**queued_run().model_dump(), **updates})


def test_cancellation_marks_request_and_completion() -> None:
    cancelled = transition_run(
        queued_run(),
        RunStatus.CANCELLED,
        occurred_at=NOW + timedelta(seconds=1),
    )

    assert cancelled.cancellation_requested is True
    assert cancelled.completed_at == NOW + timedelta(seconds=1)


def test_tool_call_hash_is_canonical_and_validated() -> None:
    arguments: JsonObject = {
        "path": "src/main.py",
        "replacements": [{"old": "a", "new": "b"}],
    }
    reordered: JsonObject = {
        "replacements": [{"new": "b", "old": "a"}],
        "path": "src/main.py",
    }
    argument_hash = canonical_argument_hash(arguments)

    assert argument_hash == canonical_argument_hash(reordered)
    call = ToolCall(
        id="call-stable-1",
        run_id=RUN_ID,
        turn_number=1,
        tool_name="edit_file",
        arguments=arguments,
        argument_hash=argument_hash,
        status=ToolCallStatus.RECEIVED,
    )
    assert call.model_dump()["arguments"] == arguments
    assert ToolCall.model_validate_json(call.model_dump_json()) == call
    with pytest.raises(TypeError, match="does not support item assignment"):
        call.arguments["path"] = "mutated.py"

    with pytest.raises(ValidationError, match="does not match"):
        ToolCall.model_validate({**call.model_dump(), "argument_hash": "0" * 64})
    with pytest.raises(ValidationError, match="string_pattern_mismatch"):
        ToolCall.model_validate({**call.model_dump(), "tool_name": "../edit"})


def test_tool_call_validates_execution_timestamps() -> None:
    arguments: JsonObject = {"command": ["pytest"]}
    values = {
        "id": "call-stable-2",
        "run_id": RUN_ID,
        "turn_number": 2,
        "tool_name": "run_command",
        "arguments": arguments,
        "argument_hash": canonical_argument_hash(arguments),
        "status": ToolCallStatus.COMPLETED,
        "started_at": NOW,
        "completed_at": NOW + timedelta(seconds=1),
        "result": {"exit_code": 0},
    }

    completed = ToolCall.model_validate(values)
    assert completed.result == {"exit_code": 0}
    with pytest.raises(ValidationError, match="must have started_at"):
        ToolCall.model_validate({**values, "started_at": None})
    with pytest.raises(ValidationError, match="may not precede"):
        ToolCall.model_validate({**values, "completed_at": NOW - timedelta(seconds=1)})
    with pytest.raises(ValidationError, match="must have completed_at"):
        ToolCall.model_validate({**values, "completed_at": None})
    with pytest.raises(ValidationError, match="non-terminal"):
        ToolCall.model_validate(
            {
                **values,
                "status": ToolCallStatus.RECEIVED,
            }
        )


def test_checkpoint_preserves_recovery_identifiers_and_json_plan() -> None:
    checkpoint = Checkpoint(
        id=CHECKPOINT_ID,
        run_id=RUN_ID,
        session_id=SESSION_ID,
        message_sequence=3,
        workspace_snapshot_uri="s3://agent-platform/checkpoints/5",
        workspace_revision="abc123",
        task_plan={"steps": [{"title": "test", "completed": False}]},
        context_summary=None,
        created_at=NOW,
    )

    assert checkpoint.workspace_revision == "abc123"
    assert checkpoint.model_dump()["task_plan"]["steps"] == [{"title": "test", "completed": False}]


def test_model_call_validates_accounting_and_temporal_state() -> None:
    values = {
        "id": "model-call-1",
        "run_id": RUN_ID,
        "request_id": "request-1",
        "route_name": "coding-default",
        "provider": "fake",
        "model": "fake-primary",
        "status": ModelCallStatus.COMPLETED,
        "input_tokens": 10,
        "output_tokens": 4,
        "cached_tokens": 2,
        "estimated_cost_usd": Decimal("0.001"),
        "retry_count": 0,
        "fallback_count": 0,
        "started_at": NOW,
        "first_token_at": NOW + timedelta(milliseconds=50),
        "completed_at": NOW + timedelta(milliseconds=100),
    }

    call = ModelCall.model_validate(values)
    assert call.estimated_cost_usd == Decimal("0.001")
    with pytest.raises(ValidationError, match="must have first_token_at"):
        ModelCall.model_validate(
            {
                **values,
                "status": ModelCallStatus.STREAMING,
                "first_token_at": None,
                "completed_at": None,
            }
        )
    with pytest.raises(ValidationError, match="greater than or equal"):
        ModelCall.model_validate({**values, "input_tokens": -1})


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        (
            {"first_token_at": NOW - timedelta(milliseconds=1)},
            "first_token_at may not precede",
        ),
        (
            {"completed_at": NOW - timedelta(milliseconds=1)},
            "completed_at may not precede",
        ),
        (
            {
                "first_token_at": NOW + timedelta(milliseconds=200),
                "completed_at": NOW + timedelta(milliseconds=100),
            },
            "first_token_at may not follow",
        ),
        ({"completed_at": None}, "must have completed_at"),
        (
            {
                "status": ModelCallStatus.STARTED,
                "first_token_at": None,
            },
            "non-terminal",
        ),
    ],
)
def test_model_call_rejects_inconsistent_persisted_lifecycle(
    updates: dict[str, object],
    message: str,
) -> None:
    values = {
        "id": "model-call-invalid",
        "run_id": RUN_ID,
        "request_id": "request-invalid",
        "route_name": "coding-default",
        "status": ModelCallStatus.COMPLETED,
        "retry_count": 0,
        "fallback_count": 0,
        "started_at": NOW,
        "first_token_at": NOW + timedelta(milliseconds=50),
        "completed_at": NOW + timedelta(milliseconds=100),
    }
    with pytest.raises(ValidationError, match=message):
        ModelCall.model_validate({**values, **updates})
