from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from agent_core.control import (
    ApprovalDecision,
    ApprovalStatus,
    PersistedApproval,
    RunSubmission,
    TaskStatus,
    TrackedTask,
    run_creation_hash,
)
from agent_core.domain.base import FrozenJsonObject
from agent_core.domain.models import Run
from agent_core.domain.status import RunStatus
from agent_core.event_store import MAX_EVENT_PAGE_SIZE, EventDraft, EventPage, StoredEvent
from agent_core.scheduling import RunPriorityClass
from agent_core.workspace_access import WorkspaceFileReference


def test_run_creation_hash_is_canonical_and_payload_sensitive() -> None:
    assert run_creation_hash(priority=0) == run_creation_hash(priority=0)
    assert run_creation_hash(priority=0) != run_creation_hash(priority=1)
    assert run_creation_hash(priority=0) != run_creation_hash(
        priority=0,
        priority_class=RunPriorityClass.BACKGROUND,
    )
    assert len(run_creation_hash(priority=0)) == 64

    task = TrackedTask(id="inspect", title="Inspect the repository")
    submission_hash = run_creation_hash(
        priority=0,
        task="Fix the failing test.",
        initial_tasks=(task,),
    )
    assert submission_hash == run_creation_hash(
        priority=0,
        task="Fix the failing test.",
        initial_tasks=(task,),
    )
    assert submission_hash != run_creation_hash(
        priority=0,
        task="Fix a different test.",
        initial_tasks=(task,),
    )
    referenced = (WorkspaceFileReference(path="docs/plan.md"),)
    assert submission_hash != run_creation_hash(
        priority=0,
        task="Fix the failing test.",
        initial_tasks=(task,),
        referenced_files=referenced,
    )
    assert run_creation_hash(
        priority=0,
        referenced_files=(
            WorkspaceFileReference(path="a.txt"),
            WorkspaceFileReference(path="b.txt"),
        ),
    ) != run_creation_hash(
        priority=0,
        referenced_files=(
            WorkspaceFileReference(path="b.txt"),
            WorkspaceFileReference(path="a.txt"),
        ),
    )


def test_run_submission_validates_task_bytes_and_initial_plan() -> None:
    now = datetime.now(UTC)
    run = Run(
        id=uuid.uuid4(),
        session_id=uuid.uuid4(),
        workspace_id=uuid.uuid4(),
        status=RunStatus.QUEUED,
        priority=0,
        attempt=1,
        created_at=now,
    )
    submission = RunSubmission(
        run=run,
        task=" Fix the repository. ",
        initial_tasks=(
            TrackedTask(
                id="inspect",
                title="Inspect",
                status=TaskStatus.IN_PROGRESS,
            ),
        ),
    )
    assert submission.task == "Fix the repository."

    with pytest.raises(ValidationError):
        RunSubmission(run=run, task="🙂" * 20_000)
    with pytest.raises(ValidationError):
        RunSubmission(run=run, task=" ")


def test_run_submission_validates_canonical_unique_workspace_references() -> None:
    run = Run(
        id=uuid.uuid4(),
        session_id=uuid.uuid4(),
        workspace_id=uuid.uuid4(),
        status=RunStatus.QUEUED,
        priority=0,
        attempt=1,
        created_at=datetime.now(UTC),
    )
    submission = RunSubmission(
        run=run,
        task="Use the supplied context.",
        referenced_files=(WorkspaceFileReference(path="docs//plan.md"),),
    )
    assert submission.referenced_files[0].path == "docs/plan.md"

    for invalid in ("/etc/passwd", "../secret", ".", ".GIT/config"):
        with pytest.raises(ValidationError):
            WorkspaceFileReference(path=invalid)
    with pytest.raises(ValidationError):
        RunSubmission(
            run=run,
            task="Use the supplied context.",
            referenced_files=(
                WorkspaceFileReference(path="docs/plan.md"),
                WorkspaceFileReference(path="docs/plan.md"),
            ),
        )


def test_approval_decision_requires_aware_time_and_closed_schema() -> None:
    now = datetime.now(UTC)
    decision = ApprovalDecision(approved=True, decided_by="operator", decided_at=now)
    assert decision.decided_at == now

    with pytest.raises(ValidationError):
        ApprovalDecision(
            approved=True,
            decided_by="operator",
            decided_at=now.replace(tzinfo=None),
        )
    with pytest.raises(ValidationError):
        ApprovalDecision.model_validate(
            {
                "approved": True,
                "decided_by": "operator",
                "decided_at": now,
                "extra": True,
            }
        )


def test_persisted_approval_lifecycle_is_consistent() -> None:
    now = datetime.now(UTC)
    pending = PersistedApproval(
        id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        status=ApprovalStatus.PENDING,
        reason="sensitive command",
        arguments=FrozenJsonObject({}),
        requested_at=now,
    )
    assert pending.decided_at is None

    approved = pending.model_copy(
        update={
            "status": ApprovalStatus.APPROVED,
            "decided_by": "operator",
            "decided_at": now + timedelta(seconds=1),
        }
    )
    assert approved.status is ApprovalStatus.APPROVED

    with pytest.raises(ValidationError):
        pending.model_copy(update={"status": ApprovalStatus.REJECTED})
    with pytest.raises(ValidationError):
        pending.model_copy(update={"decided_by": "operator"})
    with pytest.raises(ValidationError):
        pending.model_copy(
            update={
                "status": ApprovalStatus.APPROVED,
                "decided_by": "operator",
                "decided_at": now - timedelta(seconds=1),
            }
        )


def test_event_contract_revalidates_discriminated_payload_and_cursor() -> None:
    run_id = uuid.uuid4()
    now = datetime.now(UTC)
    draft = EventDraft(
        event_type="context.build_started",
        payload={"message_count": 1, "checkpoint_id": None},
        created_at=now,
    )
    event = StoredEvent(
        run_id=run_id,
        sequence=1,
        event_type=draft.event_type,
        payload=draft.payload,
        created_at=now,
    )
    assert event.to_agent_event().sequence == 1
    assert EventPage(events=(event,), next_after=1, has_more=False).next_after == 1

    with pytest.raises(ValidationError):
        EventDraft(
            event_type="context.build_started",
            payload={"message_count": -1, "checkpoint_id": None},
        )
    with pytest.raises(ValidationError):
        EventPage(events=(event,), next_after=0, has_more=False)
    assert EventPage.model_json_schema()["properties"]["events"]["maxItems"] == MAX_EVENT_PAGE_SIZE
