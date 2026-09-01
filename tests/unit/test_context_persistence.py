from __future__ import annotations

import uuid
from collections import deque
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Self, cast

import pytest

from agent_core.control import (
    ContextCompactionStatus,
    MemoryExtractionJob,
    MemoryExtractionStatus,
    MemoryKind,
    PersistedMemory,
    TaskPlanUpdate,
    TaskStatus,
    memory_content_hash,
)
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.gateway import MessageRole
from platform_persistence import (
    PostgresContextRepository,
    PostgresMemoryRepository,
    PostgresTaskRepository,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

NOW = datetime(2026, 7, 31, 12, tzinfo=UTC)
TENANT_ID = uuid.UUID("10000000-0000-0000-0000-000000000001")
SESSION_ID = uuid.UUID("20000000-0000-0000-0000-000000000002")
RUN_ID = uuid.UUID("30000000-0000-0000-0000-000000000003")
COMPACTION_ID = uuid.UUID("40000000-0000-0000-0000-000000000004")
JOB_ID = uuid.UUID("50000000-0000-0000-0000-000000000005")
LEASE_TOKEN = uuid.UUID("60000000-0000-0000-0000-000000000006")
REASSIGNED_LEASE_TOKEN = uuid.UUID("70000000-0000-0000-0000-000000000007")


class _Scalars:
    def __init__(self, values: list[object]) -> None:
        self.values = values
        self.consumed = 0
        self.closed = False
        self._iterator: Iterator[object] | None = None

    def __iter__(self) -> object:
        return iter(self.values)

    def __aiter__(self) -> Self:
        self._iterator = iter(self.values)
        return self

    async def __anext__(self) -> object:
        if self._iterator is None:
            raise StopAsyncIteration
        try:
            value = next(self._iterator)
        except StopIteration as error:
            raise StopAsyncIteration from error
        self.consumed += 1
        return value

    async def close(self) -> None:
        self.closed = True


class _Database:
    def __init__(
        self,
        *,
        scalar_values: list[object] | None = None,
        scalar_sets: list[list[object]] | None = None,
    ) -> None:
        self.scalar_values = deque(scalar_values or [])
        self.scalar_sets = deque(scalar_sets or [])
        self.added: list[object] = []
        self.executed = 0
        self.streams: list[_Scalars] = []

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def begin(self) -> Self:
        return self

    async def scalar(self, _statement: object) -> object:
        if not self.scalar_values:
            raise AssertionError("unexpected scalar query")
        return self.scalar_values.popleft()

    async def scalars(self, _statement: object) -> _Scalars:
        if not self.scalar_sets:
            raise AssertionError("unexpected scalar-set query")
        return _Scalars(self.scalar_sets.popleft())

    async def stream_scalars(self, _statement: object) -> _Scalars:
        if not self.scalar_sets:
            raise AssertionError("unexpected streamed scalar-set query")
        result = _Scalars(self.scalar_sets.popleft())
        self.streams.append(result)
        return result

    async def execute(self, _statement: object) -> object:
        self.executed += 1
        return object()

    def add(self, value: object) -> None:
        self.added.append(value)


class _Sessions:
    def __init__(self, *databases: _Database) -> None:
        self.databases = deque(databases)

    def __call__(self) -> _Database:
        return self.databases.popleft()


def _sessions(*databases: _Database) -> Any:
    return cast("Any", _Sessions(*databases))


def _compaction_row(
    *,
    status: ContextCompactionStatus = ContextCompactionStatus.PENDING,
    compaction_id: uuid.UUID = COMPACTION_ID,
    idempotency_key: str = "compact-1",
    source_message_sequence: int = 12,
) -> SimpleNamespace:
    terminal = status is not ContextCompactionStatus.PENDING
    return SimpleNamespace(
        id=compaction_id,
        tenant_id=TENANT_ID,
        session_id=SESSION_ID,
        context_generation=1,
        status=status.value,
        idempotency_key=idempotency_key,
        source_message_sequence=source_message_sequence,
        route_name="coding-default",
        summary="summary" if status is ContextCompactionStatus.COMPLETED else None,
        input_tokens=10 if status is ContextCompactionStatus.COMPLETED else None,
        output_tokens=4 if status is ContextCompactionStatus.COMPLETED else None,
        error=(
            {"code": "failed", "message": "failed", "retryable": True, "details": {}}
            if status is ContextCompactionStatus.FAILED
            else None
        ),
        requested_at=NOW,
        completed_at=NOW + timedelta(seconds=1) if terminal else None,
    )


def _memory_job_row(
    *,
    status: MemoryExtractionStatus = MemoryExtractionStatus.RUNNING,
) -> SimpleNamespace:
    running = status is MemoryExtractionStatus.RUNNING
    return SimpleNamespace(
        id=JOB_ID,
        tenant_id=TENANT_ID,
        session_id=SESSION_ID,
        run_id=RUN_ID,
        execution_epoch=1,
        status=status.value,
        source_message_sequence=12,
        attempt=1,
        worker_id="memory-worker" if running else None,
        lease_token=LEASE_TOKEN if running else None,
        lease_generation=1 if running else 0,
        lease_expires_at=NOW + timedelta(minutes=5) if running else None,
        error=None,
        created_at=NOW,
        started_at=NOW + timedelta(seconds=1) if running else None,
        completed_at=None,
    )


def _memory_job() -> MemoryExtractionJob:
    return MemoryExtractionJob(
        id=JOB_ID,
        tenant_id=TENANT_ID,
        session_id=SESSION_ID,
        run_id=RUN_ID,
        execution_epoch=1,
        status=MemoryExtractionStatus.RUNNING,
        source_message_sequence=12,
        worker_id="memory-worker",
        lease_token=LEASE_TOKEN,
        lease_generation=1,
        lease_expires_at=NOW + timedelta(minutes=5),
        created_at=NOW,
        started_at=NOW + timedelta(seconds=1),
    )


def _message_row(sequence: int, *, role: MessageRole = MessageRole.USER) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid.uuid4(),
        tenant_id=TENANT_ID,
        session_id=SESSION_ID,
        run_id=RUN_ID,
        execution_epoch=1,
        sequence=sequence,
        role=role.value,
        content=f"message-{sequence}",
        metadata_json={},
        created_at=NOW + timedelta(seconds=sequence),
    )


@pytest.mark.asyncio
async def test_context_repository_loads_bounded_chronological_messages() -> None:
    rows: list[object] = [
        _message_row(3),
        _message_row(4, role=MessageRole.ASSISTANT),
    ]
    repository = PostgresContextRepository(
        _sessions(_Database(scalar_sets=[rows]), _Database(scalar_sets=[rows]))
    )

    messages = await repository.list_messages(
        TENANT_ID,
        SESSION_ID,
        after_sequence=2,
        through_sequence=4,
        limit=2,
    )

    assert [message.sequence for message in messages] == [3, 4]
    assert messages[-1].role is MessageRole.ASSISTANT
    with pytest.raises(DomainOperationError) as overflow:
        await repository.list_messages(TENANT_ID, SESSION_ID, limit=1)
    assert overflow.value.code == "context_history_limit"


@pytest.mark.asyncio
async def test_context_repository_loads_validated_original_file_references() -> None:
    metadata = {
        "source": "run_submission",
        "referenced_files": [{"path": "README.md"}, {"path": "docs/design.md"}],
    }
    repository = PostgresContextRepository(
        _sessions(
            _Database(scalar_values=[metadata]),
            _Database(scalar_values=[{}]),
            _Database(scalar_values=[None]),
        )
    )

    references = await repository.referenced_files_for_run(TENANT_ID, RUN_ID)
    legacy = await repository.referenced_files_for_run(TENANT_ID, RUN_ID)
    missing = await repository.referenced_files_for_run(TENANT_ID, RUN_ID)

    assert [item.path for item in references] == ["README.md", "docs/design.md"]
    assert legacy == ()
    assert missing == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "metadata",
    [
        [],
        {"source": "run_submission", "referenced_files": "README.md"},
        {
            "source": "run_submission",
            "referenced_files": [{"path": "README.md"}, {"path": "README.md"}],
        },
        {"source": "run_submission", "referenced_files": [{"path": ".git/config"}]},
    ],
)
async def test_context_repository_fails_closed_for_invalid_file_references(
    metadata: object,
) -> None:
    repository = PostgresContextRepository(_sessions(_Database(scalar_values=[metadata])))

    with pytest.raises(DomainOperationError) as failure:
        await repository.referenced_files_for_run(TENANT_ID, RUN_ID)
    assert failure.value.code == "context_references_invalid"


@pytest.mark.asyncio
async def test_context_repository_requests_replays_and_finishes_compaction() -> None:
    inserted = _compaction_row()
    session = SimpleNamespace(id=SESSION_ID, context_generation=1)
    create_db = _Database(scalar_values=[session, None, None, 12, inserted])
    repository = PostgresContextRepository(_sessions(create_db))
    created = await repository.request_compaction(
        TENANT_ID,
        SESSION_ID,
        compaction_id=COMPACTION_ID,
        idempotency_key="compact-1",
        route_name="coding-default",
        requested_at=NOW,
    )
    assert created is not None
    assert created.source_message_sequence == 12
    assert create_db.executed == 1

    fetched = await PostgresContextRepository(_sessions(_Database(scalar_values=[inserted]))).get(
        TENANT_ID, SESSION_ID, COMPACTION_ID
    )
    assert fetched == created

    replay_repository = PostgresContextRepository(
        _sessions(_Database(scalar_values=[session, inserted]))
    )
    replay = await replay_repository.request_compaction(
        TENANT_ID,
        SESSION_ID,
        compaction_id=uuid.uuid4(),
        idempotency_key="compact-1",
        route_name="coding-default",
        requested_at=NOW,
    )
    assert replay is not None and replay.id == COMPACTION_ID

    with pytest.raises(DomainOperationError) as pending:
        await PostgresContextRepository(
            _sessions(_Database(scalar_values=[session, None, inserted]))
        ).request_compaction(
            TENANT_ID,
            SESSION_ID,
            compaction_id=uuid.uuid4(),
            idempotency_key="compact-2",
            route_name="coding-default",
            requested_at=NOW,
        )
    assert pending.value.code == "context_compaction_in_progress"
    assert pending.value.details["compaction_id"] == str(COMPACTION_ID)

    completion_row = _compaction_row()
    completed = await PostgresContextRepository(
        _sessions(_Database(scalar_values=[completion_row]))
    ).complete(
        TENANT_ID,
        COMPACTION_ID,
        summary="new summary",
        input_tokens=20,
        output_tokens=5,
        completed_at=NOW + timedelta(seconds=2),
    )
    assert completed is not None
    assert completed.status is ContextCompactionStatus.COMPLETED
    assert completed.summary == "new summary"

    with pytest.raises(DomainOperationError) as conflict:
        await PostgresContextRepository(
            _sessions(_Database(scalar_values=[session, _compaction_row()]))
        ).request_compaction(
            TENANT_ID,
            SESSION_ID,
            compaction_id=uuid.uuid4(),
            idempotency_key="compact-1",
            route_name="another-route",
            requested_at=NOW,
        )
    assert conflict.value.code == "context_compaction_idempotency_conflict"


@pytest.mark.asyncio
async def test_context_repository_proactively_schedules_compaction_idempotently() -> None:
    session = SimpleNamespace(id=SESSION_ID, context_generation=1)
    automatic_key = "auto-context-1-12"
    automatic_id = uuid.uuid5(SESSION_ID, automatic_key)
    inserted = _compaction_row(
        compaction_id=automatic_id,
        idempotency_key=automatic_key,
    )
    create_db = _Database(scalar_values=[session, None, 3, 12, None, inserted])
    repository = PostgresContextRepository(_sessions(create_db))

    created = await repository.request_compaction_if_needed(
        TENANT_ID,
        SESSION_ID,
        after_message_sequence=None,
        threshold_messages=3,
        route_name="coding-default",
        requested_at=NOW,
    )

    assert created is not None
    assert created.id == automatic_id
    assert created.idempotency_key == automatic_key
    assert create_db.executed == 1

    replay_db = _Database(scalar_values=[session, None, 3, 12, inserted])
    replay = await PostgresContextRepository(_sessions(replay_db)).request_compaction_if_needed(
        TENANT_ID,
        SESSION_ID,
        after_message_sequence=9,
        threshold_messages=3,
        route_name="coding-default",
        requested_at=NOW + timedelta(seconds=1),
    )
    assert replay == created
    assert replay_db.executed == 0


@pytest.mark.asyncio
async def test_context_repository_proactive_compaction_respects_threshold_and_pending() -> None:
    session = SimpleNamespace(id=SESSION_ID, context_generation=1)
    pending = _compaction_row()
    existing = await PostgresContextRepository(
        _sessions(_Database(scalar_values=[session, pending]))
    ).request_compaction_if_needed(
        TENANT_ID,
        SESSION_ID,
        after_message_sequence=None,
        threshold_messages=3,
        route_name="coding-default",
        requested_at=NOW,
    )
    assert existing is not None and existing.id == COMPACTION_ID

    below_threshold = await PostgresContextRepository(
        _sessions(_Database(scalar_values=[session, None, 2]))
    ).request_compaction_if_needed(
        TENANT_ID,
        SESSION_ID,
        after_message_sequence=10,
        threshold_messages=3,
        route_name="coding-default",
        requested_at=NOW,
    )
    assert below_threshold is None

    repository = PostgresContextRepository(cast("Any", None))
    for after_sequence, threshold in ((-1, 3), (None, 0), (None, 4096), (None, True)):
        with pytest.raises(ValueError):
            await repository.request_compaction_if_needed(
                TENANT_ID,
                SESSION_ID,
                after_message_sequence=after_sequence,
                threshold_messages=threshold,
                route_name="coding-default",
                requested_at=NOW,
            )


@pytest.mark.asyncio
async def test_context_repository_reads_pending_latest_and_failed_outcomes() -> None:
    repository = PostgresContextRepository(
        _sessions(
            _Database(scalar_values=[_compaction_row()]),
            _Database(scalar_values=[_compaction_row(status=ContextCompactionStatus.COMPLETED)]),
            _Database(scalar_values=[_compaction_row()]),
        )
    )
    assert await repository.pending_for_session(TENANT_ID, SESSION_ID) is not None
    latest = await repository.latest_completed(TENANT_ID, SESSION_ID)
    assert latest is not None and latest.summary == "summary"
    failed = await repository.fail(
        TENANT_ID,
        COMPACTION_ID,
        error=ErrorDetail(code="context_failed", message="failed", retryable=True),
        completed_at=NOW + timedelta(seconds=2),
    )
    assert failed is not None and failed.status is ContextCompactionStatus.FAILED


@pytest.mark.asyncio
async def test_task_repository_compare_and_set_round_trip() -> None:
    update = TaskPlanUpdate.model_validate(
        {
            "expected_version": 0,
            "tasks": [{"id": "task-1", "title": "Persist task state"}],
        }
    )
    run_record = SimpleNamespace(id=RUN_ID, execution_epoch=1)
    create_db = _Database(scalar_values=[run_record, None])
    repository = PostgresTaskRepository(_sessions(create_db))
    state = await repository.update(
        TENANT_ID,
        RUN_ID,
        update,
        plan_id=uuid.uuid4(),
        created_at=NOW,
    )
    assert state is not None and state.version == 1
    record = create_db.added[0]

    loaded = await PostgresTaskRepository(_sessions(_Database(scalar_values=[record]))).get(
        TENANT_ID, RUN_ID
    )
    assert loaded is not None and loaded.tasks[0].id == "task-1"

    existing = SimpleNamespace(version=2)
    with pytest.raises(DomainOperationError) as conflict:
        await PostgresTaskRepository(
            _sessions(_Database(scalar_values=[run_record, existing]))
        ).update(
            TENANT_ID,
            RUN_ID,
            update,
            plan_id=uuid.uuid4(),
            created_at=NOW,
        )
    assert conflict.value.code == "task_plan_version_conflict"


@pytest.mark.asyncio
async def test_task_repository_exposes_legacy_step_plans_without_mutation() -> None:
    row = SimpleNamespace(
        id=uuid.uuid4(),
        run_id=RUN_ID,
        execution_epoch=1,
        version=1,
        plan={
            "steps": [
                {"title": "Already done", "done": True},
                {"text": "Still pending", "status": "pending"},
            ]
        },
        created_at=NOW,
    )
    state = await PostgresTaskRepository(_sessions(_Database(scalar_values=[row]))).get(
        TENANT_ID,
        RUN_ID,
    )
    assert state is not None
    assert [task.id for task in state.tasks] == ["legacy-step-1", "legacy-step-2"]
    assert state.tasks[0].status is TaskStatus.COMPLETED
    assert state.tasks[1].title == "Still pending"


@pytest.mark.asyncio
async def test_memory_repository_settings_listing_source_completion_and_failure() -> None:
    session_row = SimpleNamespace(memory_enabled=True, updated_at=NOW)
    settings_repository = PostgresMemoryRepository(
        _sessions(_Database(scalar_values=[session_row]))
    )
    assert await settings_repository.set_session_enabled(
        TENANT_ID,
        SESSION_ID,
        enabled=False,
        updated_at=NOW + timedelta(seconds=1),
    )
    assert session_row.memory_enabled is False

    memory_row = SimpleNamespace(
        id=uuid.uuid4(),
        tenant_id=TENANT_ID,
        session_id=SESSION_ID,
        source_run_id=RUN_ID,
        execution_epoch=1,
        kind=MemoryKind.FACT.value,
        content="Uses PostgreSQL.",
        content_hash=memory_content_hash("Uses PostgreSQL."),
        memory_metadata={},
        extracted_at=NOW,
        archived_at=None,
    )
    listed = await PostgresMemoryRepository(
        _sessions(_Database(scalar_values=[True, True], scalar_sets=[[memory_row]]))
    ).list_active(TENANT_ID, SESSION_ID)
    assert listed[0].source_run_id == RUN_ID

    archive_row = SimpleNamespace(**memory_row.__dict__)
    archived = await PostgresMemoryRepository(
        _sessions(_Database(scalar_values=[archive_row]))
    ).archive(
        TENANT_ID,
        SESSION_ID,
        archive_row.id,
        archived_at=NOW + timedelta(seconds=1),
    )
    assert archived is not None and archived.archived_at == NOW + timedelta(seconds=1)

    job = _memory_job()
    running_row = _memory_job_row()
    messages = [
        SimpleNamespace(sequence=2, role="assistant", content="new"),
        SimpleNamespace(sequence=1, role="user", content="old"),
    ]
    source = await PostgresMemoryRepository(
        _sessions(
            _Database(
                scalar_values=[running_row],
                scalar_sets=[cast("list[object]", messages)],
            )
        )
    ).source_for_job(
        job,
        max_bytes=100,
        occurred_at=NOW + timedelta(seconds=2),
    )
    assert source == "[user] old\n[assistant] new"

    many_messages = [
        SimpleNamespace(sequence=index, role="user", content="x" * 90)
        for index in range(100, 0, -1)
    ]
    bounded_db = _Database(
        scalar_values=[_memory_job_row()],
        scalar_sets=[cast("list[object]", many_messages)],
    )
    bounded_source = await PostgresMemoryRepository(_sessions(bounded_db)).source_for_job(
        job,
        max_bytes=120,
        occurred_at=NOW + timedelta(seconds=2),
    )
    assert len(bounded_source.encode("utf-8")) <= 120
    assert bounded_db.streams[0].consumed == 2
    assert bounded_db.streams[0].closed is True

    with pytest.raises(DomainOperationError) as expired:
        await PostgresMemoryRepository(
            _sessions(_Database(scalar_values=[_memory_job_row()]))
        ).source_for_job(
            job,
            max_bytes=120,
            occurred_at=NOW + timedelta(minutes=5),
        )
    assert expired.value.code == "memory_extraction_lease_lost"

    memory = PersistedMemory(
        id=uuid.uuid4(),
        tenant_id=TENANT_ID,
        session_id=SESSION_ID,
        source_run_id=RUN_ID,
        kind=MemoryKind.DECISION,
        content="Use Podman.",
        content_hash=memory_content_hash("Use Podman."),
        extracted_at=NOW + timedelta(seconds=2),
    )
    complete_db = _Database(scalar_values=[running_row, True, True])
    completed = await PostgresMemoryRepository(_sessions(complete_db)).complete(
        job,
        (memory,),
        completed_at=NOW + timedelta(seconds=3),
    )
    assert completed.status is MemoryExtractionStatus.COMPLETED
    assert complete_db.executed == 1

    failed_row = _memory_job_row()
    failed = await PostgresMemoryRepository(_sessions(_Database(scalar_values=[failed_row]))).fail(
        job,
        error=ErrorDetail(code="memory_failed", message="failed", retryable=True),
        completed_at=NOW + timedelta(seconds=3),
    )
    assert failed.status is MemoryExtractionStatus.FAILED


@pytest.mark.asyncio
async def test_memory_claim_honors_disable_policy_and_limits() -> None:
    disabled_row = _memory_job_row(status=MemoryExtractionStatus.PENDING)
    disabled_row.started_at = None
    disabled = await PostgresMemoryRepository(
        _sessions(_Database(scalar_values=[disabled_row, False]))
    ).claim_pending(
        worker_id="memory-worker",
        occurred_at=NOW + timedelta(seconds=1),
        lease_duration=timedelta(minutes=5),
    )
    assert disabled is not None and disabled.status is MemoryExtractionStatus.COMPLETED

    with pytest.raises(ValueError):
        await PostgresMemoryRepository(_sessions()).list_active(
            TENANT_ID,
            SESSION_ID,
            limit=0,
        )


@pytest.mark.asyncio
async def test_memory_claim_recovers_expired_job_and_fences_stale_owner() -> None:
    pending_row = _memory_job_row(status=MemoryExtractionStatus.PENDING)
    databases = (
        _Database(scalar_values=[pending_row, True, True]),
        _Database(scalar_values=[pending_row, True, True]),
        _Database(scalar_values=[pending_row]),
    )
    tokens = iter((LEASE_TOKEN, REASSIGNED_LEASE_TOKEN))
    repository = PostgresMemoryRepository(
        _sessions(*databases),
        token_factory=lambda: next(tokens),
    )
    first = await repository.claim_pending(
        worker_id="memory-worker-a",
        occurred_at=NOW + timedelta(seconds=1),
        lease_duration=timedelta(seconds=1),
    )
    assert first is not None
    assert first.lease_generation == 1
    recovered = await repository.claim_pending(
        worker_id="memory-worker-b",
        occurred_at=NOW + timedelta(seconds=3),
        lease_duration=timedelta(seconds=10),
    )
    assert recovered is not None
    assert recovered.attempt == 2
    assert recovered.lease_generation == 2
    assert recovered.lease_token == REASSIGNED_LEASE_TOKEN

    with pytest.raises(DomainOperationError) as stale:
        await repository.complete(
            first,
            (),
            completed_at=NOW + timedelta(seconds=4),
        )
    assert stale.value.code == "memory_extraction_lease_lost"
