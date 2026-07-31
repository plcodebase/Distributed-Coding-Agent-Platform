from __future__ import annotations

import uuid
from collections import deque
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Self, cast

import pytest

from agent_core.distributed import (
    RunExecutionResult,
    RunLease,
    WorkerRegistration,
    WorkerStatus,
)
from agent_core.domain.errors import DomainOperationError
from agent_core.domain.status import RunStatus, ToolCallStatus
from platform_persistence import (
    PostgresRecoveryStore,
    PostgresRunQueue,
    PostgresWorkspaceLeaseStore,
)

NOW = datetime(2026, 7, 30, 12, tzinfo=UTC)
TENANT_ID = uuid.UUID("10000000-0000-0000-0000-000000000001")
SESSION_ID = uuid.UUID("20000000-0000-0000-0000-000000000002")
RUN_ID = uuid.UUID("30000000-0000-0000-0000-000000000003")
WORKSPACE_ID = uuid.UUID("40000000-0000-0000-0000-000000000004")
RUN_TOKEN = uuid.UUID("50000000-0000-0000-0000-000000000005")
WORKSPACE_TOKEN = uuid.UUID("60000000-0000-0000-0000-000000000006")


class _Scalars:
    def __init__(self, values: list[object]) -> None:
        self._values = values

    def all(self) -> list[object]:
        return self._values

    def __iter__(self) -> object:
        return iter(self._values)


class _ExecuteResult:
    def __init__(self, row: object = None) -> None:
        self._row = row

    def first(self) -> object:
        return self._row


class _Database:
    def __init__(
        self,
        *,
        scalar_values: list[object] | None = None,
        scalar_sets: list[list[object]] | None = None,
        execute_results: list[object] | None = None,
    ) -> None:
        self.scalar_values = deque(scalar_values or [])
        self.scalar_sets = deque(scalar_sets or [])
        self.execute_results = deque(execute_results or [])
        self.added: list[object] = []
        self.deleted: list[object] = []
        self.flushes = 0

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
            raise AssertionError("unexpected scalars query")
        return _Scalars(self.scalar_sets.popleft())

    async def execute(self, _statement: object) -> object:
        if not self.execute_results:
            return _ExecuteResult()
        return self.execute_results.popleft()

    def add(self, value: object) -> None:
        self.added.append(value)

    async def delete(self, value: object) -> None:
        self.deleted.append(value)

    async def flush(self) -> None:
        self.flushes += 1


class _Sessions:
    def __init__(self, *databases: _Database) -> None:
        self._databases = deque(databases)

    def __call__(self) -> _Database:
        if not self._databases:
            raise AssertionError("unexpected database session")
        return self._databases.popleft()


def _sessions(*databases: _Database) -> Any:
    return cast("Any", _Sessions(*databases))


def _worker_row(
    *,
    status: WorkerStatus = WorkerStatus.ACTIVE,
    available_slots: int = 2,
) -> SimpleNamespace:
    return SimpleNamespace(
        worker_id="worker-1",
        supported_sandbox_types=["podman"],
        total_slots=2,
        available_slots=available_slots,
        status=status.value,
        registered_at=NOW,
        last_heartbeat_at=NOW,
    )


def _run_row(
    *,
    status: RunStatus = RunStatus.QUEUED,
    cancellation_requested: bool = False,
    run_id: uuid.UUID = RUN_ID,
    workspace_id: uuid.UUID = WORKSPACE_ID,
) -> SimpleNamespace:
    assigned = "worker-1" if status in {RunStatus.LEASED, RunStatus.RUNNING} else None
    started_at = NOW if status is RunStatus.RUNNING else None
    return SimpleNamespace(
        id=run_id,
        tenant_id=TENANT_ID,
        session_id=SESSION_ID,
        workspace_id=workspace_id,
        status=status.value,
        priority=7,
        attempt=1,
        lease_generation=0 if status is RunStatus.QUEUED else 1,
        assigned_worker_id=assigned,
        lease_expires_at=NOW + timedelta(seconds=30) if assigned else None,
        last_checkpoint_id=None,
        cancellation_requested=cancellation_requested,
        created_at=NOW - timedelta(minutes=1),
        started_at=started_at,
        completed_at=None,
    )


def _run_lease_row(
    *,
    expires_at: datetime | None = None,
    run_id: uuid.UUID = RUN_ID,
) -> SimpleNamespace:
    return SimpleNamespace(
        tenant_id=TENANT_ID,
        run_id=run_id,
        worker_id="worker-1",
        lease_token=RUN_TOKEN,
        generation=1,
        acquired_at=NOW,
        last_heartbeat_at=NOW,
        expires_at=expires_at or NOW + timedelta(seconds=30),
    )


def _workspace_row(*, owned: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        tenant_id=TENANT_ID,
        workspace_id=WORKSPACE_ID,
        run_id=RUN_ID if owned else None,
        worker_id="worker-1" if owned else None,
        run_lease_token=RUN_TOKEN if owned else None,
        lease_token=WORKSPACE_TOKEN if owned else None,
        generation=1 if owned else 0,
        acquired_at=NOW if owned else None,
        expires_at=NOW + timedelta(seconds=30) if owned else None,
    )


def _lease(*, expires_at: datetime | None = None) -> RunLease:
    return RunLease(
        tenant_id=TENANT_ID,
        run_id=RUN_ID,
        session_id=SESSION_ID,
        workspace_id=WORKSPACE_ID,
        worker_id="worker-1",
        route_name="coding-default",
        lease_token=RUN_TOKEN,
        generation=1,
        attempt=1,
        priority=7,
        acquired_at=NOW,
        expires_at=expires_at or NOW + timedelta(seconds=30),
    )


@pytest.mark.asyncio
async def test_postgres_queue_worker_lifecycle_and_validation() -> None:
    registration = WorkerRegistration(
        worker_id="worker-1",
        supported_sandbox_types=("podman",),
        total_slots=2,
        available_slots=2,
        status=WorkerStatus.ACTIVE,
        registered_at=NOW,
        last_heartbeat_at=NOW,
    )
    create_database = _Database(scalar_values=[None])
    existing = _worker_row()
    update_database = _Database(scalar_values=[existing], scalar_sets=[[]])
    heartbeat_database = _Database(scalar_values=[existing], scalar_sets=[[]])
    drain_database = _Database(scalar_values=[existing])
    queue = PostgresRunQueue(
        _sessions(
            create_database,
            update_database,
            heartbeat_database,
            drain_database,
        )
    )

    created = await queue.register_worker(registration)
    assert created == registration
    assert len(create_database.added) == 1
    updated = await queue.register_worker(registration.model_copy(update={"available_slots": 1}))
    assert updated.available_slots == 1
    heartbeat = await queue.heartbeat_worker(
        "worker-1",
        occurred_at=NOW + timedelta(seconds=1),
        available_slots=2,
    )
    assert heartbeat.available_slots == 2
    drained = await queue.set_worker_draining(
        "worker-1",
        draining=True,
        occurred_at=NOW + timedelta(seconds=2),
    )
    assert drained.status is WorkerStatus.DRAINING

    with pytest.raises(TypeError, match="token_factory"):
        PostgresRunQueue(_sessions(), token_factory=cast("Any", None))
    invalid_time = PostgresRunQueue(_sessions())
    with pytest.raises(DomainOperationError) as timestamp:
        await invalid_time.claim(
            "worker-1",
            occurred_at=NOW.replace(tzinfo=None),
            lease_duration=timedelta(seconds=1),
        )
    assert timestamp.value.code == "run_claim_invalid"
    with pytest.raises(DomainOperationError) as duration:
        await invalid_time.claim(
            "worker-1",
            occurred_at=NOW,
            lease_duration=timedelta(0),
        )
    assert duration.value.code == "run_claim_invalid"


@pytest.mark.asyncio
async def test_postgres_queue_claim_start_heartbeat_and_finish() -> None:
    worker = _worker_row()
    queued = _run_row()
    workspace = _workspace_row()
    claim_database = _Database(
        scalar_values=[worker, workspace],
        execute_results=[_ExecuteResult((queued, "coding-default")), _ExecuteResult()],
    )
    lease_row = _run_lease_row()
    leased = _run_row(status=RunStatus.LEASED)
    start_database = _Database(
        scalar_values=[lease_row],
        execute_results=[_ExecuteResult((leased, "coding-default"))],
    )
    running_lease_row = _run_lease_row()
    running = _run_row(status=RunStatus.RUNNING)
    heartbeat_database = _Database(
        scalar_values=[running_lease_row],
        execute_results=[_ExecuteResult((running, "coding-default"))],
    )
    owned_workspace = _workspace_row(owned=True)
    finishing_worker = _worker_row(available_slots=1)
    finish_database = _Database(
        scalar_values=[running_lease_row, owned_workspace, finishing_worker],
        execute_results=[_ExecuteResult((running, "coding-default"))],
    )
    tokens = iter((RUN_TOKEN, WORKSPACE_TOKEN))
    queue = PostgresRunQueue(
        _sessions(
            claim_database,
            start_database,
            heartbeat_database,
            finish_database,
        ),
        token_factory=lambda: next(tokens),
    )

    lease = await queue.claim(
        "worker-1",
        occurred_at=NOW,
        lease_duration=timedelta(seconds=30),
    )
    assert lease is not None
    assert lease.lease_token == RUN_TOKEN
    assert lease.generation == 1
    assert queued.status == RunStatus.LEASED.value
    assert worker.available_slots == 1
    assert workspace.lease_token == WORKSPACE_TOKEN

    started = await queue.start(lease, occurred_at=NOW + timedelta(seconds=1))
    assert started.route_name == "coding-default"
    assert leased.status == RunStatus.RUNNING.value

    heartbeat = await queue.heartbeat(
        lease,
        occurred_at=NOW + timedelta(seconds=2),
        lease_duration=timedelta(seconds=30),
    )
    assert heartbeat.expires_at == NOW + timedelta(seconds=32)
    assert running.lease_expires_at == heartbeat.expires_at

    completed = await queue.finish(
        lease,
        RunExecutionResult(status=RunStatus.COMPLETED),
        occurred_at=NOW + timedelta(seconds=3),
    )
    assert completed.status is RunStatus.COMPLETED
    assert finish_database.deleted == [running_lease_row]
    assert owned_workspace.lease_token is None
    assert finishing_worker.available_slots == 2


@pytest.mark.asyncio
async def test_postgres_queue_claim_contention_and_fencing_fail_closed() -> None:
    inactive = _Database(scalar_values=[_worker_row(status=WorkerStatus.DRAINING)])
    missing_candidate = _Database(
        scalar_values=[_worker_row()],
        execute_results=[_ExecuteResult(None)],
    )
    queue = PostgresRunQueue(_sessions(inactive, missing_candidate))
    assert (
        await queue.claim(
            "worker-1",
            occurred_at=NOW,
            lease_duration=timedelta(seconds=30),
        )
        is None
    )
    assert (
        await queue.claim(
            "worker-1",
            occurred_at=NOW,
            lease_duration=timedelta(seconds=30),
        )
        is None
    )

    lost = PostgresRunQueue(_sessions(_Database(scalar_values=[None])))
    with pytest.raises(DomainOperationError) as error:
        await lost.start(_lease(), occurred_at=NOW + timedelta(seconds=1))
    assert error.value.code == "run_lease_lost"

    expired_row = _run_lease_row(expires_at=NOW)
    expired = PostgresRunQueue(
        _sessions(
            _Database(
                scalar_values=[expired_row],
                execute_results=[_ExecuteResult((_run_row(status=RunStatus.LEASED), "route"))],
            )
        )
    )
    with pytest.raises(DomainOperationError) as error:
        await expired.start(_lease(), occurred_at=NOW + timedelta(seconds=1))
    assert error.value.code == "run_lease_expired"

    invalid_token = PostgresRunQueue(
        _sessions(
            _Database(
                scalar_values=[_worker_row(), _workspace_row()],
                execute_results=[
                    _ExecuteResult((_run_row(), "route")),
                    _ExecuteResult(),
                ],
            )
        ),
        token_factory=cast("Any", lambda: "not-a-uuid"),
    )
    with pytest.raises(DomainOperationError) as error:
        await invalid_token.claim(
            "worker-1",
            occurred_at=NOW,
            lease_duration=timedelta(seconds=30),
        )
    assert error.value.code == "lease_token_invalid"


@pytest.mark.asyncio
async def test_postgres_queue_recovers_expired_and_cancelled_attempts() -> None:
    lost_lease = _run_lease_row(run_id=RUN_ID)
    cancelled_run_id = uuid.UUID("70000000-0000-0000-0000-000000000007")
    cancelled_lease = _run_lease_row(run_id=cancelled_run_id)
    lost_run = _run_row(status=RunStatus.RUNNING)
    cancelled_run = _run_row(
        status=RunStatus.RUNNING,
        cancellation_requested=True,
        run_id=cancelled_run_id,
        workspace_id=uuid.UUID("80000000-0000-0000-0000-000000000008"),
    )
    lost_workspace = _workspace_row(owned=True)
    cancelled_workspace = _workspace_row(owned=True)
    worker = _worker_row(available_slots=0)
    database = _Database(
        scalar_values=[
            lost_run,
            lost_workspace,
            worker,
            cancelled_run,
            cancelled_workspace,
            worker,
        ],
        scalar_sets=[[lost_lease, cancelled_lease]],
    )
    queue = PostgresRunQueue(_sessions(database))

    recovered = await queue.recover_expired(
        occurred_at=NOW + timedelta(minutes=1),
        limit=2,
    )

    assert [run.status for run in recovered] == [RunStatus.QUEUED, RunStatus.CANCELLED]
    assert recovered[0].attempt == 2
    assert worker.available_slots == 2
    assert database.deleted == [lost_lease, cancelled_lease]
    with pytest.raises(ValueError, match="limit"):
        await queue.recover_expired(occurred_at=NOW, limit=0)


@pytest.mark.asyncio
async def test_postgres_workspace_lease_acquire_heartbeat_and_release() -> None:
    active_run = _run_lease_row()
    row = _workspace_row()
    acquire_database = _Database(
        scalar_values=[active_run, row],
        execute_results=[_ExecuteResult()],
    )
    heartbeat_database = _Database(scalar_values=[active_run, row])
    release_database = _Database(scalar_values=[row])
    store = PostgresWorkspaceLeaseStore(
        _sessions(acquire_database, heartbeat_database, release_database),
        token_factory=lambda: WORKSPACE_TOKEN,
    )

    lease = await store.acquire(
        _lease(),
        occurred_at=NOW + timedelta(seconds=1),
        lease_duration=timedelta(seconds=10),
    )
    assert lease is not None
    assert lease.lease_token == WORKSPACE_TOKEN
    renewed = await store.heartbeat(
        lease,
        occurred_at=NOW + timedelta(seconds=2),
        lease_duration=timedelta(seconds=10),
    )
    assert renewed.expires_at == NOW + timedelta(seconds=12)
    await store.release(renewed)
    assert row.lease_token is None
    assert row.generation == 1

    with pytest.raises(TypeError, match="token_factory"):
        PostgresWorkspaceLeaseStore(_sessions(), token_factory=cast("Any", None))


@pytest.mark.asyncio
async def test_postgres_workspace_lease_rejects_stale_owners() -> None:
    active_run = _run_lease_row()
    occupied = _workspace_row(owned=True)
    occupied.run_id = uuid.uuid4()
    conflict = PostgresWorkspaceLeaseStore(
        _sessions(
            _Database(
                scalar_values=[active_run, occupied],
                execute_results=[_ExecuteResult()],
            )
        )
    )
    assert (
        await conflict.acquire(
            _lease(),
            occurred_at=NOW + timedelta(seconds=1),
            lease_duration=timedelta(seconds=10),
        )
        is None
    )

    stale = PostgresWorkspaceLeaseStore(
        _sessions(_Database(scalar_values=[active_run, _workspace_row(owned=True)]))
    )
    workspace_lease = await PostgresWorkspaceLeaseStore(
        _sessions(
            _Database(
                scalar_values=[active_run, _workspace_row()],
                execute_results=[_ExecuteResult()],
            )
        ),
        token_factory=lambda: WORKSPACE_TOKEN,
    ).acquire(
        _lease(),
        occurred_at=NOW + timedelta(seconds=1),
        lease_duration=timedelta(seconds=10),
    )
    assert workspace_lease is not None
    with pytest.raises(DomainOperationError) as heartbeat:
        await stale.heartbeat(
            workspace_lease.model_copy(update={"generation": 2}),
            occurred_at=NOW + timedelta(seconds=2),
            lease_duration=timedelta(seconds=10),
        )
    assert heartbeat.value.code == "workspace_lease_lost"

    stale_release_row = _workspace_row(owned=True)
    stale_release = PostgresWorkspaceLeaseStore(
        _sessions(_Database(scalar_values=[stale_release_row]))
    )
    with pytest.raises(DomainOperationError) as release:
        await stale_release.release(workspace_lease.model_copy(update={"generation": 2}))
    assert release.value.code == "workspace_lease_lost"


@pytest.mark.asyncio
async def test_postgres_recovery_loads_messages_plan_and_terminal_tool_outcomes() -> None:
    message = SimpleNamespace(
        sequence=1,
        role="user",
        content="continue",
        metadata_json={},
    )
    tool = SimpleNamespace(
        tool_call_id="call-1",
        tool_name="read_file",
        turn_number=1,
        argument_hash="a" * 64,
        status=ToolCallStatus.COMPLETED.value,
        workspace_version="revision-1",
        result={"content": "durable"},
        error=None,
    )
    database = _Database(
        scalar_values=[_run_lease_row(), None, SimpleNamespace(plan={"steps": []})],
        scalar_sets=[[message], [tool]],
    )
    state = await PostgresRecoveryStore(_sessions(database)).load(_lease())

    assert state.messages[0].content == "continue"
    assert state.task_plan.to_json_object() == {"steps": []}
    assert state.prior_tool_outcomes[0].result is not None

    missing_owner = PostgresRecoveryStore(_sessions(_Database(scalar_values=[None])))
    with pytest.raises(DomainOperationError) as error:
        await missing_owner.load(_lease())
    assert error.value.code == "run_lease_lost"


@pytest.mark.asyncio
async def test_postgres_recovery_uses_checkpoint_and_fails_closed_on_bad_context() -> None:
    checkpoint_id = uuid.UUID("90000000-0000-0000-0000-000000000009")
    checkpoint = SimpleNamespace(
        id=checkpoint_id,
        tenant_id=TENANT_ID,
        run_id=RUN_ID,
        session_id=SESSION_ID,
        message_sequence=1,
        workspace_snapshot_uri="s3://agent-platform/checkpoint",
        workspace_revision="revision-1",
        task_plan={"steps": [{"done": False}]},
        context_summary="summary",
        created_at=NOW,
    )
    lease = _lease().model_copy(update={"checkpoint_id": checkpoint_id})
    database = _Database(
        scalar_values=[_run_lease_row(), checkpoint],
        scalar_sets=[
            [
                SimpleNamespace(
                    sequence=1,
                    role="user",
                    content="resume",
                    metadata_json={},
                )
            ],
            [
                SimpleNamespace(
                    tool_call_id="mutating-call",
                    tool_name="edit_file",
                    turn_number=1,
                    argument_hash="a" * 64,
                    status=ToolCallStatus.COMPLETED.value,
                    workspace_version="revision-after-edit",
                    result={"workspace_revision": "revision-after-edit"},
                    error=None,
                )
            ],
        ],
    )
    state = await PostgresRecoveryStore(_sessions(database)).load(lease)
    assert state.checkpoint is not None
    assert state.context_summary == "summary"
    assert state.workspace_restore_revision == "revision-after-edit"

    missing_checkpoint = PostgresRecoveryStore(
        _sessions(
            _Database(
                scalar_values=[_run_lease_row(), None],
            )
        )
    )
    with pytest.raises(DomainOperationError) as error:
        await missing_checkpoint.load(lease)
    assert error.value.code == "recovery_checkpoint_missing"

    empty = PostgresRecoveryStore(
        _sessions(
            _Database(
                scalar_values=[_run_lease_row(), None],
                scalar_sets=[[]],
            )
        )
    )
    with pytest.raises(DomainOperationError) as error:
        await empty.load(_lease())
    assert error.value.code == "recovery_context_missing"

    invalid_message = PostgresRecoveryStore(
        _sessions(
            _Database(
                scalar_values=[_run_lease_row(), None],
                scalar_sets=[
                    [
                        SimpleNamespace(
                            sequence=1,
                            role="invalid",
                            content="bad",
                            metadata_json={},
                        )
                    ]
                ],
            )
        )
    )
    with pytest.raises(DomainOperationError) as error:
        await invalid_message.load(_lease())
    assert error.value.code == "recovery_message_invalid"

    reserved_metadata = PostgresRecoveryStore(
        _sessions(
            _Database(
                scalar_values=[_run_lease_row(), None],
                scalar_sets=[
                    [
                        SimpleNamespace(
                            sequence=1,
                            role="user",
                            content="trusted column",
                            metadata_json={"content": "metadata override"},
                        )
                    ]
                ],
            )
        )
    )
    with pytest.raises(DomainOperationError) as error:
        await reserved_metadata.load(_lease())
    assert error.value.code == "recovery_message_invalid"
