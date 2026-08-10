from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import time
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from agent_api.factory import AgentApiSettings, create_production_app
from agent_core.control import (
    ApprovalStatus,
    PersistedApproval,
    PersistedMessage,
    PersistedTaskPlan,
)
from agent_core.distributed import (
    RunExecutionResult,
    RunLease,
    RunRecoveryState,
    WorkerRegistration,
    WorkerStatus,
    WorkspaceWriterLease,
)
from agent_core.domain.base import FrozenJsonObject
from agent_core.domain.errors import DomainOperationError
from agent_core.domain.models import (
    Checkpoint,
    ModelCall,
    Run,
    Session,
    ToolCall,
    canonical_argument_hash,
)
from agent_core.domain.status import (
    ApprovalMode,
    ModelCallStatus,
    RunStatus,
    SessionStatus,
    ToolCallStatus,
)
from agent_core.event_store import EventDraft
from agent_core.fakes import (
    ScriptedGatewayTurn,
    ScriptedModelGateway,
    SequentialIdGenerator,
    SteppingClock,
)
from agent_core.gateway import (
    GatewayFinishReason,
    GatewayResponseCompleted,
    GatewayToolCall,
    MessageRole,
)
from agent_core.loop import AgentLoop
from agent_core.tools import (
    RegisteredTool,
    ToolArguments,
    ToolEffect,
    ToolExecutionCompleted,
    ToolExecutionContext,
    ToolExecutionEvent,
    ToolRegistry,
)
from agent_worker import AgentLoopRunExecutor, WorkerConfig, WorkerService
from event_store import PostgresEventStore
from gateway_client import GatewayRequestClaimStatus
from platform_persistence import (
    Database,
    DatabaseSettings,
    PostgresExecutionRepository,
    PostgresGatewayCircuitBreaker,
    PostgresGatewayRateLimiter,
    PostgresGatewayRequestStore,
    PostgresRecoveryStore,
    PostgresRunQueue,
    PostgresRunRepository,
    PostgresSessionRepository,
    PostgresWorkspaceLeaseStore,
    run_creation_hash,
)
from platform_persistence.models import (
    AgentEventRecord,
    ApprovalRecord,
    CheckpointRecord,
    MessageRecord,
    ModelCallRecord,
    RunRecord,
    TaskPlanRecord,
    ToolCallRecord,
    WorkerRecord,
    WorkspaceLeaseRecord,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator

pytestmark = [
    pytest.mark.security,
    pytest.mark.skipif(
        os.getenv("AGENT_PLATFORM_RUN_POSTGRES_INTEGRATION") != "1",
        reason="set AGENT_PLATFORM_RUN_POSTGRES_INTEGRATION=1",
    ),
]

ROOT = Path(__file__).resolve().parents[2]
PODMAN = shutil.which("podman") or "podman"
POSTGRES_IMAGE = "postgres:18.4-alpine3.23"
TENANT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
OTHER_TENANT_ID = uuid.UUID("00000000-0000-0000-0000-000000000002")
API_TOKEN = "postgres-test-token"  # noqa: S105 - inert integration credential
OTHER_API_TOKEN = "postgres-other-token"  # noqa: S105 - inert integration credential


class MutableClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class SteppingWorkerClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def now(self) -> datetime:
        self.value += timedelta(milliseconds=1)
        return self.value


class RecordingWorkspaceRestorer:
    def __init__(self) -> None:
        self.calls: list[tuple[uuid.UUID, uuid.UUID, uuid.UUID, str]] = []

    async def restore(
        self,
        lease: RunLease,
        checkpoint: Checkpoint,
        *,
        writer_lease: WorkspaceWriterLease,
        workspace_revision: str,
    ) -> None:
        self.calls.append(
            (
                lease.run_id,
                checkpoint.id,
                writer_lease.lease_token,
                workspace_revision,
            )
        )


class ReplayEditArguments(ToolArguments):
    path: str
    old: str
    new: str


class CountingMutationHandler:
    def __init__(self) -> None:
        self.calls = 0

    async def __call__(
        self,
        arguments: ReplayEditArguments,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        del arguments, context
        self.calls += 1
        yield ToolExecutionCompleted(
            result={
                "path": "README.md",
                "workspace_revision": "duplicate-revision",
            }
        )


async def wait_for_worker_idle(worker: WorkerService) -> None:
    for _ in range(200):
        if worker.active_count == 0:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("worker did not finish its claimed run")


@pytest.fixture(scope="module")
def postgres_url() -> Iterator[str]:
    external_url = os.getenv("AGENT_PLATFORM_POSTGRES_TEST_URL")
    if external_url is not None:
        if not external_url.startswith("postgresql+asyncpg://"):
            raise ValueError("AGENT_PLATFORM_POSTGRES_TEST_URL must use postgresql+asyncpg")
        with _migrated_database(external_url) as migrated_url:
            yield migrated_url
        return

    port = 55_000 + os.getpid() % 1000
    name = f"agent-platform-postgres-test-{os.getpid()}"
    _podman(
        "run",
        "--detach",
        "--name",
        name,
        "--pull=never",
        "--restart=no",
        "--cap-drop=all",
        "--cap-add=CHOWN",
        "--cap-add=SETUID",
        "--cap-add=SETGID",
        "--security-opt=no-new-privileges",
        "--env",
        "POSTGRES_DB=agent_test",
        "--env",
        "POSTGRES_USER=agent_test",
        "--env",
        "POSTGRES_PASSWORD=agent_test_password",
        "--env",
        "PGDATA=/var/lib/postgresql/data",
        "--tmpfs",
        "/var/lib/postgresql:rw,nodev,nosuid,size=256m",
        "--publish",
        f"127.0.0.1:{port}:5432",
        POSTGRES_IMAGE,
    )
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            ready = _podman(
                "exec",
                name,
                "psql",
                "-U",
                "agent_test",
                "-d",
                "agent_test",
                "--tuples-only",
                "--no-align",
                "--command",
                "SELECT 1",
                check=False,
            )
            if ready.returncode == 0 and ready.stdout.strip() == b"1":
                break
            time.sleep(0.25)
        else:
            raise AssertionError("PostgreSQL test container did not become ready")

        url = f"postgresql+asyncpg://agent_test:agent_test_password@127.0.0.1:{port}/agent_test"
        with _migrated_database(url) as migrated_url:
            yield migrated_url
    finally:
        _podman("rm", "--force", name, check=False)


@contextmanager
def _migrated_database(url: str) -> Iterator[str]:
    previous = os.environ.get("AGENT_PLATFORM_DATABASE_URL")
    os.environ["AGENT_PLATFORM_DATABASE_URL"] = url
    try:
        configuration = Config(str(ROOT / "alembic.ini"))
        command.upgrade(configuration, "head")
        command.check(configuration)
        yield url
        command.downgrade(configuration, "base")
        command.upgrade(configuration, "head")
    finally:
        if previous is None:
            os.environ.pop("AGENT_PLATFORM_DATABASE_URL", None)
        else:
            os.environ["AGENT_PLATFORM_DATABASE_URL"] = previous


@pytest.mark.asyncio
async def test_sessions_runs_and_gateway_requests_survive_recomposition(
    postgres_url: str,
) -> None:
    database = Database(DatabaseSettings(database_url=postgres_url))
    sessions = PostgresSessionRepository(database.sessions)
    runs = PostgresRunRepository(database.sessions)
    gateway = PostgresGatewayRequestStore(database.sessions)
    now = datetime.now(UTC)
    session = _session(now)
    await sessions.create(session)
    run = _run(session, now)
    created = await runs.create_idempotent(
        TENANT_ID,
        run,
        idempotency_key="create-run",
        creation_hash=run_creation_hash(priority=run.priority),
    )
    replayed = await runs.create_idempotent(
        TENANT_ID,
        _run(session, now),
        idempotency_key="create-run",
        creation_hash=run_creation_hash(priority=run.priority),
    )
    assert created.created is True
    assert replayed.created is False
    assert replayed.run.id == run.id

    request_hash = "0" * 64
    claim = await gateway.claim(TENANT_ID, "gateway-request", request_hash)
    assert claim.status is GatewayRequestClaimStatus.EXECUTE
    terminal = GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP)
    await gateway.complete(
        TENANT_ID,
        "gateway-request",
        request_hash,
        (terminal,),
    )
    await database.aclose()

    recomposed = Database(DatabaseSettings(database_url=postgres_url))
    try:
        assert await PostgresSessionRepository(recomposed.sessions).get(TENANT_ID, session.id)
        assert await PostgresRunRepository(recomposed.sessions).get(TENANT_ID, run.id)
        stored = await PostgresGatewayRequestStore(recomposed.sessions).claim(
            TENANT_ID,
            "gateway-request",
            request_hash,
        )
        assert stored.status is GatewayRequestClaimStatus.COMPLETED
        assert stored.events == (terminal,)
        other_tenant = await PostgresGatewayRequestStore(recomposed.sessions).claim(
            uuid.uuid4(),
            "gateway-request",
            request_hash,
        )
        assert other_tenant.status is GatewayRequestClaimStatus.EXECUTE
    finally:
        await recomposed.aclose()


@pytest.mark.asyncio
async def test_concurrent_event_appends_are_unique_ordered_and_replayable(
    postgres_url: str,
) -> None:
    database = Database(DatabaseSettings(database_url=postgres_url))
    sessions = PostgresSessionRepository(database.sessions)
    runs = PostgresRunRepository(database.sessions)
    now = datetime.now(UTC)
    session = _session(now)
    await sessions.create(session)
    run = _run(session, now)
    await runs.create_idempotent(
        TENANT_ID,
        run,
        idempotency_key=f"events-{run.id}",
        creation_hash=run_creation_hash(priority=0),
    )
    store = PostgresEventStore(database.sessions)
    try:
        appended = await asyncio.gather(
            *(
                store.append(
                    TENANT_ID,
                    run.id,
                    EventDraft(
                        event_type="context.build_started",
                        payload={
                            "message_count": index,
                            "checkpoint_id": None,
                        },
                    ),
                )
                for index in range(50)
            )
        )
        assert sorted(event.sequence for event in appended) == list(range(1, 51))
        first_page = await store.read_page(TENANT_ID, run.id, limit=20)
        assert [event.sequence for event in first_page.events] == list(range(1, 21))
        assert first_page.has_more is True
        replayed = [
            event
            async for event in store.iter_after(
                TENANT_ID,
                run.id,
                after=20,
                page_size=7,
            )
        ]
        assert [event.sequence for event in replayed] == list(range(21, 51))
    finally:
        await database.aclose()


@pytest.mark.asyncio
async def test_idempotent_worker_event_delivery_reuses_one_sequence(
    postgres_url: str,
) -> None:
    database = Database(DatabaseSettings(database_url=postgres_url))
    sessions = PostgresSessionRepository(database.sessions)
    runs = PostgresRunRepository(database.sessions)
    now = datetime.now(UTC)
    session = _session(now)
    run = _run(session, now)
    await sessions.create(session)
    await runs.create_idempotent(
        TENANT_ID,
        run,
        idempotency_key=f"delivery-{run.id}",
        creation_hash=run_creation_hash(priority=run.priority),
    )
    store = PostgresEventStore(database.sessions)
    draft = EventDraft(
        event_type="context.build_started",
        payload={"message_count": 1, "checkpoint_id": None},
        created_at=now,
    )
    try:
        first = await store.append_idempotent(
            TENANT_ID,
            run.id,
            "a1.g1.e1",
            draft,
        )
        replay = await store.append_idempotent(
            TENANT_ID,
            run.id,
            "a1.g1.e1",
            draft.model_copy(update={"created_at": now + timedelta(seconds=1)}),
        )
        assert replay == first
        with pytest.raises(DomainOperationError) as conflict:
            await store.append_idempotent(
                TENANT_ID,
                run.id,
                "a1.g1.e1",
                EventDraft(
                    event_type="context.build_started",
                    payload={"message_count": 2, "checkpoint_id": None},
                    created_at=now,
                ),
            )
        assert conflict.value.code == "event_delivery_conflict"
    finally:
        await database.aclose()


@pytest.mark.asyncio
async def test_reassigned_run_fences_stale_worker_event_and_tool_writes(
    postgres_url: str,
) -> None:
    database = Database(DatabaseSettings(database_url=postgres_url))
    sessions = PostgresSessionRepository(database.sessions)
    runs = PostgresRunRepository(database.sessions)
    queue = PostgresRunQueue(database.sessions)
    execution = PostgresExecutionRepository(database.sessions)
    events = PostgresEventStore(database.sessions)
    now = datetime.now(UTC)
    session = _session(now)
    run = _run(session, now).model_copy(update={"priority": 100})
    await sessions.create(session)
    await runs.create_idempotent(
        TENANT_ID,
        run,
        idempotency_key=f"fenced-worker-writes-{run.id}",
        creation_hash=run_creation_hash(priority=run.priority),
    )
    for worker_id in ("fenced-worker-a", "fenced-worker-b"):
        await queue.register_worker(
            WorkerRegistration(
                worker_id=worker_id,
                supported_sandbox_types=("podman",),
                total_slots=1,
                available_slots=1,
                status=WorkerStatus.ACTIVE,
                registered_at=now,
                last_heartbeat_at=now,
            )
        )
    draft = EventDraft(
        event_type="context.build_started",
        payload={"message_count": 1, "checkpoint_id": None},
        created_at=now,
    )
    arguments = FrozenJsonObject({"path": "README.md"})
    tool_call = ToolCall(
        id="fenced-call",
        run_id=run.id,
        turn_number=1,
        tool_name="read_file",
        arguments=arguments,
        argument_hash=canonical_argument_hash(arguments),
        status=ToolCallStatus.RECEIVED,
    )
    try:
        worker_a = await queue.claim(
            "fenced-worker-a",
            occurred_at=now,
            lease_duration=timedelta(seconds=2),
        )
        assert worker_a is not None and worker_a.run_id == run.id
        worker_a = await queue.start(worker_a, occurred_at=now + timedelta(milliseconds=1))
        first = await events.append_idempotent_fenced(worker_a, "a1.g1.e1", draft)
        assert await execution.save_tool_call_fenced(worker_a, tool_call) == tool_call

        recovered = await queue.recover_expired(
            occurred_at=now + timedelta(seconds=3),
            limit=10,
        )
        assert [item.id for item in recovered] == [run.id]
        worker_b = await queue.claim(
            "fenced-worker-b",
            occurred_at=now + timedelta(seconds=4),
            lease_duration=timedelta(seconds=30),
        )
        assert worker_b is not None and worker_b.run_id == run.id
        worker_b = await queue.start(worker_b, occurred_at=now + timedelta(seconds=5))

        with pytest.raises(DomainOperationError) as stale_event:
            await events.append_idempotent_fenced(worker_a, "a1.g1.e1", draft)
        assert stale_event.value.code == "run_lease_lost"
        with pytest.raises(DomainOperationError) as stale_tool:
            await execution.save_tool_call_fenced(worker_a, tool_call)
        assert stale_tool.value.code == "run_lease_lost"

        replay = await events.append_idempotent_fenced(worker_b, "a2.g2.e1", draft)
        assert replay.sequence == first.sequence + 1
        assert await execution.save_tool_call_fenced(worker_b, tool_call) == tool_call
    finally:
        await database.aclose()


@pytest.mark.asyncio
async def test_three_workers_claim_distinct_runs_without_overlap(
    postgres_url: str,
) -> None:
    database = Database(DatabaseSettings(database_url=postgres_url))
    sessions = PostgresSessionRepository(database.sessions)
    runs = PostgresRunRepository(database.sessions)
    queue = PostgresRunQueue(database.sessions)
    now = datetime.now(UTC)
    worker_ids = ("worker-process-1", "worker-process-2", "worker-process-3")
    for worker_id in worker_ids:
        await queue.register_worker(
            WorkerRegistration(
                worker_id=worker_id,
                supported_sandbox_types=("podman",),
                total_slots=1,
                available_slots=1,
                status=WorkerStatus.ACTIVE,
                registered_at=now,
                last_heartbeat_at=now,
            )
        )
    created_runs: list[Run] = []
    for index in range(3):
        session = _session(now + timedelta(microseconds=index))
        await sessions.create(session)
        run = _run(session, now + timedelta(microseconds=index)).model_copy(
            update={"priority": 100}
        )
        await runs.create_idempotent(
            TENANT_ID,
            run,
            idempotency_key=f"three-workers-{run.id}",
            creation_hash=run_creation_hash(priority=run.priority),
        )
        created_runs.append(run)
    try:
        leases = await asyncio.gather(
            *(
                queue.claim(
                    worker_id,
                    occurred_at=now + timedelta(seconds=1),
                    lease_duration=timedelta(seconds=30),
                )
                for worker_id in worker_ids
            )
        )
        assert all(lease is not None for lease in leases)
        assert {lease.run_id for lease in leases if lease is not None} == {
            run.id for run in created_runs
        }
        assert {lease.worker_id for lease in leases if lease is not None} == set(worker_ids)
    finally:
        await queue.recover_expired(
            occurred_at=now + timedelta(seconds=32),
            limit=10,
        )
        await database.aclose()


@pytest.mark.asyncio
async def test_worker_loss_reclaims_checkpoint_and_terminal_tool_without_duplicate(  # noqa: PLR0915
    postgres_url: str,
) -> None:
    database = Database(DatabaseSettings(database_url=postgres_url))
    sessions = PostgresSessionRepository(database.sessions)
    runs = PostgresRunRepository(database.sessions)
    execution = PostgresExecutionRepository(database.sessions)
    queue = PostgresRunQueue(database.sessions)
    workspaces = PostgresWorkspaceLeaseStore(database.sessions)
    recovery = PostgresRecoveryStore(database.sessions)
    now = datetime.now(UTC)
    session = _session(now)
    await sessions.create(session)
    primary = _run(session, now).model_copy(update={"priority": 1000})
    waiting = _run(session, now + timedelta(microseconds=1))
    for run in (primary, waiting):
        await runs.create_idempotent(
            TENANT_ID,
            run,
            idempotency_key=f"recovery-{run.id}",
            creation_hash=run_creation_hash(priority=run.priority),
        )
    await execution.append_message(
        TENANT_ID,
        PersistedMessage(
            id=uuid.uuid4(),
            session_id=session.id,
            run_id=primary.id,
            sequence=1,
            role=MessageRole.USER,
            content="recover this run",
            created_at=now,
        ),
    )
    checkpoint = Checkpoint(
        id=uuid.uuid4(),
        run_id=primary.id,
        session_id=session.id,
        message_sequence=1,
        workspace_snapshot_uri="s3://agent-platform/recovery-checkpoint",
        workspace_revision="revision-before-recovery",
        task_plan=FrozenJsonObject({"steps": [{"title": "finish", "done": False}]}),
        context_summary="durable summary",
        created_at=now + timedelta(seconds=2),
    )
    await execution.create_checkpoint(TENANT_ID, checkpoint)
    await runs.rewind(TENANT_ID, primary.id, checkpoint.id)
    arguments = FrozenJsonObject({"path": "README.md", "old": "a", "new": "b"})
    completed_tool = ToolCall(
        id="mutating-call-1",
        run_id=primary.id,
        turn_number=1,
        tool_name="edit_file",
        arguments=arguments,
        argument_hash=canonical_argument_hash(arguments),
        status=ToolCallStatus.COMPLETED,
        workspace_version="revision-after-edit",
        result=FrozenJsonObject(
            {
                "path": "README.md",
                "workspace_revision": "revision-after-edit",
            }
        ),
        started_at=now + timedelta(seconds=3),
        completed_at=now + timedelta(seconds=4),
    )
    await execution.save_tool_call(TENANT_ID, completed_tool)

    worker_ids = ("worker-a", "worker-b", "worker-c")
    for worker_id in worker_ids:
        await queue.register_worker(
            WorkerRegistration(
                worker_id=worker_id,
                supported_sandbox_types=("podman",),
                total_slots=2 if worker_id == "worker-b" else 1,
                available_slots=2 if worker_id == "worker-b" else 1,
                status=WorkerStatus.ACTIVE,
                registered_at=now,
                last_heartbeat_at=now,
            )
        )
    try:
        worker_a = await queue.claim(
            "worker-a",
            occurred_at=now + timedelta(seconds=5),
            lease_duration=timedelta(seconds=10),
        )
        assert worker_a is not None
        assert worker_a.run_id == primary.id
        worker_a = await queue.start(
            worker_a,
            occurred_at=now + timedelta(seconds=6),
        )
        writer_a = await workspaces.acquire(
            worker_a,
            occurred_at=now + timedelta(seconds=6),
            lease_duration=timedelta(seconds=10),
        )
        assert writer_a is not None

        other_lease = await queue.claim(
            "worker-b",
            occurred_at=now + timedelta(seconds=7),
            lease_duration=timedelta(seconds=10),
        )
        if other_lease is not None:
            assert other_lease.run_id != waiting.id
            assert other_lease.workspace_id != session.workspace_id

        recovered = await queue.recover_expired(
            occurred_at=now + timedelta(seconds=16),
            limit=10,
        )
        assert len(recovered) == 1
        assert recovered[0].id == primary.id
        assert recovered[0].status is RunStatus.QUEUED
        assert recovered[0].attempt == 2
        with pytest.raises(DomainOperationError) as stale:
            await queue.heartbeat(
                worker_a,
                occurred_at=now + timedelta(seconds=17),
                lease_duration=timedelta(seconds=10),
            )
        assert stale.value.code == "run_lease_lost"

        worker_b = await queue.claim(
            "worker-b",
            occurred_at=now + timedelta(seconds=17),
            lease_duration=timedelta(seconds=10),
        )
        assert worker_b is not None
        assert worker_b.run_id == primary.id
        assert worker_b.attempt == 2
        worker_b = await queue.start(
            worker_b,
            occurred_at=now + timedelta(seconds=18),
        )
        writer_b = await workspaces.acquire(
            worker_b,
            occurred_at=now + timedelta(seconds=18),
            lease_duration=timedelta(seconds=10),
        )
        assert writer_b is not None
        assert writer_b.generation > writer_a.generation

        restored = await recovery.load(worker_b)
        assert restored.checkpoint == checkpoint
        assert restored.workspace_restore_revision == "revision-after-edit"
        assert restored.context_summary == "durable summary"
        assert restored.prior_tool_outcomes[0].tool_call_id == completed_tool.id
        assert restored.prior_tool_outcomes[0].result == completed_tool.result

        completed = await queue.finish(
            worker_b,
            RunExecutionResult(
                status=RunStatus.COMPLETED,
                last_checkpoint_id=checkpoint.id,
            ),
            occurred_at=now + timedelta(seconds=19),
        )
        assert completed.status is RunStatus.COMPLETED
        assert completed.attempt == 2

        await queue.set_worker_draining(
            "worker-c",
            draining=True,
            occurred_at=now + timedelta(seconds=20),
        )
        assert (
            await queue.claim(
                "worker-c",
                occurred_at=now + timedelta(seconds=20),
                lease_duration=timedelta(seconds=10),
            )
            is None
        )

        async with database.sessions() as query:
            tool_count = await query.scalar(
                select(func.count())
                .select_from(ToolCallRecord)
                .where(ToolCallRecord.run_id == primary.id)
            )
            active_writer = await query.scalar(
                select(WorkspaceLeaseRecord).where(
                    WorkspaceLeaseRecord.tenant_id == TENANT_ID,
                    WorkspaceLeaseRecord.workspace_id == session.workspace_id,
                )
            )
            workers = await query.scalar(select(func.count()).select_from(WorkerRecord))
        assert tool_count == 1
        assert active_writer is not None
        assert active_writer.lease_token is None
        assert workers is not None and workers >= 3
    finally:
        await database.aclose()


@pytest.mark.asyncio
async def test_reassigned_worker_service_restores_post_tool_revision_and_completes(  # noqa: PLR0915
    postgres_url: str,
) -> None:
    database = Database(DatabaseSettings(database_url=postgres_url))
    sessions = PostgresSessionRepository(database.sessions)
    runs = PostgresRunRepository(database.sessions)
    execution = PostgresExecutionRepository(database.sessions)
    queue = PostgresRunQueue(database.sessions)
    recovery = PostgresRecoveryStore(database.sessions)
    workspaces = PostgresWorkspaceLeaseStore(database.sessions)
    now = datetime.now(UTC)
    session = _session(now)
    await sessions.create(session)
    await queue.register_worker(
        WorkerRegistration(
            worker_id="historical-worker",
            supported_sandbox_types=("podman",),
            total_slots=1,
            available_slots=1,
            status=WorkerStatus.ACTIVE,
            registered_at=now - timedelta(seconds=10),
            last_heartbeat_at=now - timedelta(seconds=10),
        )
    )
    previous_run = _run(session, now - timedelta(seconds=10)).model_copy(
        update={"id": uuid.uuid4()}
    )
    await runs.create_idempotent(
        TENANT_ID,
        previous_run,
        idempotency_key=f"prior-conversation-{previous_run.id}",
        creation_hash=run_creation_hash(priority=previous_run.priority),
    )
    assert await runs.transition(
        TENANT_ID,
        previous_run.id,
        RunStatus.QUEUED,
        RunStatus.LEASED,
        occurred_at=now - timedelta(seconds=9),
        worker_id="historical-worker",
        lease_expires_at=now - timedelta(seconds=5),
    )
    assert await runs.transition(
        TENANT_ID,
        previous_run.id,
        RunStatus.LEASED,
        RunStatus.RUNNING,
        occurred_at=now - timedelta(seconds=8),
    )
    assert await runs.transition(
        TENANT_ID,
        previous_run.id,
        RunStatus.RUNNING,
        RunStatus.COMPLETED,
        occurred_at=now - timedelta(seconds=7),
    )
    await execution.append_message(
        TENANT_ID,
        PersistedMessage(
            id=uuid.uuid4(),
            session_id=session.id,
            run_id=previous_run.id,
            sequence=1,
            role=MessageRole.USER,
            content="prior session context",
            created_at=now - timedelta(seconds=8),
        ),
    )
    run = _run(session, now).model_copy(update={"priority": 100})
    await runs.create_idempotent(
        TENANT_ID,
        run,
        idempotency_key=f"worker-service-recovery-{run.id}",
        creation_hash=run_creation_hash(priority=run.priority),
    )
    await execution.append_message(
        TENANT_ID,
        PersistedMessage(
            id=uuid.uuid4(),
            session_id=session.id,
            run_id=run.id,
            sequence=2,
            role=MessageRole.USER,
            content="resume safely",
            created_at=now,
        ),
    )
    earlier_arguments = FrozenJsonObject({"path": "old.txt", "old": "x", "new": "y"})
    await execution.save_tool_call(
        TENANT_ID,
        ToolCall(
            id="tool-before-selected-checkpoint",
            run_id=run.id,
            turn_number=1,
            tool_name="edit_file",
            arguments=earlier_arguments,
            argument_hash=canonical_argument_hash(earlier_arguments),
            status=ToolCallStatus.COMPLETED,
            workspace_version="obsolete-revision",
            result=FrozenJsonObject(
                {
                    "path": "old.txt",
                    "workspace_revision": "obsolete-revision",
                }
            ),
            started_at=now + timedelta(milliseconds=100),
            completed_at=now + timedelta(milliseconds=200),
        ),
    )
    checkpoint = Checkpoint(
        id=uuid.uuid4(),
        run_id=run.id,
        session_id=session.id,
        message_sequence=2,
        workspace_snapshot_uri="s3://agent-platform/worker-service-recovery",
        workspace_revision="revision-before-tool",
        task_plan=FrozenJsonObject({"steps": [{"title": "finish", "done": False}]}),
        created_at=now + timedelta(seconds=1),
    )
    await execution.create_checkpoint(TENANT_ID, checkpoint)
    await runs.rewind(TENANT_ID, run.id, checkpoint.id)
    arguments = FrozenJsonObject({"path": "README.md", "old": "a", "new": "b"})
    completed_tool = ToolCall(
        id="worker-service-tool",
        run_id=run.id,
        turn_number=1,
        tool_name="edit_file",
        arguments=arguments,
        argument_hash=canonical_argument_hash(arguments),
        status=ToolCallStatus.COMPLETED,
        workspace_version="revision-after-tool",
        result=FrozenJsonObject(
            {
                "path": "README.md",
                "workspace_revision": "revision-after-tool",
            }
        ),
        started_at=now + timedelta(seconds=2),
        completed_at=now + timedelta(seconds=3),
    )
    await execution.save_tool_call(TENANT_ID, completed_tool)
    await queue.register_worker(
        WorkerRegistration(
            worker_id="abandoned-worker",
            supported_sandbox_types=("podman",),
            total_slots=1,
            available_slots=1,
            status=WorkerStatus.ACTIVE,
            registered_at=now,
            last_heartbeat_at=now,
        )
    )
    try:
        abandoned = await queue.claim(
            "abandoned-worker",
            occurred_at=now + timedelta(seconds=4),
            lease_duration=timedelta(seconds=5),
        )
        assert abandoned is not None and abandoned.run_id == run.id
        await queue.start(abandoned, occurred_at=now + timedelta(seconds=5))
        recovered = await queue.recover_expired(
            occurred_at=now + timedelta(seconds=10),
            limit=10,
        )
        assert [item.id for item in recovered] == [run.id]

        restorer = RecordingWorkspaceRestorer()
        mutation = CountingMutationHandler()
        captured_recovery: list[RunRecoveryState] = []
        tools = ToolRegistry(
            (
                RegisteredTool(
                    name="edit_file",
                    description="Edit one workspace file",
                    arguments_type=ReplayEditArguments,
                    handler=mutation,
                    effect=ToolEffect.WORKSPACE_MUTATION,
                ),
            )
        )

        def loop_factory(
            lease: RunLease,
            writer_lease: WorkspaceWriterLease,
            recovery_state: RunRecoveryState,
        ) -> AgentLoop:
            assert writer_lease.run_id == lease.run_id
            captured_recovery.append(recovery_state)
            call = GatewayToolCall(
                id=completed_tool.id,
                name=completed_tool.tool_name,
                arguments=completed_tool.arguments,
            )
            return AgentLoop(
                gateway=ScriptedModelGateway(
                    (
                        ScriptedGatewayTurn.tool_calls(call),
                        ScriptedGatewayTurn.text("recovery complete"),
                    )
                ),
                tools=tools,
                clock=SteppingClock(now + timedelta(seconds=11)),
                id_generator=SequentialIdGenerator(),
            )

        executor = AgentLoopRunExecutor(
            loop_factory=loop_factory,
            events=PostgresEventStore(database.sessions),
            tool_calls=execution,
        )
        worker = WorkerService(
            config=WorkerConfig(worker_id="replacement-worker"),
            queue=queue,
            workspace_leases=workspaces,
            recovery=recovery,
            restorer=restorer,
            executor=executor,
            clock=SteppingWorkerClock(now + timedelta(seconds=11)),
        )
        assert await worker.run_once() is True
        await wait_for_worker_idle(worker)

        assert len(restorer.calls) == 1
        restored_run, restored_checkpoint, writer_token, restored_revision = restorer.calls[0]
        assert restored_run == run.id
        assert restored_checkpoint == checkpoint.id
        assert writer_token.version == 4
        assert restored_revision == "revision-after-tool"
        assert len(captured_recovery) == 1
        restored = captured_recovery[0]
        assert [message.content for message in restored.messages] == [
            "prior session context",
            "resume safely",
        ]
        assert len(restored.prior_tool_outcomes) == 1
        assert restored.prior_tool_outcomes[0].tool_call_id == completed_tool.id
        assert restored.prior_tool_outcomes[0].result == completed_tool.result
        assert mutation.calls == 0
        persisted = await runs.get(TENANT_ID, run.id)
        assert persisted is not None
        assert persisted.status is RunStatus.COMPLETED
        assert persisted.attempt == 2
        async with database.sessions() as query:
            tool_count = await query.scalar(
                select(func.count())
                .select_from(ToolCallRecord)
                .where(
                    ToolCallRecord.run_id == run.id,
                    ToolCallRecord.tool_call_id == completed_tool.id,
                )
            )
        assert tool_count == 1
    finally:
        await database.aclose()


@pytest.mark.asyncio
async def test_gateway_limits_and_circuits_are_shared_in_postgresql(
    postgres_url: str,
) -> None:
    database = Database(DatabaseSettings(database_url=postgres_url))
    clock = MutableClock(datetime.now(UTC))
    limiter_one = PostgresGatewayRateLimiter(
        database.sessions,
        requests_per_window=1,
        window_seconds=10,
        clock=clock,
    )
    limiter_two = PostgresGatewayRateLimiter(
        database.sessions,
        requests_per_window=1,
        window_seconds=10,
        clock=clock,
    )
    circuit_one = PostgresGatewayCircuitBreaker(
        database.sessions,
        failure_threshold=2,
        recovery_seconds=10,
        clock=clock,
    )
    circuit_two = PostgresGatewayCircuitBreaker(
        database.sessions,
        failure_threshold=2,
        recovery_seconds=10,
        clock=clock,
    )
    try:
        assert await limiter_one.acquire(TENANT_ID, "coding-default") is None
        assert await limiter_two.acquire(TENANT_ID, "coding-default") == pytest.approx(10)

        await circuit_one.record_failure("coding-default")
        await circuit_two.record_failure("coding-default")
        assert await circuit_one.allow("coding-default") is False
        clock.value += timedelta(seconds=11)
        assert await circuit_one.allow("coding-default") is True
        assert await circuit_two.allow("coding-default") is False
        clock.value += timedelta(seconds=11)
        assert await circuit_two.allow("coding-default") is True
        assert await circuit_one.allow("coding-default") is False
        await circuit_one.record_success("coding-default")
        assert await circuit_two.allow("coding-default") is True
    finally:
        await database.aclose()


@pytest.mark.asyncio
async def test_execution_entities_are_durable_and_tool_ids_fail_closed(
    postgres_url: str,
) -> None:
    database = Database(DatabaseSettings(database_url=postgres_url))
    sessions = PostgresSessionRepository(database.sessions)
    runs = PostgresRunRepository(database.sessions)
    execution = PostgresExecutionRepository(database.sessions)
    now = datetime.now(UTC)
    session = _session(now)
    await sessions.create(session)
    run = _run(session, now)
    await runs.create_idempotent(
        TENANT_ID,
        run,
        idempotency_key=f"entities-{run.id}",
        creation_hash=run_creation_hash(priority=0),
    )
    message = PersistedMessage(
        id=uuid.uuid4(),
        session_id=session.id,
        run_id=run.id,
        sequence=1,
        role=MessageRole.USER,
        content="persist this",
        created_at=now,
    )
    task_plan = PersistedTaskPlan(
        id=uuid.uuid4(),
        run_id=run.id,
        version=1,
        plan=FrozenJsonObject({"steps": []}),
        created_at=now,
    )
    arguments = FrozenJsonObject({"path": "README.md"})
    tool_call = ToolCall(
        id="tool-call-1",
        run_id=run.id,
        turn_number=1,
        tool_name="read_file",
        arguments=arguments,
        argument_hash=canonical_argument_hash(arguments),
        status=ToolCallStatus.COMPLETED,
        result=FrozenJsonObject({"content": "bounded"}),
        started_at=now,
        completed_at=now,
    )
    approval = PersistedApproval(
        id=uuid.uuid4(),
        run_id=run.id,
        status=ApprovalStatus.PENDING,
        reason="sensitive operation",
        requested_at=now,
    )
    checkpoint = Checkpoint(
        id=uuid.uuid4(),
        run_id=run.id,
        session_id=session.id,
        message_sequence=1,
        workspace_snapshot_uri="s3://agent-platform/checkpoint",
        workspace_revision="revision-1",
        task_plan=FrozenJsonObject({"steps": []}),
        created_at=now,
    )
    model_call = ModelCall(
        id="model-call-1",
        run_id=run.id,
        request_id="request-model-1",
        route_name="coding-default",
        status=ModelCallStatus.COMPLETED,
        input_tokens=10,
        output_tokens=5,
        cached_tokens=0,
        estimated_cost_usd=Decimal("0.001"),
        retry_count=1,
        fallback_count=0,
        started_at=now,
        completed_at=now,
    )
    try:
        await execution.append_message(TENANT_ID, message)
        await execution.save_task_plan(TENANT_ID, task_plan)
        await execution.save_tool_call(TENANT_ID, tool_call)
        received_replay = ToolCall(
            id=tool_call.id,
            run_id=tool_call.run_id,
            turn_number=tool_call.turn_number,
            tool_name=tool_call.tool_name,
            arguments=tool_call.arguments,
            argument_hash=tool_call.argument_hash,
            status=ToolCallStatus.RECEIVED,
        )
        assert (await execution.save_tool_call(TENANT_ID, received_replay)).status is (
            ToolCallStatus.COMPLETED
        )
        running_replay = received_replay.model_copy(
            update={"status": ToolCallStatus.RUNNING, "started_at": now}
        )
        assert (await execution.save_tool_call(TENANT_ID, running_replay)).status is (
            ToolCallStatus.COMPLETED
        )
        timestamp_replay = tool_call.model_copy(
            update={
                "started_at": now + timedelta(microseconds=1),
                "completed_at": now + timedelta(microseconds=2),
            }
        )
        assert await execution.save_tool_call(TENANT_ID, timestamp_replay) == tool_call
        await execution.create_approval(TENANT_ID, approval, tool_call_id=tool_call.id)
        await execution.create_checkpoint(TENANT_ID, checkpoint)
        await execution.save_model_call(TENANT_ID, model_call)

        conflicting_arguments = FrozenJsonObject({"path": "DESIGN.md"})
        with pytest.raises(DomainOperationError) as conflict:
            await execution.save_tool_call(
                TENANT_ID,
                tool_call.model_copy(
                    update={
                        "arguments": conflicting_arguments,
                        "argument_hash": canonical_argument_hash(conflicting_arguments),
                    }
                ),
            )
        assert getattr(conflict.value, "code", None) == "tool_call_id_conflict"
        with pytest.raises(DomainOperationError) as name_conflict:
            await execution.save_tool_call(
                TENANT_ID,
                tool_call.model_copy(update={"tool_name": "search_files"}),
            )
        assert name_conflict.value.code == "tool_call_id_conflict"

        with pytest.raises(DomainOperationError) as state_conflict:
            await execution.save_tool_call(
                TENANT_ID,
                tool_call.model_copy(update={"result": FrozenJsonObject({"content": "different"})}),
            )
        assert state_conflict.value.code == "tool_call_state_conflict"

        with pytest.raises(DomainOperationError) as model_conflict:
            await execution.save_model_call(
                TENANT_ID,
                model_call.model_copy(update={"route_name": "coding-strong"}),
            )
        assert model_conflict.value.code == "model_call_id_conflict"

        async with database.sessions() as query:
            counts = [
                await query.scalar(
                    select(func.count()).select_from(record).where(record.run_id == run.id)
                )
                for record in (
                    MessageRecord,
                    TaskPlanRecord,
                    ToolCallRecord,
                    ApprovalRecord,
                    CheckpointRecord,
                    ModelCallRecord,
                )
            ]
        assert counts == [1, 1, 1, 1, 1, 1]
    finally:
        await database.aclose()


@pytest.mark.asyncio
async def test_relational_constraints_reject_cross_entity_mismatches(
    postgres_url: str,
) -> None:
    database = Database(DatabaseSettings(database_url=postgres_url))
    sessions = PostgresSessionRepository(database.sessions)
    runs = PostgresRunRepository(database.sessions)
    execution = PostgresExecutionRepository(database.sessions)
    now = datetime.now(UTC)
    first_session = _session(now)
    second_session = _session(now)
    await sessions.create(first_session)
    await sessions.create(second_session)
    first_run = _run(first_session, now)
    second_run = _run(second_session, now)
    for run in (first_run, second_run):
        await runs.create_idempotent(
            TENANT_ID,
            run,
            idempotency_key=f"relational-{run.id}",
            creation_hash=run_creation_hash(priority=run.priority),
        )

    bad_workspace_run = _run(first_session, now).model_copy(
        update={"workspace_id": second_session.workspace_id}
    )
    with pytest.raises(IntegrityError):
        await runs.create_idempotent(
            TENANT_ID,
            bad_workspace_run,
            idempotency_key=f"bad-workspace-{bad_workspace_run.id}",
            creation_hash=run_creation_hash(priority=bad_workspace_run.priority),
        )

    mismatched_message = PersistedMessage(
        id=uuid.uuid4(),
        session_id=second_session.id,
        run_id=first_run.id,
        sequence=1,
        role=MessageRole.USER,
        content="must be rejected",
        created_at=now,
    )
    with pytest.raises(IntegrityError):
        await execution.append_message(TENANT_ID, mismatched_message)

    mismatched_checkpoint = Checkpoint(
        id=uuid.uuid4(),
        run_id=first_run.id,
        session_id=second_session.id,
        message_sequence=0,
        workspace_snapshot_uri="s3://agent-platform/invalid",
        workspace_revision="invalid",
        task_plan=FrozenJsonObject({}),
        created_at=now,
    )
    with pytest.raises(IntegrityError):
        await execution.create_checkpoint(TENANT_ID, mismatched_checkpoint)

    arguments = FrozenJsonObject({"path": "README.md"})
    second_tool = ToolCall(
        id="second-run-tool",
        run_id=second_run.id,
        turn_number=1,
        tool_name="read_file",
        arguments=arguments,
        argument_hash=canonical_argument_hash(arguments),
        status=ToolCallStatus.COMPLETED,
        result=FrozenJsonObject({"content": "bounded"}),
        started_at=now,
        completed_at=now,
    )
    await execution.save_tool_call(TENANT_ID, second_tool)
    mismatched_approval = PersistedApproval(
        id=uuid.uuid4(),
        run_id=first_run.id,
        status=ApprovalStatus.PENDING,
        reason="must not reference another run",
        requested_at=now,
    )
    with pytest.raises(IntegrityError):
        await execution.create_approval(
            TENANT_ID,
            mismatched_approval,
            tool_call_id=second_tool.id,
        )

    second_checkpoint = Checkpoint(
        id=uuid.uuid4(),
        run_id=second_run.id,
        session_id=second_session.id,
        message_sequence=0,
        workspace_snapshot_uri="s3://agent-platform/second",
        workspace_revision="second",
        task_plan=FrozenJsonObject({}),
        created_at=now,
    )
    await execution.create_checkpoint(TENANT_ID, second_checkpoint)
    with pytest.raises(IntegrityError):
        async with database.sessions() as transaction, transaction.begin():
            await transaction.execute(
                update(RunRecord)
                .where(
                    RunRecord.tenant_id == TENANT_ID,
                    RunRecord.id == first_run.id,
                )
                .values(last_checkpoint_id=second_checkpoint.id)
            )

    with pytest.raises(IntegrityError):
        async with database.sessions() as transaction, transaction.begin():
            transaction.add(
                AgentEventRecord(
                    tenant_id=TENANT_ID,
                    run_id=first_run.id,
                    sequence=1,
                    event_type="unsupported.event",
                    payload={},
                    created_at=now,
                )
            )

    worker_id = f"relational-worker-{uuid.uuid4()}"
    async with database.sessions() as transaction, transaction.begin():
        transaction.add(
            WorkerRecord(
                worker_id=worker_id,
                supported_sandbox_types=["podman"],
                total_slots=1,
                available_slots=1,
                status=WorkerStatus.ACTIVE.value,
                registered_at=now,
                last_heartbeat_at=now,
            )
        )
    with pytest.raises(IntegrityError):
        async with database.sessions() as transaction, transaction.begin():
            transaction.add(
                WorkspaceLeaseRecord(
                    tenant_id=TENANT_ID,
                    workspace_id=first_session.workspace_id,
                    run_id=second_run.id,
                    worker_id=worker_id,
                    run_lease_token=uuid.uuid4(),
                    lease_token=uuid.uuid4(),
                    generation=1,
                    acquired_at=now,
                    expires_at=now + timedelta(seconds=30),
                )
            )
    await database.aclose()


@pytest.mark.asyncio
async def test_production_api_restart_preserves_tenant_state_and_event_replay(
    postgres_url: str,
) -> None:
    api_settings = AgentApiSettings(
        api_credentials_json=json.dumps(
            {
                API_TOKEN: {
                    "tenant_id": str(TENANT_ID),
                    "subject": "postgres-user",
                },
                OTHER_API_TOKEN: {
                    "tenant_id": str(OTHER_TENANT_ID),
                    "subject": "other-user",
                },
            }
        )
    )
    database_settings = DatabaseSettings(database_url=postgres_url)
    authorization = {"Authorization": f"Bearer {API_TOKEN}"}
    other_authorization = {"Authorization": f"Bearer {OTHER_API_TOKEN}"}

    with TestClient(
        create_production_app(
            api_settings=api_settings,
            database_settings=database_settings,
        )
    ) as client:
        session_response = client.post(
            "/v1/sessions",
            headers=authorization,
            json={"workspace_id": str(uuid.uuid4())},
        )
        assert session_response.status_code == 201
        session_id = uuid.UUID(session_response.json()["id"])
        run_response = client.post(
            f"/v1/sessions/{session_id}/runs",
            headers={**authorization, "Idempotency-Key": "api-restart-run"},
            json={"priority": 3},
        )
        assert run_response.status_code == 200
        run_id = uuid.UUID(run_response.json()["run"]["id"])

    event_database = Database(database_settings)
    event_store = PostgresEventStore(event_database.sessions)
    try:
        for message_count in (1, 2):
            await event_store.append(
                TENANT_ID,
                run_id,
                EventDraft(
                    event_type="context.build_started",
                    payload={
                        "message_count": message_count,
                        "checkpoint_id": None,
                    },
                ),
            )
    finally:
        await event_database.aclose()

    with TestClient(
        create_production_app(
            api_settings=api_settings,
            database_settings=database_settings,
        )
    ) as restarted:
        assert (
            restarted.get(
                f"/v1/sessions/{session_id}",
                headers=authorization,
            ).status_code
            == 200
        )
        assert (
            restarted.get(
                f"/v1/runs/{run_id}",
                headers=other_authorization,
            ).status_code
            == 404
        )
        replay = restarted.get(
            f"/v1/runs/{run_id}/events?after=1",
            headers=authorization,
        )
        assert replay.status_code == 200
        assert [event["sequence"] for event in replay.json()["events"]] == [2]

        with restarted.websocket_connect(
            f"/v1/runs/{run_id}/stream?after=1",
            headers=authorization,
        ) as websocket:
            assert websocket.receive_json()["sequence"] == 2


def _session(now: datetime) -> Session:
    return Session(
        id=uuid.uuid4(),
        tenant_id=TENANT_ID,
        workspace_id=uuid.uuid4(),
        status=SessionStatus.ACTIVE,
        approval_mode=ApprovalMode.REQUIRE_SENSITIVE,
        model_route="coding-default",
        created_at=now,
        updated_at=now,
    )


def _run(session: Session, now: datetime) -> Run:
    return Run(
        id=uuid.uuid4(),
        session_id=session.id,
        workspace_id=session.workspace_id,
        status=RunStatus.QUEUED,
        priority=0,
        attempt=1,
        created_at=now,
    )


def _podman(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[bytes]:
    result = subprocess.run(  # noqa: S603 - fixed Podman test boundary
        (PODMAN, *arguments),
        cwd=ROOT,
        check=False,
        capture_output=True,
        timeout=90,
    )
    if check and result.returncode != 0:
        raise AssertionError(
            f"Podman command failed ({result.returncode}): "
            f"{result.stderr.decode('utf-8', errors='replace')[:2000]}"
        )
    return result
