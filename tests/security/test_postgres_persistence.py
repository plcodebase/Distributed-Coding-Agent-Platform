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
from sqlalchemy import func, select

from agent_api.factory import AgentApiSettings, create_production_app
from agent_core.control import (
    ApprovalStatus,
    PersistedApproval,
    PersistedMessage,
    PersistedTaskPlan,
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
from agent_core.gateway import GatewayFinishReason, GatewayResponseCompleted, MessageRole
from event_store import PostgresEventStore
from gateway_client import GatewayRequestClaimStatus
from platform_persistence import (
    Database,
    DatabaseSettings,
    PostgresExecutionRepository,
    PostgresGatewayCircuitBreaker,
    PostgresGatewayRateLimiter,
    PostgresGatewayRequestStore,
    PostgresRunRepository,
    PostgresSessionRepository,
    run_creation_hash,
)
from platform_persistence.models import (
    ApprovalRecord,
    CheckpointRecord,
    MessageRecord,
    ModelCallRecord,
    TaskPlanRecord,
    ToolCallRecord,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

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
                "pg_isready",
                "-U",
                "agent_test",
                "-d",
                "agent_test",
                check=False,
            )
            if ready.returncode == 0:
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

        with pytest.raises(DomainOperationError) as model_conflict:
            await execution.save_model_call(
                TENANT_ID,
                model_call.model_copy(update={"route_name": "coding-strong"}),
            )
        assert model_conflict.value.code == "model_call_id_conflict"

        async with database.sessions() as query:
            counts = [
                await query.scalar(select(func.count()).select_from(record))
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
