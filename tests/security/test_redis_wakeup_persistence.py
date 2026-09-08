from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from agent_core.artifacts import Workspace, WorkspaceStatus
from agent_core.distributed import WorkerRegistration, WorkerStatus
from agent_core.domain.models import Run, Session
from agent_core.domain.status import ApprovalMode, RunStatus, SessionStatus
from platform_persistence import (
    Database,
    DatabaseSettings,
    PostgresRunQueue,
    PostgresRunRepository,
    PostgresSessionRepository,
    PostgresWorkspaceRepository,
    run_creation_hash,
)
from queue_wakeup import RedisRunWakeup, RedisWakeupSettings

pytestmark = [
    pytest.mark.security,
    pytest.mark.skipif(
        os.getenv("AGENT_PLATFORM_RUN_REDIS_INTEGRATION") != "1",
        reason="set AGENT_PLATFORM_RUN_REDIS_INTEGRATION=1",
    ),
]

PODMAN = shutil.which("podman") or "/usr/bin/podman"
REDIS_CONTAINER_ENV = "AGENT_PLATFORM_REDIS_TEST_CONTAINER"


def _podman(*arguments: str) -> None:
    result = subprocess.run(  # noqa: S603 - exact test-owned Podman service only
        (PODMAN, *arguments),
        check=False,
        capture_output=True,
        timeout=60,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace")[:2000]
        raise AssertionError(f"Podman command failed with exit {result.returncode}: {stderr}")


async def _wait_for_redis(wakeup: RedisRunWakeup, *, expected: bool) -> None:
    for _ in range(100):
        if await wakeup.ready() is expected:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"Redis readiness did not become {expected}")


@pytest.mark.asyncio
async def test_real_redis_publish_wakes_one_waiter() -> None:
    publisher = RedisRunWakeup.create(RedisWakeupSettings())
    waiter = RedisRunWakeup.create(RedisWakeupSettings())
    try:
        assert await publisher.ready() is True
        wait_task = asyncio.create_task(waiter.wait(2))
        await asyncio.sleep(0.05)
        await publisher.publish(uuid.uuid4())
        await asyncio.wait_for(wait_task, timeout=3)
    finally:
        await waiter.aclose()
        await publisher.aclose()


@pytest.mark.asyncio
async def test_redis_outage_preserves_durable_run_and_postgres_polling() -> None:
    container_name = os.environ.get(REDIS_CONTAINER_ENV)
    assert container_name is not None and container_name.startswith(
        "agent-platform-redis-security_"
    )
    database = Database(DatabaseSettings())
    wakeup = RedisRunWakeup.create(RedisWakeupSettings())
    workspaces = PostgresWorkspaceRepository(database.sessions)
    sessions = PostgresSessionRepository(database.sessions)
    runs = PostgresRunRepository(database.sessions, wakeup=wakeup.publish)
    queue = PostgresRunQueue(database.sessions)
    tenant_id = uuid.UUID("50000000-0000-0000-0000-000000000001")
    workspace_id = uuid.UUID("50000000-0000-0000-0000-000000000002")
    session_id = uuid.UUID("50000000-0000-0000-0000-000000000003")
    run_id = uuid.UUID("50000000-0000-0000-0000-000000000004")
    now = datetime.now(UTC)
    workspace = Workspace(
        id=workspace_id,
        tenant_id=tenant_id,
        status=WorkspaceStatus.PENDING,
        display_name="Redis outage persistence",
        created_at=now,
        updated_at=now,
    )
    session = Session(
        id=session_id,
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        status=SessionStatus.ACTIVE,
        approval_mode=ApprovalMode.REQUIRE_SENSITIVE,
        model_route="coding-default",
        created_at=now,
        updated_at=now,
    )
    run = Run(
        id=run_id,
        session_id=session_id,
        workspace_id=workspace_id,
        status=RunStatus.QUEUED,
        priority=0,
        attempt=1,
        created_at=now,
    )
    redis_stopped = False
    try:
        assert await database.ready() is True
        assert await wakeup.ready() is True
        await workspaces.create(workspace)
        await sessions.create(session)
        await queue.register_worker(
            WorkerRegistration(
                worker_id="redis-fallback-worker",
                supported_sandbox_types=("podman",),
                total_slots=1,
                available_slots=1,
                status=WorkerStatus.ACTIVE,
                registered_at=now,
                last_heartbeat_at=now,
            )
        )

        await asyncio.to_thread(_podman, "stop", container_name)
        redis_stopped = True
        await _wait_for_redis(wakeup, expected=False)

        created = await runs.create_idempotent(
            tenant_id,
            run,
            idempotency_key="redis-outage-run",
            creation_hash=run_creation_hash(priority=run.priority),
        )
        assert created.created is True
        persisted = await runs.get(tenant_id, run_id)
        assert persisted is not None and persisted.status is RunStatus.QUEUED

        wait_started = time.monotonic()
        await wakeup.wait(0.05)
        assert time.monotonic() - wait_started >= 0.04
        lease = await queue.claim(
            "redis-fallback-worker",
            occurred_at=now + timedelta(seconds=1),
            lease_duration=timedelta(seconds=30),
        )
        assert lease is not None and lease.run_id == run_id

        await asyncio.to_thread(_podman, "start", container_name)
        redis_stopped = False
        await _wait_for_redis(wakeup, expected=True)
        await wakeup.publish(uuid.uuid4())
    finally:
        if redis_stopped:
            await asyncio.to_thread(_podman, "start", container_name)
        await wakeup.aclose()
        await database.aclose()
