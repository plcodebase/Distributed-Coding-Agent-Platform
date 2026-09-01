from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from agent_core.distributed import RunLease, WorkspaceWriterLease
from agent_core.domain.base import FrozenJsonObject
from agent_core.domain.errors import DomainOperationError
from agent_core.gateway import GatewayMessage, MessageRole
from agent_core.loop import UtcClock
from agent_core.sandbox import WorkspaceSnapshot
from agent_core.workspace_access import WorkspaceFileReference
from agent_scheduler.production import (
    ProductionSchedulerSettings,
    _scheduler_id,
)
from agent_scheduler.production import _route_prices as scheduler_route_prices
from agent_worker.production import (
    DurableCheckpointCoordinator,
    ProductionWorkerSettings,
    RemoteAgentLoopFactory,
    _route_budgets,
    _worker_id,
)
from agent_worker.production import _route_prices as worker_route_prices
from gateway_client import GatewayClientConfig


def _prices() -> str:
    return json.dumps(
        {
            route: {"input_usd_per_million": "1", "output_usd_per_million": "2"}
            for route in GatewayClientConfig().route_names
        }
    )


def _budgets() -> str:
    return json.dumps(
        {
            route: {"max_context_tokens": 128_000, "reserved_output_tokens": 16_000}
            for route in GatewayClientConfig().route_names
        }
    )


def _worker_settings(instance_id: str) -> ProductionWorkerSettings:
    return ProductionWorkerSettings(
        instance_id=instance_id,
        node_agent_ca_file="/tls/ca.crt",
        node_agent_certificate_file="/tls/tls.crt",
        node_agent_private_key_file="/tls/tls.key",
        route_prices_json=_prices(),
        route_context_budgets_json=_budgets(),
    )


def test_replica_identity_is_bounded_stable_and_unique() -> None:
    first = _worker_settings("pod-uid-a")
    second = _worker_settings("pod-uid-b")

    assert _worker_id(first, 0) == _worker_id(first, 0)
    assert _worker_id(first, 0) != _worker_id(second, 0)
    assert _worker_id(first, 0) != _worker_id(first, 1)

    scheduler_a = ProductionSchedulerSettings(
        instance_id="pod-uid-a",
        route_prices_json=_prices(),
    )
    scheduler_b = ProductionSchedulerSettings(
        instance_id="pod-uid-b",
        route_prices_json=_prices(),
    )
    assert _scheduler_id(scheduler_a) != _scheduler_id(scheduler_b)


def test_route_pricing_and_context_budgets_require_exact_unique_routes() -> None:
    routes = GatewayClientConfig().route_names

    assert set(worker_route_prices(_prices(), routes)) == set(routes)
    assert set(scheduler_route_prices(_prices(), routes)) == set(routes)
    assert {budget.route_name for budget in _route_budgets(_budgets(), routes)} == set(routes)

    with pytest.raises(ValueError, match="valid JSON"):
        worker_route_prices('{"coding-default": {}, "coding-default": {}}', routes)
    with pytest.raises(ValueError, match="valid JSON"):
        scheduler_route_prices('{"coding-default": {}, "coding-default": {}}', routes)
    with pytest.raises(ValueError, match="valid JSON"):
        _route_budgets('{"coding-default": {}, "coding-default": {}}', routes)


class _FixedClock(UtcClock):
    def now(self) -> datetime:
        return datetime(2026, 1, 1, tzinfo=UTC)


class _SnapshotSandbox:
    def __init__(self) -> None:
        self._next = 0

    async def create_snapshot(self) -> WorkspaceSnapshot:
        self._next += 1
        snapshot_id = uuid.UUID(int=self._next)
        return WorkspaceSnapshot(
            id=snapshot_id.hex,
            uri=(
                f"artifact:///tenants/test/checkpoints/{snapshot_id.hex}.tar.gz"
                f"?sha256={'a' * 64}&size_bytes=1"
            ),
            revision=f"revision-{self._next}",
        )


class _CheckpointStore:
    def __init__(self) -> None:
        self.created: list[object] = []

    async def create_checkpoint_fenced(self, lease: RunLease, checkpoint: object) -> object:
        del lease
        self.created.append(checkpoint)
        return checkpoint


class _ContextRemote:
    def __init__(self, files: dict[str, bytes], patch: bytes = b"") -> None:
        self.files = files
        self.patch = patch
        self.reads: list[tuple[str, int]] = []
        self.patch_limits: list[int] = []

    async def read_file(self, path: str, *, max_bytes: int) -> bytes:
        self.reads.append((path, max_bytes))
        try:
            content = self.files[path]
        except KeyError:
            raise DomainOperationError(
                code="workspace_file_not_found",
                message="the workspace file does not exist",
            ) from None
        if len(content) > max_bytes:
            raise DomainOperationError(
                code="sandbox_read_limit",
                message="the workspace file exceeds its context limit",
            )
        return content

    async def current_patch(self, *, max_bytes: int) -> bytes:
        self.patch_limits.append(max_bytes)
        return self.patch


def _context_factory() -> RemoteAgentLoopFactory:
    return RemoteAgentLoopFactory(
        gateway=cast("Any", object()),
        workspaces=cast("Any", object()),
        node_tls=cast("Any", object()),
        loop_config=cast("Any", object()),
        telemetry=cast("Any", object()),
        redactor=cast("Any", object()),
        tasks=cast("Any", object()),
        execution=cast("Any", object()),
        project_instruction_paths=("AGENTS.md", "docs/AGENTS.md"),
    )


def _active_leases() -> tuple[RunLease, WorkspaceWriterLease]:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    tenant_id = uuid.uuid4()
    run_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    run_token = uuid.uuid4()
    lease = RunLease(
        tenant_id=tenant_id,
        run_id=run_id,
        session_id=uuid.uuid4(),
        workspace_id=workspace_id,
        worker_id="worker-1",
        route_name="coding-default",
        lease_token=run_token,
        generation=1,
        attempt=1,
        priority=0,
        acquired_at=now,
        expires_at=now + timedelta(minutes=1),
    )
    writer = WorkspaceWriterLease(
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        run_id=run_id,
        worker_id="worker-1",
        run_lease_token=run_token,
        lease_token=uuid.uuid4(),
        generation=1,
        acquired_at=now,
        expires_at=now + timedelta(minutes=1),
    )
    return lease, writer


@pytest.mark.asyncio
async def test_remote_loop_factory_loads_bounded_context_from_active_sandbox() -> None:
    factory = _context_factory()
    lease, writer = _active_leases()
    remote = _ContextRemote(
        {
            "AGENTS.md": b"Use the project formatter.",
            "README.md": b"Repository overview.",
        },
        patch=b"diff --git a/a.py b/a.py\n+change\n",
    )
    factory._active[lease.lease_token] = (cast("Any", object()), cast("Any", remote))

    snapshot = await factory.load_workspace_context(
        lease,
        writer,
        (WorkspaceFileReference(path="README.md"),),
    )

    assert "Use the project formatter." in snapshot.project_instructions
    assert "docs/AGENTS.md" not in snapshot.project_instructions
    assert snapshot.referenced_files[0].path == "README.md"
    assert snapshot.referenced_files[0].content == "Repository overview."
    assert snapshot.referenced_files[0].active is True
    assert snapshot.current_git_diff.startswith("diff --git")
    assert remote.reads == [
        ("AGENTS.md", 64 * 1024),
        ("docs/AGENTS.md", 64 * 1024),
        ("README.md", 1024 * 1024),
    ]
    assert remote.patch_limits == [1024 * 1024]


@pytest.mark.asyncio
async def test_remote_loop_factory_rejects_invalid_or_excess_workspace_context() -> None:
    factory = _context_factory()
    lease, writer = _active_leases()
    invalid = _ContextRemote({"README.md": b"\xff"})
    factory._active[lease.lease_token] = (cast("Any", object()), cast("Any", invalid))

    with pytest.raises(DomainOperationError) as encoding:
        await factory.load_workspace_context(
            lease,
            writer,
            (WorkspaceFileReference(path="README.md"),),
        )
    assert encoding.value.code == "context_workspace_encoding"

    large = _ContextRemote({f"{index}.txt": b"x" * (1024 * 1024) for index in range(5)})
    factory._active[lease.lease_token] = (cast("Any", object()), cast("Any", large))
    with pytest.raises(DomainOperationError) as aggregate:
        await factory.load_workspace_context(
            lease,
            writer,
            tuple(WorkspaceFileReference(path=f"{index}.txt") for index in range(5)),
        )
    assert aggregate.value.code == "context_referenced_files_limit"


@pytest.mark.asyncio
async def test_durable_checkpoints_bind_each_same_turn_mutation_to_its_tool_call() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    tenant_id = uuid.uuid4()
    run_id = uuid.uuid4()
    session_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    run_token = uuid.uuid4()
    lease = RunLease(
        tenant_id=tenant_id,
        run_id=run_id,
        session_id=session_id,
        workspace_id=workspace_id,
        worker_id="worker-1",
        route_name="coding-default",
        lease_token=run_token,
        generation=1,
        attempt=1,
        priority=0,
        acquired_at=now,
        expires_at=now + timedelta(minutes=1),
    )
    writer_lease = WorkspaceWriterLease(
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        run_id=run_id,
        worker_id="worker-1",
        run_lease_token=run_token,
        lease_token=uuid.uuid4(),
        generation=1,
        acquired_at=now,
        expires_at=now + timedelta(minutes=1),
    )
    store = _CheckpointStore()
    coordinator = DurableCheckpointCoordinator(
        lease=lease,
        writer_lease=writer_lease,
        sandbox=_SnapshotSandbox(),  # type: ignore[arg-type]
        store=store,  # type: ignore[arg-type]
        clock=_FixedClock(),
    )
    messages = (GatewayMessage(role=MessageRole.USER, content="make two changes"),)

    first = await coordinator.create_before_tool(
        run_id=run_id,
        tool_call_id="mutation-a",
        messages=messages,
        task_plan=FrozenJsonObject({}),
        context_summary=None,
    )
    second = await coordinator.create_before_tool(
        run_id=run_id,
        tool_call_id="mutation-b",
        messages=messages,
        task_plan=FrozenJsonObject({}),
        context_summary=None,
    )

    assert first.message_sequence == second.message_sequence == 1
    assert first.tool_call_id == "mutation-a"
    assert second.tool_call_id == "mutation-b"
    assert first.id != second.id
    assert store.created == [first, second]

    with pytest.raises(DomainOperationError) as mismatch:
        await coordinator.complete_tool(first, tool_call_id="mutation-b")
    assert mismatch.value.code == "checkpoint_tool_call_mismatch"
