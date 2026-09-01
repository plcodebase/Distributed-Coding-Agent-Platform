"""Production worker composition over PostgreSQL, LiteLLM, and the mTLS node agent."""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from typing import TYPE_CHECKING, Annotated, NoReturn

import httpx
from pydantic import Field, StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict

from agent_core.artifacts import (
    Artifact,
    ArtifactKind,
    DurableSnapshotReference,
    SourceSnapshotStatus,
    WorkspaceStatus,
)
from agent_core.checkpoints import RewindState
from agent_core.context import (
    ContextBudgetRegistry,
    ContextPipeline,
    ContextRouteBudget,
    GatewayContextCompressor,
    ReferencedContextFile,
    WorkspaceContextSnapshot,
)
from agent_core.control_tools import AgentControlToolset, AgentTaskPlanStore
from agent_core.domain.base import FrozenJsonObject
from agent_core.domain.errors import DomainOperationError
from agent_core.domain.models import Checkpoint
from agent_core.domain.status import RunStatus
from agent_core.loop import AgentLoop, AgentLoopConfig, UtcClock, UuidIdGenerator
from agent_core.settings import PlatformSettings
from agent_core.tools import ToolRegistry
from agent_core.workspace_access import WorkspaceFileReference
from agent_worker.context import DurableRunContextBuilder, PersistentRunContextSource
from agent_worker.executor import AgentLoopRunExecutor
from agent_worker.service import WorkerConfig, WorkerService
from agents_sdk_adapter import create_openai_compatible_agents_gateway
from event_store import PostgresEventStore
from gateway_client import (
    ConfiguredCostCalculator,
    GatewayClient,
    GatewayClientConfig,
    RoutePrice,
)
from platform_persistence import (
    Database,
    DatabaseSettings,
    PostgresContextRepository,
    PostgresExecutionRepository,
    PostgresGatewayCapacityStore,
    PostgresGatewayCircuitBreaker,
    PostgresGatewayRateLimiter,
    PostgresGatewayRequestStore,
    PostgresMemoryRepository,
    PostgresRecoveryStore,
    PostgresRunQueue,
    PostgresTaskRepository,
    PostgresWorkspaceLeaseStore,
    PostgresWorkspaceRepository,
)
from platform_telemetry import PlatformTelemetry, Redactor, TelemetrySettings
from queue_wakeup import RedisRunWakeup, RedisWakeupSettings
from sandbox_node_agent import (
    CreateSandboxRequest,
    NodeAgentTlsSettings,
    RemoteSandbox,
)
from sandbox_node_agent.tls import create_client_ssl_context

if TYPE_CHECKING:
    from collections.abc import Sequence

    from agent_core.artifacts import SourceSnapshot, Workspace
    from agent_core.distributed import (
        RunExecutionResult,
        RunLease,
        RunRecoveryState,
        WorkspaceWriterLease,
    )
    from agent_core.gateway import GatewayMessage
    from agent_core.sandbox import WorkspaceSnapshot

_MAX_WORKER_PROCESS_INDEX = 127
_MAX_PROJECT_INSTRUCTION_FILES = 8
_HTTP_OK = 200
_MAX_PROJECT_INSTRUCTION_BYTES = 64 * 1024
_MAX_REFERENCED_CONTEXT_BYTES = 4 * 1024 * 1024
_MAX_CURRENT_DIFF_BYTES = 1024 * 1024
_SYSTEM_INSTRUCTIONS = (
    "You are a coding agent operating inside one isolated repository workspace. "
    "Inspect before changing files, validate every tool argument, make the smallest correct "
    "change, run relevant checks, and report verified results without inventing outcomes."
)


class _DuplicateJsonKeyError(ValueError):
    pass


class ProductionWorkerSettings(BaseSettings):
    """Worker-only settings; node credentials are mounted files, never inline values."""

    model_config = SettingsConfigDict(
        env_prefix="AGENT_WORKER_",
        case_sensitive=False,
        extra="ignore",
        frozen=True,
    )

    worker_id_prefix: Annotated[
        str,
        StringConstraints(pattern=r"^[a-z][a-z0-9-]{0,31}$"),
    ] = "worker"
    instance_id: Annotated[
        str,
        StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"),
    ] = "local"
    total_slots: int = Field(default=1, ge=1, le=128)
    sandbox_slots: int = Field(default=1, ge=1, le=128)
    poll_seconds: float = Field(default=0.25, ge=0.01, le=10)
    node_agent_url: str = Field(default="https://127.0.0.1:9443", min_length=1, max_length=2048)
    node_agent_ca_file: str = Field(min_length=1, max_length=4096)
    node_agent_certificate_file: str = Field(min_length=1, max_length=4096)
    node_agent_private_key_file: str = Field(min_length=1, max_length=4096)
    route_prices_json: str = Field(min_length=2, max_length=64 * 1024)
    route_context_budgets_json: str = Field(min_length=2, max_length=64 * 1024)

    def node_tls(self, environment: str) -> NodeAgentTlsSettings:
        return NodeAgentTlsSettings(
            environment="production" if environment == "production" else "test",
            base_url=self.node_agent_url,
            ca_file=self.node_agent_ca_file,
            certificate_file=self.node_agent_certificate_file,
            private_key_file=self.node_agent_private_key_file,
        )


class RemoteAgentLoopFactory:
    """Create and own one capability-scoped node sandbox per run lease."""

    def __init__(
        self,
        *,
        gateway: GatewayClient,
        workspaces: PostgresWorkspaceRepository,
        node_tls: NodeAgentTlsSettings,
        loop_config: AgentLoopConfig,
        telemetry: PlatformTelemetry,
        redactor: Redactor,
        tasks: AgentTaskPlanStore,
        execution: PostgresExecutionRepository,
        project_instruction_paths: Sequence[str] = ("AGENTS.md",),
    ) -> None:
        self._gateway = gateway
        self._workspaces = workspaces
        self._node_tls = node_tls
        self._loop_config = loop_config
        self._telemetry = telemetry
        self._redactor = redactor
        self._tasks = tasks
        self._execution = execution
        instructions = tuple(
            WorkspaceFileReference(path=path) for path in project_instruction_paths
        )
        if not instructions or len(instructions) > _MAX_PROJECT_INSTRUCTION_FILES:
            raise ValueError("project instruction paths must contain between 1 and 8 files")
        if len({item.path for item in instructions}) != len(instructions):
            raise ValueError("project instruction paths must be unique")
        self._project_instruction_paths = instructions
        self._clock = UtcClock()
        self._ids = UuidIdGenerator()
        self._active: dict[uuid.UUID, tuple[AgentLoop, RemoteSandbox]] = {}
        self._lock = asyncio.Lock()

    async def create(
        self,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        recovery: RunRecoveryState,
    ) -> AgentLoop:
        if (
            writer_lease.run_lease_token != lease.lease_token
            or writer_lease.workspace_id != lease.workspace_id
        ):
            raise DomainOperationError(
                code="workspace_lease_mismatch",
                message="the workspace lease does not match the run lease",
                retryable=True,
            )
        workspace, snapshot = await self._source_snapshot(lease)
        if snapshot.expected_sha256 is None or snapshot.compressed_bytes is None:
            raise DomainOperationError(
                code="source_snapshot_invalid",
                message="the current source snapshot lacks verified object metadata",
            )
        restore = (
            DurableSnapshotReference.from_uri(recovery.checkpoint.workspace_snapshot_uri)
            if recovery.checkpoint is not None
            else None
        )
        remote = await RemoteSandbox.create(
            CreateSandboxRequest(
                tenant_id=lease.tenant_id,
                session_id=lease.session_id,
                run_id=lease.run_id,
                execution_epoch=lease.execution_epoch,
                workspace_id=workspace.id,
                worker_id=lease.worker_id,
                run_lease_token=lease.lease_token,
                run_lease_generation=lease.generation,
                source_object_key=snapshot.object_key,
                source_sha256=snapshot.expected_sha256,
                source_size_bytes=snapshot.compressed_bytes,
                restore_object_key=restore.object_key if restore is not None else None,
                restore_sha256=restore.sha256 if restore is not None else None,
                restore_size_bytes=restore.size_bytes if restore is not None else None,
            ),
            self._node_tls,
        )
        try:
            remote_tools = remote.tool_registry()
            control_tools = AgentControlToolset(
                tenant_id=lease.tenant_id,
                run_id=lease.run_id,
                tasks=self._tasks,
                clock=self._clock,
            ).registry()
            loop = AgentLoop(
                gateway=self._gateway,
                tools=ToolRegistry((*remote_tools.registrations, *control_tools.registrations)),
                clock=self._clock,
                id_generator=self._ids,
                config=self._loop_config,
                redactor=self._redactor,
                telemetry=self._telemetry,
                checkpoints=DurableCheckpointCoordinator(
                    lease=lease,
                    writer_lease=writer_lease,
                    sandbox=remote,
                    store=self._execution,
                    clock=self._clock,
                ),
                transcript_journal=FencedTranscriptJournal(
                    lease=lease,
                    store=self._execution,
                    clock=self._clock,
                ),
            )
            async with self._lock:
                if lease.lease_token in self._active:
                    _raise_attempt_conflict()
                self._active[lease.lease_token] = (loop, remote)
        except BaseException:
            await remote.destroy()
            raise
        else:
            return loop

    async def cleanup(self, lease: RunLease, loop: AgentLoop) -> None:
        async with self._lock:
            active = self._active.get(lease.lease_token)
            if active is None:
                return
            if active[0] is not loop:
                raise DomainOperationError(
                    code="worker_attempt_conflict",
                    message="attempt cleanup does not match the active agent loop",
                )
            remote = active[1]
        await remote.destroy()
        async with self._lock:
            self._active.pop(lease.lease_token, None)

    async def load_workspace_context(
        self,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        referenced_files: tuple[WorkspaceFileReference, ...],
    ) -> WorkspaceContextSnapshot:
        """Read model context only through the active capability-scoped node sandbox."""

        if (
            writer_lease.tenant_id != lease.tenant_id
            or writer_lease.workspace_id != lease.workspace_id
            or writer_lease.run_id != lease.run_id
            or writer_lease.run_lease_token != lease.lease_token
        ):
            raise DomainOperationError(
                code="context_workspace_lease_mismatch",
                message="workspace context does not match the active writer lease",
                retryable=True,
            )
        async with self._lock:
            active = self._active.get(lease.lease_token)
            if active is None:
                raise DomainOperationError(
                    code="context_workspace_source_unavailable",
                    message="the active workspace context source is unavailable",
                    retryable=True,
                )
            remote = active[1]

        instruction_parts: list[str] = []
        for item in self._project_instruction_paths:
            content = await _optional_workspace_file(
                remote,
                item.path,
                max_bytes=_MAX_PROJECT_INSTRUCTION_BYTES,
            )
            if content is not None:
                instruction_parts.append(
                    f"Project instructions from `{item.path}`:\n"
                    f"{_decode_workspace_context(content)}"
                )

        referenced: list[ReferencedContextFile] = []
        total_referenced_bytes = 0
        for item in referenced_files:
            content = await remote.read_file(item.path, max_bytes=1024 * 1024)
            total_referenced_bytes += len(content)
            if total_referenced_bytes > _MAX_REFERENCED_CONTEXT_BYTES:
                raise DomainOperationError(
                    code="context_referenced_files_limit",
                    message="explicitly referenced files exceed the aggregate context limit",
                )
            referenced.append(
                ReferencedContextFile(
                    path=item.path,
                    content=_decode_workspace_context(content),
                    active=True,
                )
            )

        patch = await remote.current_patch(max_bytes=_MAX_CURRENT_DIFF_BYTES)
        return WorkspaceContextSnapshot(
            project_instructions="\n\n".join(instruction_parts),
            referenced_files=tuple(referenced),
            current_git_diff=_decode_workspace_context(patch),
        )

    async def finalize(
        self,
        lease: RunLease,
        loop: AgentLoop,
        result: RunExecutionResult,
    ) -> None:
        """Persist the checksum-bound final patch before completing the run lease."""

        if result.status is not RunStatus.COMPLETED:
            return
        async with self._lock:
            active = self._active.get(lease.lease_token)
            if active is None or active[0] is not loop:
                _raise_attempt_conflict()
            remote = active[1]
        stored = await remote.final_patch()
        artifact = Artifact(
            id=uuid.uuid5(uuid.NAMESPACE_URL, stored.object_key),
            tenant_id=lease.tenant_id,
            workspace_id=lease.workspace_id,
            run_id=lease.run_id,
            execution_epoch=lease.execution_epoch,
            kind=ArtifactKind.FINAL_PATCH,
            object=stored,
            created_at=self._clock.now(),
        )
        await self._workspaces.create_artifact_fenced(lease, artifact)

    async def aclose(self) -> None:
        async with self._lock:
            active = tuple(self._active.values())
        failures: list[BaseException] = []
        for _, remote in active:
            try:
                await remote.destroy()
            except BaseException as error:
                failures.append(error)
        if failures:
            raise DomainOperationError(
                code="worker_attempt_cleanup_failed",
                message="one or more remote sandboxes could not be released",
                retryable=True,
            ) from failures[0]
        async with self._lock:
            self._active.clear()

    async def _source_snapshot(self, lease: RunLease) -> tuple[Workspace, SourceSnapshot]:
        workspace = await self._workspaces.get(lease.tenant_id, lease.workspace_id)
        if (
            workspace is None
            or workspace.status is not WorkspaceStatus.READY
            or workspace.current_snapshot_id is None
        ):
            raise DomainOperationError(
                code="workspace_not_ready",
                message="the run workspace has no ready immutable source snapshot",
                retryable=True,
            )
        snapshot = await self._workspaces.get_snapshot(
            lease.tenant_id,
            lease.workspace_id,
            workspace.current_snapshot_id,
        )
        if snapshot is None or snapshot.status is not SourceSnapshotStatus.READY:
            raise DomainOperationError(
                code="source_snapshot_not_ready",
                message="the workspace source snapshot is not ready",
                retryable=True,
            )
        return workspace, snapshot


class DurableCheckpointCoordinator:
    """Persist checksum-bound pre/post snapshots under the active run fence."""

    def __init__(
        self,
        *,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        sandbox: RemoteSandbox,
        store: PostgresExecutionRepository,
        clock: UtcClock,
    ) -> None:
        if writer_lease.run_id != lease.run_id or writer_lease.run_lease_token != lease.lease_token:
            raise DomainOperationError(
                code="workspace_lease_mismatch",
                message="the checkpoint writer lease does not match the run lease",
            )
        self._lease = lease
        self._writer_lease = writer_lease
        self._sandbox = sandbox
        self._store = store
        self._clock = clock
        self._snapshots: dict[uuid.UUID, WorkspaceSnapshot] = {}
        self._checkpoints: dict[uuid.UUID, Checkpoint] = {}
        self._lock = asyncio.Lock()

    async def create_before_tool(
        self,
        *,
        run_id: uuid.UUID,
        tool_call_id: str,
        messages: Sequence[GatewayMessage],
        task_plan: FrozenJsonObject,
        context_summary: str | None,
    ) -> Checkpoint:
        if run_id != self._lease.run_id:
            raise DomainOperationError(
                code="checkpoint_run_mismatch",
                message="the checkpoint request does not belong to this run",
            )
        async with self._lock:
            snapshot = await self._sandbox.create_snapshot()
            checkpoint_id = _snapshot_uuid(snapshot)
            DurableSnapshotReference.from_uri(snapshot.uri)
            checkpoint = Checkpoint(
                id=checkpoint_id,
                run_id=run_id,
                session_id=self._lease.session_id,
                execution_epoch=self._lease.execution_epoch,
                tool_call_id=tool_call_id,
                message_sequence=len(messages),
                messages=tuple(
                    FrozenJsonObject(message.model_dump(mode="json")) for message in messages
                ),
                workspace_snapshot_uri=snapshot.uri,
                workspace_revision=snapshot.revision,
                task_plan=task_plan,
                context_summary=context_summary,
                created_at=self._clock.now(),
            )
            await self._store.create_checkpoint_fenced(self._lease, checkpoint)
            self._snapshots[checkpoint.id] = snapshot
            self._checkpoints[checkpoint.id] = checkpoint
            return checkpoint

    async def complete_tool(
        self,
        checkpoint: Checkpoint,
        *,
        tool_call_id: str,
    ) -> str:
        if checkpoint.tool_call_id != tool_call_id:
            raise DomainOperationError(
                code="checkpoint_tool_call_mismatch",
                message="the checkpoint belongs to a different tool call",
            )
        self._require_known(checkpoint)
        async with self._lock:
            completed = await self._sandbox.create_snapshot()
            DurableSnapshotReference.from_uri(completed.uri)
            await self._store.complete_checkpoint_fenced(
                self._lease,
                checkpoint.id,
                workspace_snapshot_uri=completed.uri,
                workspace_revision=completed.revision,
            )
            return completed.revision

    async def rollback(self, checkpoint: Checkpoint) -> None:
        snapshot = self._require_known(checkpoint)
        async with self._lock:
            await self._sandbox.restore_snapshot(snapshot)

    async def rewind(self, checkpoint_id: uuid.UUID) -> RewindState:
        checkpoint = self._checkpoints.get(checkpoint_id)
        if checkpoint is None:
            raise DomainOperationError(
                code="checkpoint_not_found",
                message="the checkpoint does not belong to this sandbox attempt",
            )
        await self.rollback(checkpoint)
        return RewindState(
            checkpoint=checkpoint,
            messages=(),
            task_plan=checkpoint.task_plan,
            context_summary=checkpoint.context_summary,
            workspace_revision=checkpoint.workspace_revision,
        )

    def _require_known(self, checkpoint: Checkpoint) -> WorkspaceSnapshot:
        if checkpoint.run_id != self._lease.run_id:
            raise DomainOperationError(
                code="checkpoint_run_mismatch",
                message="the checkpoint does not belong to this run",
            )
        stored = self._checkpoints.get(checkpoint.id)
        snapshot = self._snapshots.get(checkpoint.id)
        if stored != checkpoint or snapshot is None:
            raise DomainOperationError(
                code="checkpoint_identity_mismatch",
                message="the checkpoint state does not match this sandbox attempt",
            )
        return snapshot


class FencedTranscriptJournal:
    """Append normalized conversation state before the attempt can report completion."""

    def __init__(
        self,
        *,
        lease: RunLease,
        store: PostgresExecutionRepository,
        clock: UtcClock,
    ) -> None:
        self._lease = lease
        self._store = store
        self._clock = clock

    async def append(
        self,
        run_id: uuid.UUID,
        *,
        start_index: int,
        messages: tuple[GatewayMessage, ...],
    ) -> None:
        if run_id != self._lease.run_id:
            raise DomainOperationError(
                code="transcript_run_mismatch",
                message="the transcript does not belong to the active run lease",
            )
        await self._store.append_transcript_fenced(
            self._lease,
            start_index=start_index,
            messages=messages,
            occurred_at=self._clock.now(),
        )


class ObjectCheckpointRestorer:
    """Validate durable restore metadata before node-side atomic materialization."""

    async def restore(
        self,
        lease: RunLease,
        checkpoint: Checkpoint,
        *,
        writer_lease: WorkspaceWriterLease,
        workspace_revision: str,
    ) -> None:
        if (
            checkpoint.run_id != lease.run_id
            or checkpoint.session_id != lease.session_id
            or writer_lease.run_id != lease.run_id
            or writer_lease.run_lease_token != lease.lease_token
            or checkpoint.workspace_revision != workspace_revision
        ):
            raise DomainOperationError(
                code="checkpoint_restore_mismatch",
                message="the selected checkpoint does not match the active leases",
            )
        DurableSnapshotReference.from_uri(checkpoint.workspace_snapshot_uri)


async def _optional_workspace_file(
    remote: RemoteSandbox,
    path: str,
    *,
    max_bytes: int,
) -> bytes | None:
    try:
        return await remote.read_file(path, max_bytes=max_bytes)
    except DomainOperationError as error:
        if error.code == "workspace_file_not_found":
            return None
        raise


def _decode_workspace_context(content: bytes) -> str:
    try:
        return content.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise DomainOperationError(
            code="context_workspace_encoding",
            message="workspace context must be valid UTF-8 text",
        ) from None


def _snapshot_uuid(snapshot: WorkspaceSnapshot) -> uuid.UUID:
    try:
        return uuid.UUID(hex=snapshot.id)
    except ValueError:
        raise DomainOperationError(
            code="checkpoint_protocol_error",
            message="the sandbox returned an invalid checkpoint identifier",
        ) from None


def _route_prices(raw: str, route_names: tuple[str, ...]) -> dict[str, RoutePrice]:
    try:
        value = json.loads(raw, object_pairs_hook=_unique_json_object)
    except (TypeError, ValueError):
        raise ValueError("route_prices_json must be valid JSON") from None
    if not isinstance(value, dict) or set(value) != set(route_names):
        raise ValueError("route_prices_json must define every configured route exactly once")
    try:
        prices = {
            route_name: RoutePrice.model_validate(price)
            for route_name, price in value.items()
            if isinstance(route_name, str)
        }
    except ValueError:
        raise ValueError("route_prices_json contains an invalid price") from None
    if len(prices) != len(value):
        raise ValueError("route_prices_json route names must be strings")
    return prices


def _route_budgets(
    raw: str,
    route_names: tuple[str, ...],
) -> tuple[ContextRouteBudget, ...]:
    try:
        value = json.loads(raw, object_pairs_hook=_unique_json_object)
    except (TypeError, ValueError):
        raise ValueError("route_context_budgets_json must be valid JSON") from None
    if not isinstance(value, dict) or set(value) != set(route_names):
        raise ValueError(
            "route_context_budgets_json must define every configured route exactly once"
        )
    try:
        budgets = tuple(
            ContextRouteBudget.model_validate({"route_name": route_name, **budget})
            for route_name, budget in value.items()
            if isinstance(route_name, str) and isinstance(budget, dict)
        )
    except (TypeError, ValueError):
        raise ValueError("route_context_budgets_json contains an invalid budget") from None
    if len(budgets) != len(value):
        raise ValueError("route_context_budgets_json entries must be named objects")
    return budgets


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateJsonKeyError
        value[key] = item
    return value


async def create_production_worker(
    process_index: int,
    *,
    platform_settings: PlatformSettings | None = None,
    worker_settings: ProductionWorkerSettings | None = None,
    database_settings: DatabaseSettings | None = None,
) -> WorkerService:
    """Create one fully owned worker process or fail before advertising readiness."""

    if type(process_index) is not int or not 0 <= process_index <= _MAX_WORKER_PROCESS_INDEX:
        raise ValueError("process_index must be an integer in [0, 127]")
    platform = platform_settings or PlatformSettings()
    configured = worker_settings or ProductionWorkerSettings()
    database = Database(database_settings or DatabaseSettings())
    wakeup = RedisRunWakeup.create(RedisWakeupSettings(url=platform.redis_url))
    telemetry = PlatformTelemetry(
        TelemetrySettings(
            service_name="agent-worker",
            environment=platform.environment,
        )
    )
    sdk_gateway = create_openai_compatible_agents_gateway(platform)
    gateway_config = GatewayClientConfig()
    execution = PostgresExecutionRepository(database.sessions)
    context_store = PostgresContextRepository(database.sessions)
    memory_store = PostgresMemoryRepository(database.sessions)
    gateway = GatewayClient(
        sdk_gateway,
        config=gateway_config,
        close=sdk_gateway.aclose,
        request_store=PostgresGatewayRequestStore(database.sessions),
        rate_limiter=PostgresGatewayRateLimiter(
            database.sessions,
            requests_per_window=gateway_config.rate_limit_requests,
            window_seconds=gateway_config.rate_limit_window_seconds,
        ),
        capacity_store=PostgresGatewayCapacityStore(
            database.sessions,
            provider_request_limit=gateway_config.provider_concurrent_requests,
            provider_token_limit=gateway_config.provider_tokens_per_window,
            token_window_seconds=gateway_config.provider_token_window_seconds,
        ),
        circuit_breaker=PostgresGatewayCircuitBreaker(
            database.sessions,
            failure_threshold=gateway_config.circuit_failure_threshold,
            recovery_seconds=gateway_config.circuit_recovery_seconds,
        ),
        telemetry=telemetry,
        cost_calculator=ConfiguredCostCalculator(
            _route_prices(configured.route_prices_json, gateway_config.route_names)
        ),
        model_calls=execution,
    )
    loop_factory = RemoteAgentLoopFactory(
        gateway=gateway,
        workspaces=PostgresWorkspaceRepository(database.sessions),
        node_tls=configured.node_tls(platform.environment),
        loop_config=AgentLoopConfig(
            max_turns=platform.max_turns,
            max_tool_calls=platform.max_tool_calls,
            max_semantic_retries=platform.max_semantic_retries,
            model_timeout_seconds=min(platform.max_run_seconds, 3600),
            tool_timeout_seconds=platform.command_timeout_seconds,
            max_tool_output_bytes=platform.max_command_output_bytes,
        ),
        telemetry=telemetry,
        redactor=Redactor(platform.redaction_values()),
        tasks=PostgresTaskRepository(database.sessions),
        execution=execution,
    )
    executor = AgentLoopRunExecutor(
        loop_factory=loop_factory.create,
        attempt_finalizer=loop_factory.finalize,
        loop_cleanup=loop_factory.cleanup,
        events=PostgresEventStore(database.sessions),
        tool_calls=execution,
        approvals=execution,
        context_builder=DurableRunContextBuilder(
            pipeline=ContextPipeline(
                budgets=ContextBudgetRegistry(
                    _route_budgets(
                        configured.route_context_budgets_json,
                        gateway_config.route_names,
                    )
                ),
                compressor=GatewayContextCompressor(
                    gateway=gateway,
                    id_generator=UuidIdGenerator(),
                    redactor=Redactor(platform.redaction_values()),
                ),
            ),
            source=PersistentRunContextSource(
                history=context_store,
                memories=memory_store,
                system_instructions=_SYSTEM_INSTRUCTIONS,
                redactor=Redactor(platform.redaction_values()),
                workspace_context=loop_factory,
            ),
            compactions=context_store,
            proactive_compactions=context_store,
            clock=UtcClock(),
        ),
        telemetry=telemetry,
    )

    async def close() -> None:
        try:
            await loop_factory.aclose()
        finally:
            try:
                await gateway.aclose()
            finally:
                await database.aclose()
                try:
                    await wakeup.aclose()
                finally:
                    telemetry.shutdown()

    try:
        if not await database.ready() or not await wakeup.ready():
            _raise_database_unavailable()
        await _require_node_ready(configured.node_tls(platform.environment))
        return WorkerService(
            config=WorkerConfig(
                worker_id=_worker_id(configured, process_index),
                total_slots=configured.total_slots,
                sandbox_slots=configured.sandbox_slots,
                lease_seconds=platform.lease_duration_seconds,
                heartbeat_seconds=platform.heartbeat_interval_seconds,
                poll_seconds=configured.poll_seconds,
                max_attempt_seconds=platform.max_run_seconds,
            ),
            queue=PostgresRunQueue(database.sessions, wakeup=wakeup.publish),
            workspace_leases=PostgresWorkspaceLeaseStore(database.sessions),
            recovery=PostgresRecoveryStore(database.sessions),
            restorer=ObjectCheckpointRestorer(),
            executor=executor,
            clock=UtcClock(),
            telemetry=telemetry,
            idle_wait=wakeup.wait,
            close=close,
        )
    except BaseException:
        await close()
        raise


async def _require_node_ready(settings: NodeAgentTlsSettings) -> None:
    context = create_client_ssl_context(settings)
    async with httpx.AsyncClient(
        base_url=settings.base_url,
        verify=context,
        timeout=settings.connect_timeout_seconds,
        trust_env=False,
    ) as client:
        try:
            response = await client.get("/health/ready")
        except httpx.HTTPError:
            raise DomainOperationError(
                code="sandbox_node_unavailable",
                message="the sandbox node agent is not reachable",
                retryable=True,
            ) from None
    if response.status_code != _HTTP_OK:
        raise DomainOperationError(
            code="sandbox_node_unavailable",
            message="the sandbox node agent is not ready",
            retryable=True,
        )


def _raise_attempt_conflict() -> NoReturn:
    raise DomainOperationError(
        code="worker_attempt_conflict",
        message="the worker already owns resources for this run lease",
    )


def _worker_id(settings: ProductionWorkerSettings, process_index: int) -> str:
    instance_token = hashlib.sha256(settings.instance_id.encode("utf-8")).hexdigest()[:12]
    return f"{settings.worker_id_prefix}-{instance_token}-{process_index + 1}"


def _raise_database_unavailable() -> None:
    raise DomainOperationError(
        code="persistence_unavailable",
        message="the worker database is not ready",
        retryable=True,
    )


__all__ = [
    "ObjectCheckpointRestorer",
    "ProductionWorkerSettings",
    "RemoteAgentLoopFactory",
    "create_production_worker",
]
