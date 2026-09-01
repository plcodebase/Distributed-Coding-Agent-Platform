"""Production node composition for lease-authorized rootless Podman sandboxes."""

from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
import stat
import tempfile
import uuid
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import Field, StringConstraints, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from agent_core.artifacts import (
    DurableSnapshotReference,
    ObjectStore,
    SourceSnapshot,
    SourceSnapshotStatus,
    final_patch_object_key,
    workspace_checkpoint_object_key,
)
from agent_core.domain.errors import DomainOperationError
from agent_core.sandbox import WorkspaceSnapshot
from artifact_store import (
    S3ObjectStoreSettings,
    SnapshotValidator,
    WorkspaceArchiver,
    create_s3_object_store,
)
from platform_persistence import Database, DatabaseSettings, PostgresRunQueue
from platform_telemetry import PlatformTelemetry, TelemetrySettings
from sandbox_node_agent.app import create_node_agent_app
from sandbox_node_agent.registry import NodeSandboxRegistry, NodeSandboxResources
from sandbox_node_agent.tls import NodeAgentTlsSettings
from sandbox_runtime import (
    BoundedProcessRunner,
    GitWorktreeManager,
    GitWorktreeWorkspace,
    PodmanSandbox,
    PodmanSandboxConfig,
    WorkspaceToolset,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from fastapi import FastAPI

    from agent_core.artifacts import StoredObject
    from agent_core.sandbox import CommandEvent, CommandSpec
    from sandbox_node_agent.contracts import CreateSandboxRequest

_GIT_OUTPUT_LIMIT_BYTES = 256 * 1024
_GIT_TIMEOUT_SECONDS = 30.0


class ProductionNodeSettings(BaseSettings):
    """Node-local execution settings; the image is digest-pinned in production."""

    model_config = SettingsConfigDict(
        env_prefix="AGENT_NODE_",
        case_sensitive=False,
        extra="ignore",
        frozen=True,
    )

    environment: Literal["development", "test", "production"] = "production"
    sandbox_image: Annotated[str, StringConstraints(min_length=1, max_length=1024)]
    workspace_parent: str = Field(min_length=1, max_length=4096)
    podman_executable: str = Field(default="podman", min_length=1, max_length=4096)
    podman_socket: str = Field(
        default="/run/user/1000/podman/podman.sock",
        min_length=1,
        max_length=4096,
    )
    podman_home: str = Field(default="/home/agent-node", min_length=1, max_length=4096)
    git_executable: str = Field(default="git", min_length=1, max_length=4096)
    ripgrep_executable: str = Field(default="rg", min_length=1, max_length=4096)
    max_sandboxes: int = Field(default=8, ge=1, le=4096)
    cpu_limit: float = Field(default=1, gt=0, le=64)
    memory_limit_bytes: int = Field(
        default=512 * 1024 * 1024,
        ge=32 * 1024 * 1024,
        le=64 * 1024 * 1024 * 1024,
    )
    pids_limit: int = Field(default=256, ge=16, le=4096)
    tls_ca_file: str = Field(min_length=1, max_length=4096)
    tls_certificate_file: str = Field(min_length=1, max_length=4096)
    tls_private_key_file: str = Field(min_length=1, max_length=4096)
    listen_host: str = Field(
        default="0.0.0.0",  # noqa: S104 - production listener is protected by mTLS
        min_length=1,
        max_length=255,
    )
    listen_port: int = Field(default=9443, ge=1, le=65_535)

    @field_validator("workspace_parent", "podman_socket", "podman_home")
    @classmethod
    def canonical_parent(cls, value: str) -> str:
        path = Path(value)
        if not path.is_absolute() or "\x00" in value:
            raise ValueError("workspace and Podman socket paths must be absolute")
        return os.fspath(path)

    def tls_settings(self) -> NodeAgentTlsSettings:
        return NodeAgentTlsSettings(
            environment="production" if self.environment == "production" else "test",
            ca_file=self.tls_ca_file,
            certificate_file=self.tls_certificate_file,
            private_key_file=self.tls_private_key_file,
        )


class PostgresSandboxAuthorizer:
    """Authorize sandbox creation against current run and writer leases."""

    def __init__(self, queue: PostgresRunQueue) -> None:
        self._queue = queue

    async def authorize(self, request: CreateSandboxRequest) -> None:
        await self._queue.authorize_sandbox_lease(
            tenant_id=request.tenant_id,
            run_id=request.run_id,
            session_id=request.session_id,
            workspace_id=request.workspace_id,
            worker_id=request.worker_id,
            lease_token=request.run_lease_token,
            generation=request.run_lease_generation,
            execution_epoch=request.execution_epoch,
            occurred_at=datetime.now(UTC),
        )


class _OwnedSandbox:
    """Release source material only after the linked worktree and sandbox are gone."""

    def __init__(
        self,
        sandbox: PodmanSandbox,
        workspace: GitWorktreeWorkspace,
        source_root: Path,
        *,
        object_store: ObjectStore,
        archiver: WorkspaceArchiver,
        tenant_id: uuid.UUID,
        workspace_id: uuid.UUID,
        run_id: uuid.UUID,
        execution_epoch: int,
    ) -> None:
        self._sandbox = sandbox
        self._workspace = workspace
        self._source_root = source_root
        self._object_store = object_store
        self._archiver = archiver
        self._tenant_id = tenant_id
        self._workspace_id = workspace_id
        self._run_id = run_id
        self._execution_epoch = execution_epoch
        self._local_snapshots: dict[str, WorkspaceSnapshot] = {}
        self._snapshot_lock = asyncio.Lock()
        self._destroyed = False

    def execute(self, command: CommandSpec) -> AsyncIterator[CommandEvent]:
        return self._sandbox.execute(command)

    async def read_file(self, path: str) -> bytes:
        return await self._sandbox.read_file(path)

    async def write_file(self, path: str, content: bytes) -> None:
        await self._sandbox.write_file(path, content)

    async def create_snapshot(self) -> WorkspaceSnapshot:
        async with self._snapshot_lock:
            local = await self._sandbox.create_snapshot()
            checkpoint_id = uuid.uuid4()
            archive = self._source_root / f"checkpoint-{checkpoint_id.hex}.tar.gz"
            try:
                sha256, size_bytes = await asyncio.to_thread(
                    self._archiver.create,
                    self._workspace.root,
                    archive,
                )
                key = workspace_checkpoint_object_key(
                    self._tenant_id,
                    self._workspace_id,
                    self._run_id,
                    checkpoint_id,
                )
                stored = await self._object_store.upload_from_path(
                    key,
                    archive,
                    content_type="application/gzip",
                    max_bytes=size_bytes,
                )
                if stored.sha256 != sha256 or stored.size_bytes != size_bytes:
                    raise DomainOperationError(
                        code="checkpoint_upload_integrity_failed",
                        message="the uploaded checkpoint did not preserve its identity",
                        retryable=True,
                    )
                external = WorkspaceSnapshot(
                    id=checkpoint_id.hex,
                    uri=DurableSnapshotReference(
                        object_key=stored.object_key,
                        sha256=stored.sha256,
                        size_bytes=stored.size_bytes,
                    ).to_uri(),
                    revision=local.revision,
                )
                self._local_snapshots[external.id] = local
                return external
            finally:
                with suppress(OSError):
                    archive.unlink(missing_ok=True)

    async def restore_snapshot(self, snapshot: WorkspaceSnapshot) -> None:
        async with self._snapshot_lock:
            local = self._local_snapshots.get(snapshot.id)
            if local is None or local.revision != snapshot.revision:
                raise DomainOperationError(
                    code="checkpoint_not_local",
                    message="the checkpoint is not owned by this sandbox attempt",
                )
            await self._sandbox.restore_snapshot(local)

    async def final_patch(self) -> StoredObject:
        async with self._snapshot_lock:
            patch = await asyncio.to_thread(self._workspace.final_patch)
            destination = self._source_root / "final.patch"
            try:
                await asyncio.to_thread(_write_private_file, destination, patch)
                object_key = final_patch_object_key(
                    self._tenant_id,
                    self._workspace_id,
                    self._run_id,
                    self._execution_epoch,
                )
                stored = await self._object_store.upload_from_path(
                    object_key,
                    destination,
                    content_type="text/x-diff",
                    max_bytes=max(1, len(patch)),
                )
                if (
                    stored.object_key != object_key
                    or stored.sha256 != hashlib.sha256(patch).hexdigest()
                    or stored.size_bytes != len(patch)
                    or stored.content_type != "text/x-diff"
                ):
                    raise DomainOperationError(
                        code="final_patch_integrity_failed",
                        message="the uploaded final patch did not preserve its identity",
                        retryable=True,
                    )
                return stored
            finally:
                with suppress(OSError):
                    destination.unlink(missing_ok=True)

    async def current_patch(self, *, max_bytes: int) -> bytes:
        async with self._snapshot_lock:
            return await asyncio.to_thread(self._workspace.context_patch, max_bytes=max_bytes)

    async def cancel_active(self) -> None:
        await self._sandbox.cancel_active()

    async def destroy(self) -> None:
        if self._destroyed:
            return
        try:
            await self._sandbox.destroy()
        finally:
            await asyncio.to_thread(shutil.rmtree, self._source_root, True)
        self._destroyed = True


class RootlessPodmanSandboxFactory:
    """Materialize verified source, create a private Git worktree, then start Podman."""

    def __init__(
        self,
        *,
        validator: SnapshotValidator,
        object_store: ObjectStore,
        settings: ProductionNodeSettings,
        telemetry: PlatformTelemetry | None = None,
    ) -> None:
        parent = Path(settings.workspace_parent).resolve(strict=True)
        if not parent.is_dir() or not os.access(parent, os.W_OK | os.X_OK):
            raise ValueError("workspace_parent must be writable and searchable")
        self._parent = parent
        self._validator = validator
        self._object_store = object_store
        self._archiver = WorkspaceArchiver()
        self._settings = settings
        self._telemetry = telemetry
        self._git = _resolve_executable(settings.git_executable, name="git_executable")
        self._podman = _resolve_executable(
            settings.podman_executable,
            name="podman_executable",
        )
        self._podman_socket = Path(settings.podman_socket)
        socket_mode = self._podman_socket.stat().st_mode
        if not stat.S_ISSOCK(socket_mode):
            raise ValueError("podman_socket must reference the rootless Podman service socket")
        runtime_dir = _podman_runtime_directory(self._podman_socket).resolve(strict=True)
        if not runtime_dir.is_dir() or not os.access(runtime_dir, os.W_OK | os.X_OK):
            raise ValueError("the Podman runtime directory must be writable and searchable")
        podman_home = Path(settings.podman_home).resolve(strict=True)
        if not podman_home.is_dir() or not os.access(podman_home, os.W_OK | os.X_OK):
            raise ValueError("podman_home must be a writable private directory")
        self._podman_environment = {
            "CONTAINER_HOST": f"unix://{settings.podman_socket}",
            "HOME": os.fspath(podman_home),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "XDG_RUNTIME_DIR": os.fspath(runtime_dir),
        }
        self._manager = GitWorktreeManager(
            worktree_parent=parent,
            git_executable=self._git,
            ripgrep_path=settings.ripgrep_executable,
        )
        self._podman_config = PodmanSandboxConfig(
            image=settings.sandbox_image,
            environment=settings.environment,
            podman_executable=self._podman,
            cpu_limit=settings.cpu_limit,
            memory_limit_bytes=settings.memory_limit_bytes,
            pids_limit=settings.pids_limit,
        )

    async def ready(self) -> bool:
        """Probe the rootless runtime instead of trusting a stale socket pathname."""

        try:
            if not stat.S_ISSOCK(self._podman_socket.stat().st_mode):
                return False
            runner = BoundedProcessRunner()
            try:
                result = await runner.run(
                    (
                        self._podman,
                        "info",
                        "--format",
                        "{{.Host.Security.Rootless}}",
                    ),
                    cwd=self._parent,
                    timeout_seconds=5,
                    max_output_bytes=4096,
                    environment=self._podman_environment,
                )
            finally:
                await runner.close()
        except (DomainOperationError, OSError):
            return False
        stdout = "".join(
            chunk.text for chunk in result.chunks if chunk.channel.value == "stdout"
        ).strip()
        return (
            result.exit_code == 0
            and not result.timed_out
            and not result.output_truncated
            and stdout == "true"
        )

    async def create(self, request: CreateSandboxRequest) -> NodeSandboxResources:
        allocation, source = await asyncio.to_thread(_allocate_source, self._parent)
        workspace = None
        sandbox = None
        try:
            now = datetime.now(UTC)
            snapshot = SourceSnapshot(
                id=uuid.uuid5(uuid.NAMESPACE_URL, request.source_object_key),
                tenant_id=request.tenant_id,
                workspace_id=request.workspace_id,
                status=SourceSnapshotStatus.VALIDATING,
                object_key=request.source_object_key,
                expected_sha256=request.source_sha256,
                compressed_bytes=request.source_size_bytes,
                created_at=now,
                updated_at=now,
            )
            await self._validator.materialize(snapshot, source)
            await _initialize_repository(source, self._git, home=allocation)
            workspace = await _thread_cancellation_safe(
                self._manager.create,
                source,
                run_id=f"{request.run_id}-{request.run_lease_generation}",
            )
            if request.restore_object_key is not None:
                restore = allocation / "restore"
                restore.mkdir(mode=0o700)
                restore_snapshot = SourceSnapshot(
                    id=uuid.uuid5(uuid.NAMESPACE_URL, request.restore_object_key),
                    tenant_id=request.tenant_id,
                    workspace_id=request.workspace_id,
                    status=SourceSnapshotStatus.VALIDATING,
                    object_key=request.restore_object_key,
                    expected_sha256=request.restore_sha256,
                    compressed_bytes=request.restore_size_bytes,
                    created_at=now,
                    updated_at=now,
                )
                await self._validator.materialize(restore_snapshot, restore)
                await _thread_cancellation_safe(
                    _replace_worktree_from_checkpoint,
                    restore,
                    workspace.root,
                )
                await _thread_cancellation_safe(
                    workspace.commit_state,
                    label="restored durable checkpoint",
                )
                await asyncio.to_thread(shutil.rmtree, restore, True)
            sandbox = await PodmanSandbox.create(
                workspace,
                config=self._podman_config,
                control_environment=self._podman_environment,
                telemetry=self._telemetry,
            )
            owned = _OwnedSandbox(
                sandbox,
                workspace,
                allocation,
                object_store=self._object_store,
                archiver=self._archiver,
                tenant_id=request.tenant_id,
                workspace_id=request.workspace_id,
                run_id=request.run_id,
                execution_epoch=request.execution_epoch,
            )
            tools = WorkspaceToolset(workspace, sandbox=owned).registry(
                include_edit=True,
                include_command=True,
            )
            return NodeSandboxResources(
                sandbox=owned,
                tools=tools,
                final_patch_exporter=owned,
                current_patch_exporter=owned,
            )
        except BaseException:
            if sandbox is not None:
                await sandbox.destroy()
            elif workspace is not None:
                await workspace.destroy()
            await asyncio.to_thread(shutil.rmtree, allocation, True)
            raise


async def create_production_node_app(
    *,
    node_settings: ProductionNodeSettings | None = None,
    database_settings: DatabaseSettings | None = None,
    object_store_settings: S3ObjectStoreSettings | None = None,
) -> FastAPI:
    """Create the node's owned database, object-store, registry, and ASGI graph."""

    settings = node_settings or ProductionNodeSettings()
    database = Database(database_settings or DatabaseSettings())
    object_store = create_s3_object_store(
        object_store_settings or S3ObjectStoreSettings(environment=settings.environment)
    )
    telemetry = PlatformTelemetry(
        TelemetrySettings(service_name="sandbox-node-agent", environment=settings.environment)
    )
    queue = PostgresRunQueue(database.sessions)
    factory = RootlessPodmanSandboxFactory(
        validator=SnapshotValidator(object_store),
        object_store=object_store,
        settings=settings,
        telemetry=telemetry,
    )
    registry = NodeSandboxRegistry(
        authorizer=PostgresSandboxAuthorizer(queue),
        factory=factory,
        max_sandboxes=settings.max_sandboxes,
    )

    async def dependencies_ready() -> bool:
        database_ready, object_store_ready, runtime_ready = await asyncio.gather(
            database.ready(),
            object_store.ready(),
            factory.ready(),
        )
        return database_ready and object_store_ready and runtime_ready

    async def close() -> None:
        try:
            await object_store.aclose()
        finally:
            await database.aclose()
            telemetry.shutdown()

    try:
        if not await dependencies_ready():
            _raise_dependencies_unavailable()
        return create_node_agent_app(
            registry,
            close=close,
            dependencies_ready=dependencies_ready,
        )
    except BaseException:
        try:
            await registry.aclose()
        finally:
            await close()
        raise


def _write_private_file(path: Path, content: bytes) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short final patch write")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


async def _initialize_repository(source: Path, git: str, *, home: Path) -> None:
    runner = BoundedProcessRunner()
    environment = {
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_PAGER": "cat",
        "GIT_TERMINAL_PROMPT": "0",
        "HOME": os.fspath(home),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    }
    common = (
        git,
        "--no-pager",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "commit.gpgsign=false",
        "-c",
        "tag.gpgsign=false",
        "-C",
        os.fspath(source),
    )
    try:
        for arguments in (
            ("init", "--initial-branch=source", "."),
            ("add", "-A"),
            (
                "-c",
                "user.name=Agent Platform",
                "-c",
                "user.email=agent@invalid",
                "commit",
                "--no-verify",
                "--allow-empty",
                "-m",
                "immutable source snapshot",
            ),
        ):
            result = await runner.run(
                (*common, *arguments),
                cwd=source,
                timeout_seconds=_GIT_TIMEOUT_SECONDS,
                max_output_bytes=_GIT_OUTPUT_LIMIT_BYTES,
                environment=environment,
            )
            if result.timed_out or result.output_truncated or result.exit_code != 0:
                raise DomainOperationError(
                    code="source_repository_init_failed",
                    message="the validated source could not be initialized as a repository",
                )
    finally:
        await runner.close()


def _resolve_executable(value: str, *, name: str) -> str:
    resolved = value if Path(value).is_absolute() else shutil.which(value)
    if resolved is None:
        raise ValueError(f"{name} must resolve to an executable")
    candidate = Path(resolved).resolve(strict=True)
    if not stat.S_ISREG(candidate.stat().st_mode) or not os.access(candidate, os.X_OK):
        raise ValueError(f"{name} must be an executable regular file")
    return os.fspath(candidate)


async def _thread_cancellation_safe[ResultT](
    function: Callable[..., ResultT],
    *args: object,
    **kwargs: object,
) -> ResultT:
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


def _raise_dependencies_unavailable() -> None:
    raise DomainOperationError(
        code="sandbox_node_dependency_unavailable",
        message="one or more sandbox node dependencies are not ready",
        retryable=True,
    )


def _allocate_source(parent: Path) -> tuple[Path, Path]:
    allocation = Path(tempfile.mkdtemp(prefix="agent-source-", dir=parent))
    allocation.chmod(0o700)
    source = allocation / "source"
    source.mkdir(mode=0o700)
    return allocation, source


def _replace_worktree_from_checkpoint(checkpoint: Path, worktree: Path) -> None:
    """Overlay a validated full checkpoint while retaining linked-worktree metadata."""

    if not checkpoint.is_dir() or not worktree.is_dir():
        raise DomainOperationError(
            code="checkpoint_materialization_failed",
            message="checkpoint materialization paths are unavailable",
        )
    if any(entry.name.casefold() == ".git" for entry in os.scandir(checkpoint)):
        raise DomainOperationError(
            code="checkpoint_materialization_failed",
            message="checkpoint content may not replace repository metadata",
        )
    for entry in os.scandir(worktree):
        if entry.name.casefold() == ".git":
            continue
        target = Path(entry.path)
        if entry.is_dir(follow_symlinks=False):
            shutil.rmtree(target)
        else:
            target.unlink()
    shutil.copytree(
        checkpoint,
        worktree,
        dirs_exist_ok=True,
        symlinks=True,
        copy_function=shutil.copy2,
    )


def _podman_runtime_directory(socket_path: Path) -> Path:
    parent = socket_path.parent
    return parent.parent if parent.name == "podman" else parent


__all__ = [
    "PostgresSandboxAuthorizer",
    "ProductionNodeSettings",
    "RootlessPodmanSandboxFactory",
    "create_production_node_app",
]
