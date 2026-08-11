"""Hardened rootless Podman sandbox adapter."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import re
import shutil
import stat
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Annotated, Literal, Self

from pydantic import Field, StringConstraints, field_validator, model_validator

from agent_core.domain.base import DomainModel
from agent_core.domain.errors import DomainOperationError
from agent_core.sandbox import (
    CommandCompleted,
    CommandEvent,
    CommandOutput,
    CommandSpec,
    WorkspaceSnapshot,
)
from platform_telemetry import PlatformTelemetry, TelemetryContext
from sandbox_runtime._process import (
    BoundedProcessRunner,
    ProcessChunk,
    ProcessResult,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Mapping
    from types import TracebackType

    from sandbox_runtime.git_workspace import GitWorktreeWorkspace

_OUTPUT_QUEUE_CHUNKS = 16
_MAX_CONTROL_ENVIRONMENT_BYTES = 64 * 1024
_MAX_CONTAINER_NAME_CHARACTERS = 63
_MAX_IMAGE_ID_CHARACTERS = 255
_PODMAN_RUNTIME_ERROR_EXIT = 125
_CONTAINER_TMP = "/tmp"  # noqa: S108 - isolated container tmpfs, never a host path
_CONTAINER_PATH = "/usr/local/bin:/usr/bin:/bin"
_CONTROL_ENVIRONMENT_NAMES = (
    "CONTAINER_HOST",
    "CONTAINER_SSHKEY",
    "HOME",
    "LANG",
    "LC_ALL",
    "PATH",
    "SSH_AUTH_SOCK",
    "XDG_RUNTIME_DIR",
)
_IMAGE_DIGEST = re.compile(r"@sha256:[0-9a-f]{64}\Z")


class PodmanSandboxConfig(DomainModel):
    """Validated immutable limits for one hardened Podman sandbox."""

    image: Annotated[str, StringConstraints(min_length=1, max_length=1024)]
    environment: Literal["development", "test", "production"] = "production"
    podman_executable: Annotated[str, StringConstraints(min_length=1, max_length=4096)] = "podman"
    cpu_limit: float = Field(default=1.0, gt=0, le=64)
    memory_limit_bytes: int = Field(
        default=512 * 1024 * 1024,
        ge=32 * 1024 * 1024,
        le=64 * 1024 * 1024 * 1024,
    )
    pids_limit: int = Field(default=256, ge=16, le=4096)
    open_files_limit: int = Field(default=1024, ge=64, le=1_048_576)
    tmpfs_limit_bytes: int = Field(
        default=64 * 1024 * 1024,
        ge=1024 * 1024,
        le=4 * 1024 * 1024 * 1024,
    )
    max_write_bytes: int = Field(default=4 * 1024 * 1024, ge=1, le=100 * 1024 * 1024)
    max_snapshots: int = Field(default=1000, ge=1, le=10_000)
    control_timeout_seconds: float = Field(default=30.0, gt=0, le=120)
    control_output_bytes: int = Field(default=256 * 1024, ge=1024, le=1024 * 1024)
    container_uid: int = Field(default=10001, ge=1, le=2_147_483_647)
    container_gid: int = Field(default=10001, ge=1, le=2_147_483_647)
    network_mode: Literal["none"] = "none"

    @field_validator("image", "podman_executable")
    @classmethod
    def reject_unsafe_text(cls, value: str) -> str:
        if (
            value != value.strip()
            or "\x00" in value
            or any(character.isspace() for character in value)
            or value.startswith("-")
        ):
            raise ValueError("container image and executable values must be canonical text")
        return value

    @model_validator(mode="after")
    def require_production_digest(self) -> Self:
        if self.environment == "production" and _IMAGE_DIGEST.search(self.image) is None:
            raise ValueError("production sandbox images must be pinned by SHA-256 digest")
        return self


@dataclass(frozen=True, slots=True)
class _ActiveCommand:
    container_name: str
    task: asyncio.Task[ProcessResult]


def _sandbox_error(
    code: str,
    message: str,
    *,
    retryable: bool = False,
) -> DomainOperationError:
    return DomainOperationError(
        code=code,
        message=message,
        retryable=retryable,
    )


def _resolve_executable(value: str) -> str:
    resolved = value if Path(value).is_absolute() else shutil.which(value)
    if resolved is None:
        raise ValueError("podman_executable must resolve to an executable")
    candidate = Path(resolved).resolve(strict=True)
    mode = candidate.stat().st_mode
    if not stat.S_ISREG(mode) or not os.access(candidate, os.X_OK):
        raise ValueError("podman_executable must be an executable regular file")
    return os.fspath(candidate)


def _control_environment(source: Mapping[str, str]) -> dict[str, str]:
    environment: dict[str, str] = {}
    for name in _CONTROL_ENVIRONMENT_NAMES:
        value = source.get(name)
        if value:
            environment[name] = value
    environment.setdefault("LANG", "C.UTF-8")
    environment.setdefault("LC_ALL", "C.UTF-8")
    size = sum(
        len(name.encode("utf-8")) + len(value.encode("utf-8"))
        for name, value in environment.items()
    )
    if size > _MAX_CONTROL_ENVIRONMENT_BYTES:
        raise ValueError("Podman control environment exceeds its byte limit")
    return environment


def _rootless_from_info(value: object) -> bool | None:
    if not isinstance(value, dict):
        return None
    host = value.get("host", value.get("Host"))
    if not isinstance(host, dict):
        return None
    security = host.get("security", host.get("Security"))
    if not isinstance(security, dict):
        return None
    rootless = security.get("rootless", security.get("Rootless"))
    return rootless if isinstance(rootless, bool) else None


class PodmanSandbox:
    """Execute each command in a disposable, resource-limited rootless container."""

    def __init__(
        self,
        workspace: GitWorktreeWorkspace,
        *,
        config: PodmanSandboxConfig,
        podman_executable: str,
        control_environment: Mapping[str, str],
        runner: BoundedProcessRunner,
        owns_runner: bool,
        id_factory: Callable[[], uuid.UUID],
    ) -> None:
        self._workspace = workspace
        self._config = config
        self._podman = podman_executable
        self._control_environment = dict(control_environment)
        self._runner = runner
        self._owns_runner = owns_runner
        sandbox_id = id_factory()
        if not isinstance(sandbox_id, uuid.UUID):
            raise TypeError("sandbox id_factory must return UUID values")
        self._sandbox_token = hashlib.sha256(sandbox_id.bytes).hexdigest()[:16]
        self._next_command = 0
        self._generation = 0
        self._active: dict[str, _ActiveCommand] = {}
        self._pending_container_cleanup: set[str] = set()
        self._snapshots: dict[str, WorkspaceSnapshot] = {}
        self._snapshot_order: list[str] = []
        self._cancelling = False
        self._destroying = False
        self._cleanup_required = False
        self._destroyed = False
        self._workspace_operation = False
        self._command_slot = asyncio.Semaphore(1)
        self._state_lock = asyncio.Lock()
        self._cancel_lock = asyncio.Lock()
        self._container_cleanup_lock = asyncio.Lock()
        self._destroy_lock = asyncio.Lock()
        self._workspace_lock = asyncio.Lock()

    @classmethod
    async def create(
        cls,
        workspace: GitWorktreeWorkspace,
        *,
        config: PodmanSandboxConfig,
        runner: BoundedProcessRunner | None = None,
        control_environment: Mapping[str, str] | None = None,
        id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
        telemetry: PlatformTelemetry | None = None,
        telemetry_context: TelemetryContext | None = None,
    ) -> Self:
        """Validate the rootless runtime and configured image before returning."""

        if not isinstance(config, PodmanSandboxConfig):
            raise TypeError("config must be a PodmanSandboxConfig")
        if not callable(id_factory):
            raise TypeError("id_factory must be callable")
        if "," in os.fspath(workspace.root):
            raise ValueError("workspace path may not contain a Podman mount separator")
        executable = _resolve_executable(config.podman_executable)
        environment = _control_environment(control_environment or os.environ)
        actual_runner = runner or BoundedProcessRunner()
        sandbox = cls(
            workspace,
            config=config,
            podman_executable=executable,
            control_environment=environment,
            runner=actual_runner,
            owns_runner=runner is None,
            id_factory=id_factory,
        )
        started = time.monotonic()
        outcome = "error"
        try:
            if telemetry is None:
                await sandbox._verify_runtime()
            else:
                with telemetry.span(
                    "sandbox.startup",
                    context=telemetry_context or TelemetryContext(),
                ):
                    await sandbox._verify_runtime()
            outcome = "success"
        except BaseException:
            if sandbox._owns_runner:
                await actual_runner.close()
            raise
        finally:
            if telemetry is not None:
                telemetry.metrics.sandbox_startup.labels(outcome=outcome).observe(
                    time.monotonic() - started
                )
        return sandbox

    @property
    def workspace(self) -> GitWorktreeWorkspace:
        return self._workspace

    async def __aenter__(self) -> Self:
        async with self._state_lock:
            self._require_available()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        await self.destroy()

    async def execute(  # noqa: PLR0912, PLR0915 - explicit secure lifecycle
        self,
        command: CommandSpec,
    ) -> AsyncIterator[CommandEvent]:
        if not isinstance(command, CommandSpec):
            raise _sandbox_error("command_invalid", "the command contract is invalid")
        async with self._state_lock:
            self._require_available()
            generation = self._generation

        await self._command_slot.acquire()
        active: _ActiveCommand | None = None
        queue: asyncio.Queue[ProcessChunk] = asyncio.Queue(maxsize=_OUTPUT_QUEUE_CHUNKS)
        try:
            async with self._state_lock:
                self._require_available()
                if generation != self._generation or self._cancelling or self._workspace_operation:
                    raise _sandbox_error(
                        "command_cancelled",
                        "the command was invalidated before container creation",
                        retryable=True,
                    )
                cwd = self._workspace.resolve_path(command.cwd)
                if not cwd.is_dir():
                    raise _sandbox_error(
                        "invalid_command_cwd",
                        "the command working directory is not a directory",
                    )
                container_name = self._new_container_name()

                async def receive(chunk: ProcessChunk) -> None:
                    await queue.put(chunk)

                task = asyncio.create_task(
                    self._runner.run(
                        self._command_argv(
                            command,
                            container_name=container_name,
                            cwd=cwd,
                        ),
                        cwd=self._workspace.root,
                        timeout_seconds=command.timeout_seconds,
                        max_output_bytes=command.max_output_bytes,
                        environment=self._control_environment,
                        on_chunk=receive,
                        retain_output=False,
                    )
                )
                active = _ActiveCommand(container_name=container_name, task=task)
                self._active[container_name] = active

            sequence = 0
            while not active.task.done() or not queue.empty():
                if queue.empty() and not active.task.done():
                    get_task = asyncio.create_task(queue.get())
                    done, _ = await asyncio.wait(
                        {get_task, active.task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if get_task in done:
                        chunk = get_task.result()
                    else:
                        get_task.cancel()
                        await asyncio.gather(get_task, return_exceptions=True)
                        continue
                else:
                    chunk = queue.get_nowait()
                sequence += 1
                yield CommandOutput(
                    sequence=sequence,
                    channel=chunk.channel,
                    chunk=chunk.text,
                )
            result = await active.task
        finally:
            cleanup_error: Exception | None = None
            if active is not None:
                if not active.task.done():
                    active.task.cancel()
                await asyncio.gather(active.task, return_exceptions=True)
                owns_cleanup = await self._claim_container_cleanup(active.container_name)
                if owns_cleanup:
                    try:
                        await self._remove_container(active.container_name)
                    except Exception as error:
                        cleanup_error = error
                async with self._state_lock:
                    if cleanup_error is not None and not self._destroyed:
                        self._cleanup_required = True
            self._command_slot.release()
            if cleanup_error is not None:
                raise _sandbox_error(
                    "sandbox_cleanup_failed",
                    "the command container could not be removed",
                    retryable=True,
                ) from cleanup_error

        if result.exit_code == _PODMAN_RUNTIME_ERROR_EXIT:
            raise _sandbox_error(
                "sandbox_command_start_failed",
                "Podman could not create or start the command container",
                retryable=True,
            )
        yield CommandCompleted(
            exit_code=result.exit_code,
            timed_out=result.timed_out,
            output_truncated=result.output_truncated,
        )

    async def read_file(self, path: str) -> bytes:
        async with self._exclusive_workspace_operation():
            return await self._thread_cancellation_safe(self._workspace.file_bytes, path)

    async def write_file(self, path: str, content: bytes) -> None:
        if not isinstance(content, bytes):
            raise _sandbox_error("sandbox_write_invalid", "sandbox writes require bytes")
        if len(content) > self._config.max_write_bytes:
            raise DomainOperationError(
                code="sandbox_write_limit",
                message="sandbox write content exceeds the configured byte limit",
                details={"limit_bytes": self._config.max_write_bytes},
            )
        async with self._exclusive_workspace_operation():
            await self._thread_cancellation_safe(
                self._workspace.write_file_atomic,
                path,
                content,
            )

    async def create_snapshot(self) -> WorkspaceSnapshot:
        async with self._exclusive_workspace_operation():
            snapshot = await self._thread_cancellation_safe(
                self._workspace.create_snapshot,
                label="Podman sandbox snapshot",
            )
            existing = self._snapshots.get(snapshot.id)
            if existing is not None and existing != snapshot:
                raise _sandbox_error(
                    "snapshot_identity_conflict",
                    "the snapshot identifier conflicts with retained state",
                )
            if existing is None:
                if len(self._snapshots) >= self._config.max_snapshots:
                    raise _sandbox_error(
                        "snapshot_limit",
                        "the sandbox snapshot limit has been reached",
                    )
                self._snapshots[snapshot.id] = snapshot
                self._snapshot_order.append(snapshot.id)
            return snapshot

    async def restore_snapshot(self, snapshot: WorkspaceSnapshot) -> None:
        if not isinstance(snapshot, WorkspaceSnapshot):
            raise _sandbox_error("snapshot_invalid", "the snapshot contract is invalid")
        async with self._workspace_lock:
            async with self._state_lock:
                self._require_available()
                retained = self._snapshots.get(snapshot.id)
                if retained is None:
                    raise _sandbox_error(
                        "snapshot_not_found",
                        "the snapshot is not owned by this sandbox",
                    )
                if retained != snapshot:
                    raise _sandbox_error(
                        "snapshot_identity_mismatch",
                        "the snapshot does not match retained immutable state",
                    )
                self._workspace_operation = True
            try:
                await self.cancel_active()
                await self._thread_cancellation_safe(
                    self._workspace.restore_revision,
                    snapshot.revision,
                )
                index = self._snapshot_order.index(snapshot.id)
                for removed_id in self._snapshot_order[index + 1 :]:
                    self._snapshots.pop(removed_id, None)
                del self._snapshot_order[index + 1 :]
            finally:
                async with self._state_lock:
                    self._workspace_operation = False

    async def cancel_active(self) -> None:
        task = asyncio.create_task(self._cancel_active())
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def destroy(self) -> None:
        task = asyncio.create_task(self._destroy())
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def _verify_runtime(self) -> None:
        info_text = await self._control(("info", "--format", "json"))
        try:
            info: object = json.loads(info_text)
        except (TypeError, ValueError) as error:
            raise _sandbox_error(
                "sandbox_runtime_protocol_error",
                "Podman returned malformed runtime information",
            ) from error
        if _rootless_from_info(info) is not True:
            raise _sandbox_error(
                "sandbox_rootless_required",
                "the sandbox requires a rootless Podman engine",
            )
        image_id = (
            await self._control(
                (
                    "image",
                    "inspect",
                    "--format",
                    "{{.Id}}",
                    self._config.image,
                )
            )
        ).strip()
        if (
            not image_id
            or len(image_id) > _MAX_IMAGE_ID_CHARACTERS
            or any(character.isspace() for character in image_id)
        ):
            raise _sandbox_error(
                "sandbox_runtime_protocol_error",
                "Podman returned invalid image identity",
            )

    async def _cancel_active(self) -> None:
        async with self._cancel_lock:
            async with self._state_lock:
                self._generation += 1
                self._cancelling = True
                active = tuple(self._active.values())
                for item in active:
                    item.task.cancel()
            failures: list[BaseException] = []
            results = await asyncio.gather(
                *(item.task for item in active),
                return_exceptions=True,
            )
            failures.extend(
                result
                for result in results
                if isinstance(result, BaseException)
                and not isinstance(result, asyncio.CancelledError)
            )
            initial_cleanup_failures: dict[str, Exception] = {}
            for item in active:
                owns_cleanup = await self._claim_container_cleanup(item.container_name)
                if owns_cleanup:
                    try:
                        await self._remove_container(item.container_name)
                    except Exception as error:
                        initial_cleanup_failures[item.container_name] = error
            # A command generator may have claimed its own cleanup just before
            # cancellation did. Wait for that targeted removal before the
            # owning process runner can be closed by destroy().
            async with self._container_cleanup_lock:
                pass
            retry_failures = await self._retry_pending_container_cleanup()
            for container_name, retry_failure in retry_failures.items():
                initial_failure = initial_cleanup_failures.get(container_name)
                if initial_failure is not None:
                    retry_failure.add_note(
                        f"initial targeted removal failed: {type(initial_failure).__name__}"
                    )
                failures.append(retry_failure)
            async with self._state_lock:
                self._cancelling = False
                if failures:
                    self._cleanup_required = True
            if failures:
                raise _sandbox_error(
                    "sandbox_cancel_failed",
                    "one or more command containers could not be terminated",
                    retryable=True,
                ) from failures[0]

    async def _destroy(self) -> None:
        async with self._destroy_lock:
            async with self._state_lock:
                if self._destroyed:
                    return
                self._destroying = True
            try:
                await self.cancel_active()
                if self._owns_runner:
                    await self._runner.close()
                await self._workspace.destroy()
            except Exception as error:
                async with self._state_lock:
                    self._destroying = False
                    self._cleanup_required = True
                raise _sandbox_error(
                    "sandbox_cleanup_failed",
                    "sandbox cleanup failed and may be retried",
                    retryable=True,
                ) from error
            async with self._state_lock:
                self._destroying = False
                self._cleanup_required = False
                self._destroyed = True
                self._snapshots.clear()
                self._snapshot_order.clear()

    def _command_argv(
        self,
        command: CommandSpec,
        *,
        container_name: str,
        cwd: Path,
    ) -> tuple[str, ...]:
        relative_cwd = cwd.relative_to(self._workspace.root)
        container_cwd = PurePosixPath("/workspace", *relative_cwd.parts).as_posix()
        user = f"{self._config.container_uid}:{self._config.container_gid}"
        keep_id = f"keep-id:uid={self._config.container_uid},gid={self._config.container_gid}"
        mount = f"type=bind,source={self._workspace.root},destination=/workspace,rw,nodev,nosuid"
        return (
            self._podman,
            "run",
            "--name",
            container_name,
            "--rm",
            "--restart=no",
            "--pull=never",
            "--read-only",
            "--network",
            self._config.network_mode,
            "--http-proxy=false",
            "--image-volume=ignore",
            "--unsetenv-all",
            "--cap-drop=ALL",
            "--security-opt",
            "no-new-privileges",
            "--seccomp-policy",
            "default",
            "--userns",
            keep_id,
            "--user",
            user,
            "--pid",
            "private",
            "--cgroupns",
            "private",
            "--pids-limit",
            str(self._config.pids_limit),
            "--memory",
            str(self._config.memory_limit_bytes),
            "--memory-swap",
            str(self._config.memory_limit_bytes),
            "--cpus",
            format(self._config.cpu_limit, "g"),
            "--ulimit",
            f"nofile={self._config.open_files_limit}:{self._config.open_files_limit}",
            "--tmpfs",
            (f"{_CONTAINER_TMP}:rw,nosuid,nodev,size={self._config.tmpfs_limit_bytes},mode=1777"),
            "--mount",
            mount,
            "--workdir",
            container_cwd,
            "--env",
            f"HOME={_CONTAINER_TMP}",
            "--env",
            "LANG=C.UTF-8",
            "--env",
            "LC_ALL=C.UTF-8",
            "--env",
            f"PATH={_CONTAINER_PATH}",
            "--init",
            "--no-healthcheck",
            "--systemd=false",
            "--log-driver=none",
            "--ipc=private",
            "--uts=private",
            "--no-hosts",
            "--timeout",
            str(math.ceil(command.timeout_seconds)),
            "--stop-timeout=1",
            "--label",
            f"io.agent-platform.sandbox={self._sandbox_token}",
            "--entrypoint",
            command.argv[0],
            self._config.image,
            *command.argv[1:],
        )

    async def _control(
        self,
        arguments: tuple[str, ...],
    ) -> str:
        result = await self._runner.run(
            (self._podman, *arguments),
            cwd=self._workspace.root,
            timeout_seconds=self._config.control_timeout_seconds,
            max_output_bytes=self._config.control_output_bytes,
            environment=self._control_environment,
        )
        if result.timed_out:
            raise _sandbox_error(
                "sandbox_runtime_timeout",
                "Podman control operation timed out",
                retryable=True,
            )
        if result.output_truncated:
            raise _sandbox_error(
                "sandbox_runtime_output_limit",
                "Podman control output exceeded its configured limit",
            )
        if result.exit_code != 0:
            raise _sandbox_error(
                "sandbox_runtime_unavailable",
                "Podman control operation failed",
                retryable=True,
            )
        return "".join(chunk.text for chunk in result.chunks if chunk.channel.value == "stdout")

    async def _remove_container(self, container_name: str) -> None:
        # Cancellation and the execution generator's finally block can converge
        # on the same disposable container. Podman removal is targeted and
        # idempotent, but serializing it avoids a runtime-level removal race.
        async with self._container_cleanup_lock:
            await self._control(
                (
                    "rm",
                    "--force",
                    "--ignore",
                    "--time",
                    "0",
                    container_name,
                )
            )
            async with self._state_lock:
                self._pending_container_cleanup.discard(container_name)

    async def _claim_container_cleanup(self, container_name: str) -> bool:
        async with self._state_lock:
            if self._active.pop(container_name, None) is None:
                return False
            self._pending_container_cleanup.add(container_name)
            return True

    async def _retry_pending_container_cleanup(self) -> dict[str, BaseException]:
        async with self._state_lock:
            pending = tuple(sorted(self._pending_container_cleanup))
        failures: dict[str, BaseException] = {}
        for container_name in pending:
            try:
                await self._remove_container(container_name)
            except Exception as error:
                failures[container_name] = error
        return failures

    def _new_container_name(self) -> str:
        self._next_command += 1
        name = f"agent-sbx-{self._sandbox_token}-{self._next_command}"
        if len(name) > _MAX_CONTAINER_NAME_CHARACTERS:
            raise AssertionError("generated Podman container name exceeded its bound")
        return name

    @asynccontextmanager
    async def _exclusive_workspace_operation(self) -> AsyncIterator[None]:
        async with self._workspace_lock:
            async with self._state_lock:
                self._require_available()
                if self._active or self._cancelling:
                    raise _sandbox_error(
                        "sandbox_busy",
                        "workspace access requires all containers to be stopped",
                        retryable=True,
                    )
                self._workspace_operation = True
            try:
                yield
            finally:
                async with self._state_lock:
                    self._workspace_operation = False

    def _require_available(self) -> None:
        if self._destroyed:
            raise _sandbox_error(
                "sandbox_destroyed",
                "the Podman sandbox has been destroyed",
            )
        if self._destroying or self._cleanup_required:
            raise _sandbox_error(
                "sandbox_cleanup_required",
                "the Podman sandbox requires cleanup before reuse",
                retryable=True,
            )

    @staticmethod
    async def _thread_cancellation_safe[**P, T](
        function: Callable[P, T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise


__all__ = ["PodmanSandbox", "PodmanSandboxConfig"]
