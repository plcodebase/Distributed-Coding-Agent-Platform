"""Explicitly unsafe development-only host process adapter."""

from __future__ import annotations

import asyncio
import os
import re
import shutil
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from agent_core.domain.errors import DomainOperationError
from agent_core.sandbox import (
    CommandCompleted,
    CommandEvent,
    CommandOutput,
    CommandSpec,
    WorkspaceSnapshot,
)
from sandbox_runtime._process import (
    BoundedProcessRunner,
    ProcessChunk,
    ProcessResult,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Mapping

    from sandbox_runtime.git_workspace import GitWorktreeWorkspace

_OUTPUT_QUEUE_CHUNKS = 16
_MAX_CONCURRENT_COMMANDS = 64
_DEFAULT_MAX_WRITE_BYTES = 4 * 1024 * 1024
_DEFAULT_MAX_SNAPSHOTS = 1000
_MAX_EXECUTABLE_PATH_BYTES = 16 * 1024
_MAX_ENVIRONMENT_ENTRIES = 128
_MAX_ENVIRONMENT_NAME_BYTES = 256
_MAX_ENVIRONMENT_VALUE_BYTES = 32 * 1024
_MAX_ENVIRONMENT_BYTES = 256 * 1024
_ENVIRONMENT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_RUNTIME_ENVIRONMENTS = frozenset({"development", "test", "production"})
_RESERVED_ENVIRONMENT_NAMES = frozenset(
    {
        "BASH_ENV",
        "ENV",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_SYSTEM",
        "GIT_DIR",
        "GIT_WORK_TREE",
        "HOME",
        "LANG",
        "LC_ALL",
        "PATH",
        "PYTHONHOME",
        "PYTHONPATH",
    }
)
_RESERVED_ENVIRONMENT_PREFIXES = ("DYLD_", "LD_")


def _positive_integer(name: str, value: int, *, maximum: int | None = None) -> int:
    if type(value) is not int or value <= 0 or (maximum is not None and value > maximum):
        suffix = f" and at most {maximum}" if maximum is not None else ""
        raise ValueError(f"{name} must be a positive integer{suffix}")
    return value


def _validate_executable_path(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or "\x00" in value
        or len(value.encode("utf-8")) > _MAX_EXECUTABLE_PATH_BYTES
    ):
        raise ValueError("executable_path must be non-empty bounded text")
    entries = value.split(os.pathsep)
    if any(not entry for entry in entries):
        raise ValueError("executable_path may not contain empty entries")
    for entry in entries:
        candidate = Path(entry)
        if not candidate.is_absolute() or not candidate.is_dir():
            raise ValueError("every executable_path entry must be an existing absolute directory")
    return os.pathsep.join(os.fspath(Path(entry).resolve()) for entry in entries)


def _validate_trusted_environment(
    supplied: Mapping[str, str] | None,
) -> dict[str, str]:
    values = dict(supplied or {})
    if len(values) > _MAX_ENVIRONMENT_ENTRIES:
        raise ValueError("trusted environment contains too many entries")
    total_bytes = 0
    for name, value in values.items():
        if (
            not isinstance(name, str)
            or _ENVIRONMENT_NAME.fullmatch(name) is None
            or len(name.encode("utf-8")) > _MAX_ENVIRONMENT_NAME_BYTES
        ):
            raise ValueError("trusted environment contains an invalid variable name")
        if name in _RESERVED_ENVIRONMENT_NAMES or name.startswith(_RESERVED_ENVIRONMENT_PREFIXES):
            raise ValueError("trusted environment may not override sandbox-owned variables")
        if (
            not isinstance(value, str)
            or "\x00" in value
            or len(value.encode("utf-8")) > _MAX_ENVIRONMENT_VALUE_BYTES
        ):
            raise ValueError("trusted environment contains an invalid variable value")
        total_bytes += len(name.encode("utf-8")) + len(value.encode("utf-8"))
    if total_bytes > _MAX_ENVIRONMENT_BYTES:
        raise ValueError("trusted environment exceeds its total byte limit")
    return values


class LocalSandbox:
    """Run host processes only when explicitly enabled for development or tests."""

    def __init__(
        self,
        workspace: GitWorktreeWorkspace,
        *,
        unsafe_allow_host_execution: bool = False,
        runtime_environment: Literal["development", "test", "production"] = "production",
        executable_path: str = os.defpath,
        trusted_environment: Mapping[str, str] | None = None,
        runner: BoundedProcessRunner | None = None,
        max_concurrent_commands: int = 1,
        max_write_bytes: int = _DEFAULT_MAX_WRITE_BYTES,
        max_snapshots: int = _DEFAULT_MAX_SNAPSHOTS,
    ) -> None:
        if runtime_environment not in _RUNTIME_ENVIRONMENTS:
            raise ValueError("runtime_environment is not recognized")
        if unsafe_allow_host_execution and runtime_environment == "production":
            raise ValueError("LocalSandbox host execution is prohibited in production")
        validated_path = _validate_executable_path(executable_path)
        supplied_environment = _validate_trusted_environment(trusted_environment)
        self._max_write_bytes = _positive_integer("max_write_bytes", max_write_bytes)
        self._max_snapshots = _positive_integer("max_snapshots", max_snapshots)
        concurrency = _positive_integer(
            "max_concurrent_commands",
            max_concurrent_commands,
            maximum=_MAX_CONCURRENT_COMMANDS,
        )

        self._workspace = workspace
        self._enabled = unsafe_allow_host_execution
        self._runner = runner or BoundedProcessRunner()
        self._owns_runner = runner is None
        self._home = Path(tempfile.mkdtemp(prefix="agent-local-home-"))
        try:
            self._home.chmod(0o700)
        except OSError:
            shutil.rmtree(self._home, ignore_errors=True)
            raise
        self._environment = {
            **supplied_environment,
            "HOME": str(self._home),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": validated_path,
        }
        self._command_slots = asyncio.Semaphore(concurrency)
        self._active_tasks: set[asyncio.Task[ProcessResult]] = set()
        self._snapshots: dict[str, WorkspaceSnapshot] = {}
        self._snapshot_order: list[str] = []
        self._generation = 0
        self._cancelling = False
        self._destroying = False
        self._cleanup_required = False
        self._destroyed = False
        self._workspace_operation = False
        self._state_lock = asyncio.Lock()
        self._cancel_lock = asyncio.Lock()
        self._destroy_lock = asyncio.Lock()
        self._workspace_lock = asyncio.Lock()

    @property
    def workspace(self) -> GitWorktreeWorkspace:
        return self._workspace

    async def execute(self, command: CommandSpec) -> AsyncIterator[CommandEvent]:
        if not isinstance(command, CommandSpec):
            raise DomainOperationError(
                code="command_invalid",
                message="the command must use the validated command contract",
            )
        async with self._state_lock:
            self._require_available()
            if not self._enabled:
                raise DomainOperationError(
                    code="local_execution_disabled",
                    message="host command execution requires explicit development opt-in",
                )
            generation = self._generation

        await self._command_slots.acquire()
        runner_task: asyncio.Task[ProcessResult] | None = None
        try:
            async with self._state_lock:
                self._require_available()
                if generation != self._generation or self._cancelling or self._workspace_operation:
                    raise DomainOperationError(
                        code="command_cancelled",
                        message="the command was invalidated before it could start",
                        details={"retryable": True},
                    )
                cwd = self._workspace.resolve_path(command.cwd)
                if not cwd.is_dir():
                    raise DomainOperationError(
                        code="invalid_command_cwd",
                        message="the command working directory is not a directory",
                        details={"cwd": command.cwd},
                    )
                queue: asyncio.Queue[ProcessChunk] = asyncio.Queue(maxsize=_OUTPUT_QUEUE_CHUNKS)

                async def receive(chunk: ProcessChunk) -> None:
                    await queue.put(chunk)

                runner_task = asyncio.create_task(
                    self._runner.run(
                        command.argv,
                        cwd=cwd,
                        timeout_seconds=command.timeout_seconds,
                        max_output_bytes=command.max_output_bytes,
                        environment=self._environment,
                        on_chunk=receive,
                        retain_output=False,
                    )
                )
                self._active_tasks.add(runner_task)

            sequence = 0
            while not runner_task.done() or not queue.empty():
                if queue.empty() and not runner_task.done():
                    get_task = asyncio.create_task(queue.get())
                    done, _ = await asyncio.wait(
                        {get_task, runner_task},
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
            result = await runner_task
            yield CommandCompleted(
                exit_code=result.exit_code,
                timed_out=result.timed_out,
                output_truncated=result.output_truncated,
            )
        finally:
            if runner_task is not None:
                if not runner_task.done():
                    runner_task.cancel()
                await asyncio.gather(runner_task, return_exceptions=True)
                async with self._state_lock:
                    self._active_tasks.discard(runner_task)
            self._command_slots.release()

    async def read_file(self, path: str) -> bytes:
        async with self._exclusive_workspace_operation():
            return await self._thread_cancellation_safe(
                self._workspace.file_bytes,
                path,
            )

    async def write_file(self, path: str, content: bytes) -> None:
        if not isinstance(content, bytes):
            raise DomainOperationError(
                code="sandbox_write_invalid",
                message="sandbox writes require bytes",
            )
        if len(content) > self._max_write_bytes:
            raise DomainOperationError(
                code="sandbox_write_limit",
                message="sandbox write content exceeds the configured byte limit",
                details={"limit_bytes": self._max_write_bytes},
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
                label="local sandbox snapshot",
            )
            existing = self._snapshots.get(snapshot.id)
            if existing is not None and existing != snapshot:
                raise DomainOperationError(
                    code="snapshot_identity_conflict",
                    message="the snapshot identifier conflicts with retained state",
                )
            if existing is None:
                if len(self._snapshots) >= self._max_snapshots:
                    raise DomainOperationError(
                        code="snapshot_limit",
                        message="the local snapshot retention limit has been reached",
                    )
                self._snapshots[snapshot.id] = snapshot
                self._snapshot_order.append(snapshot.id)
            return snapshot

    async def restore_snapshot(self, snapshot: WorkspaceSnapshot) -> None:
        if not isinstance(snapshot, WorkspaceSnapshot):
            raise DomainOperationError(
                code="snapshot_invalid",
                message="the snapshot must use the validated snapshot contract",
            )
        async with self._workspace_lock:
            async with self._state_lock:
                self._require_available()
                retained = self._snapshots.get(snapshot.id)
                if retained is None:
                    raise DomainOperationError(
                        code="snapshot_not_found",
                        message="the snapshot is not owned by this sandbox",
                    )
                if retained != snapshot:
                    raise DomainOperationError(
                        code="snapshot_identity_mismatch",
                        message="the snapshot does not match its retained immutable state",
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

    async def _cancel_active(self) -> None:
        async with self._cancel_lock:
            async with self._state_lock:
                self._generation += 1
                self._cancelling = True
                active = tuple(self._active_tasks)
                for task in active:
                    task.cancel()
            failures: list[BaseException] = []
            try:
                results = await asyncio.gather(*active, return_exceptions=True)
                failures.extend(
                    result
                    for result in results
                    if isinstance(result, BaseException)
                    and not isinstance(result, asyncio.CancelledError)
                )
                try:
                    await self._runner.cancel_all()
                except Exception as error:
                    failures.append(error)
            finally:
                async with self._state_lock:
                    self._cancelling = False
                    if failures:
                        self._cleanup_required = True
            if failures:
                raise DomainOperationError(
                    code="sandbox_cancel_failed",
                    message="one or more local commands could not be cancelled",
                    details={"retryable": True},
                ) from failures[0]

    async def destroy(self) -> None:
        task = asyncio.create_task(self._destroy())
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

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
                if self._home.exists():
                    await asyncio.to_thread(shutil.rmtree, self._home)
            except Exception as error:
                async with self._state_lock:
                    self._destroying = False
                    self._cleanup_required = True
                raise DomainOperationError(
                    code="sandbox_cleanup_failed",
                    message="local sandbox cleanup failed and may be retried",
                    details={"retryable": True},
                ) from error
            async with self._state_lock:
                self._destroying = False
                self._cleanup_required = False
                self._destroyed = True
                self._snapshots.clear()
                self._snapshot_order.clear()

    @asynccontextmanager
    async def _exclusive_workspace_operation(self) -> AsyncIterator[None]:
        async with self._workspace_lock:
            async with self._state_lock:
                self._require_available()
                if self._active_tasks or self._cancelling:
                    raise DomainOperationError(
                        code="sandbox_busy",
                        message="workspace access requires all commands to be stopped",
                        details={"retryable": True},
                    )
                self._workspace_operation = True
            try:
                yield
            finally:
                async with self._state_lock:
                    self._workspace_operation = False

    def _require_available(self) -> None:
        if self._destroyed:
            raise DomainOperationError(
                code="sandbox_destroyed",
                message="the local execution adapter has been destroyed",
            )
        if self._destroying or self._cleanup_required:
            raise DomainOperationError(
                code="sandbox_cleanup_required",
                message="the local execution adapter requires cleanup before reuse",
                details={"retryable": True},
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


__all__ = ["LocalSandbox"]
