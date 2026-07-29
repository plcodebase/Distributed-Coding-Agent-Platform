"""Explicitly unsafe development-only host process adapter."""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
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
from sandbox_runtime._process import BoundedProcessRunner, ProcessChunk

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping

    from sandbox_runtime.git_workspace import GitWorktreeWorkspace

_OUTPUT_QUEUE_CHUNKS = 16


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
    ) -> None:
        self._workspace = workspace
        self._enabled = unsafe_allow_host_execution
        if unsafe_allow_host_execution and runtime_environment == "production":
            raise ValueError("LocalSandbox host execution is prohibited in production")
        self._runner = runner or BoundedProcessRunner()
        self._owns_runner = runner is None
        self._home = Path(tempfile.mkdtemp(prefix="agent-local-home-"))
        supplied_environment = dict(trusted_environment or {})
        reserved_names = {"HOME", "LANG", "LC_ALL", "PATH"}
        if reserved_names.intersection(supplied_environment):
            shutil.rmtree(self._home, ignore_errors=True)
            raise ValueError("trusted environment may not override sandbox-owned variables")
        self._environment = {
            **supplied_environment,
            "HOME": str(self._home),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": executable_path,
        }
        self._destroyed = False

    @property
    def workspace(self) -> GitWorktreeWorkspace:
        return self._workspace

    async def execute(self, command: CommandSpec) -> AsyncIterator[CommandEvent]:
        if not self._enabled:
            raise DomainOperationError(
                code="local_execution_disabled",
                message="host command execution requires explicit development opt-in",
            )
        if self._destroyed:
            raise DomainOperationError(
                code="sandbox_destroyed",
                message="the local execution adapter has been destroyed",
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
        sequence = 0
        try:
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
        finally:
            if not runner_task.done():
                runner_task.cancel()
            await asyncio.gather(runner_task, return_exceptions=True)

        yield CommandCompleted(
            exit_code=result.exit_code,
            timed_out=result.timed_out,
            output_truncated=result.output_truncated,
        )

    async def read_file(self, path: str) -> bytes:
        return await asyncio.to_thread(self._workspace.file_bytes, path)

    async def write_file(self, path: str, content: bytes) -> None:
        await asyncio.to_thread(self._workspace.write_file_atomic, path, content)

    async def create_snapshot(self) -> WorkspaceSnapshot:
        return await asyncio.to_thread(
            self._workspace.create_snapshot,
            label="local sandbox snapshot",
        )

    async def restore_snapshot(self, snapshot: WorkspaceSnapshot) -> None:
        await self.cancel_active()
        await asyncio.to_thread(self._workspace.restore_revision, snapshot.revision)

    async def cancel_active(self) -> None:
        await self._runner.cancel_all()

    async def destroy(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True
        if self._owns_runner:
            await self._runner.close()
        else:
            await self._runner.cancel_all()
        await self._workspace.destroy()
        shutil.rmtree(self._home, ignore_errors=True)


__all__ = ["LocalSandbox"]
