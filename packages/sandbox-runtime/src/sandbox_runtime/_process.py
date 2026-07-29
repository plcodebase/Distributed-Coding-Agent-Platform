"""Incrementally bounded POSIX subprocess execution shared by local adapters."""

from __future__ import annotations

import asyncio
import codecs
import os
import signal
from dataclasses import dataclass
from typing import TYPE_CHECKING

from agent_core.domain.errors import DomainOperationError
from agent_core.tools import ToolOutputChannel

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence
    from pathlib import Path

_STREAM_COUNT = 2
_STREAM_QUEUE_CHUNKS = 16


@dataclass(frozen=True, slots=True)
class ProcessChunk:
    channel: ToolOutputChannel
    text: str


@dataclass(frozen=True, slots=True)
class ProcessResult:
    chunks: tuple[ProcessChunk, ...]
    exit_code: int
    timed_out: bool = False
    output_truncated: bool = False


class BoundedProcessRunner:
    """Run argv-only subprocesses with bounded output and process-group cleanup."""

    def __init__(self, *, terminate_grace_seconds: float = 1.0) -> None:
        self._terminate_grace_seconds = terminate_grace_seconds
        self._active: set[asyncio.subprocess.Process] = set()
        self._closed = False

    async def run(  # noqa: PLR0912, PLR0915 - bounded stream/process lifecycle
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
        max_output_bytes: int,
        environment: Mapping[str, str] | None = None,
        on_chunk: Callable[[ProcessChunk], Awaitable[None]] | None = None,
        retain_output: bool = True,
    ) -> ProcessResult:
        if self._closed:
            raise DomainOperationError(
                code="command_runner_closed",
                message="the command runner is closed",
            )
        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                cwd=cwd,
                env=environment,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
        except (OSError, ValueError) as error:
            raise DomainOperationError(
                code="command_start_failed",
                message="the command could not be started",
            ) from error

        self._active.add(process)
        queue: asyncio.Queue[tuple[ToolOutputChannel, bytes | None]] = asyncio.Queue(
            maxsize=_STREAM_QUEUE_CHUNKS
        )
        stdout_task = asyncio.create_task(
            self._read_stream(process.stdout, ToolOutputChannel.STDOUT, queue)
        )
        stderr_task = asyncio.create_task(
            self._read_stream(process.stderr, ToolOutputChannel.STDERR, queue)
        )
        tasks = (stdout_task, stderr_task)
        chunks: list[ProcessChunk] = []
        decoders = {
            ToolOutputChannel.STDOUT: codecs.getincrementaldecoder("utf-8")(errors="replace"),
            ToolOutputChannel.STDERR: codecs.getincrementaldecoder("utf-8")(errors="replace"),
        }
        remaining = max_output_bytes
        completed_streams = 0
        timed_out = False
        truncated = False
        try:
            try:
                async with asyncio.timeout(timeout_seconds):
                    while completed_streams < _STREAM_COUNT:
                        channel, data = await queue.get()
                        if data is None:
                            completed_streams += 1
                            tail = decoders[channel].decode(b"", final=True)
                            if tail:
                                chunk = ProcessChunk(channel=channel, text=tail)
                                if retain_output:
                                    chunks.append(chunk)
                                if on_chunk is not None:
                                    await on_chunk(chunk)
                            continue
                        if len(data) > remaining:
                            data = data[:remaining]
                            truncated = True
                        remaining -= len(data)
                        text = decoders[channel].decode(data, final=False)
                        if text:
                            chunk = ProcessChunk(channel=channel, text=text)
                            if retain_output:
                                chunks.append(chunk)
                            if on_chunk is not None:
                                await on_chunk(chunk)
                        if truncated:
                            await self._terminate(process)
                            break
                    if not truncated:
                        await process.wait()
            except TimeoutError:
                timed_out = True
                await self._terminate(process)
            except asyncio.CancelledError:
                await self._terminate(process)
                raise
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._active.discard(process)

        return ProcessResult(
            chunks=tuple(chunks),
            exit_code=process.returncode if process.returncode is not None else -signal.SIGKILL,
            timed_out=timed_out,
            output_truncated=truncated,
        )

    async def cancel_all(self) -> None:
        await asyncio.gather(
            *(self._terminate(process) for process in tuple(self._active)),
            return_exceptions=True,
        )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self.cancel_all()

    @staticmethod
    async def _read_stream(
        stream: asyncio.StreamReader | None,
        channel: ToolOutputChannel,
        queue: asyncio.Queue[tuple[ToolOutputChannel, bytes | None]],
    ) -> None:
        if stream is None:
            await queue.put((channel, None))
            return
        while data := await stream.read(4096):
            await queue.put((channel, data))
        await queue.put((channel, None))

    async def _terminate(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(process.wait(), timeout=self._terminate_grace_seconds)
        except TimeoutError:
            pass
        else:
            return
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        await process.wait()


__all__ = ["BoundedProcessRunner", "ProcessChunk", "ProcessResult"]
