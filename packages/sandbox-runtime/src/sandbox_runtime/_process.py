"""Incrementally bounded POSIX subprocess execution shared by local adapters."""

from __future__ import annotations

import asyncio
import codecs
import math
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
_MAX_TERMINATE_GRACE_SECONDS = 60.0
_MAX_ARGV_BYTES = 256 * 1024
_MAX_TIMEOUT_SECONDS = 3600.0
_MAX_OUTPUT_BYTES = 100 * 1024 * 1024
_MAX_ENVIRONMENT_ENTRIES = 1024
_MAX_ENVIRONMENT_BYTES = 1024 * 1024


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


def _fit_utf8(value: str, max_bytes: int) -> tuple[str, bool]:
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value, False
    if max_bytes <= 0:
        return "", bool(value)
    return encoded[:max_bytes].decode("utf-8", errors="ignore"), True


class BoundedProcessRunner:
    """Run argv-only subprocesses with bounded output and process-group cleanup."""

    def __init__(self, *, terminate_grace_seconds: float = 1.0) -> None:
        if (
            isinstance(terminate_grace_seconds, bool)
            or not isinstance(terminate_grace_seconds, (int, float))
            or not math.isfinite(terminate_grace_seconds)
            or terminate_grace_seconds <= 0
            or terminate_grace_seconds > _MAX_TERMINATE_GRACE_SECONDS
        ):
            raise ValueError("terminate_grace_seconds must be finite, positive, and at most 60")
        self._terminate_grace_seconds = terminate_grace_seconds
        self._active: set[asyncio.subprocess.Process] = set()
        self._closed = False
        self._cancelling = False
        self._lifecycle_lock = asyncio.Lock()
        self._cancel_lock = asyncio.Lock()

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
        self._validate_request(
            argv,
            timeout_seconds=timeout_seconds,
            max_output_bytes=max_output_bytes,
            environment=environment,
        )
        async with self._lifecycle_lock:
            if self._closed:
                raise DomainOperationError(
                    code="command_runner_closed",
                    message="the command runner is closed",
                )
            if self._cancelling:
                raise DomainOperationError(
                    code="command_runner_cancelling",
                    message="the command runner is cancelling active processes",
                    details={"retryable": True},
                )
            start_task = asyncio.create_task(
                asyncio.create_subprocess_exec(
                    *argv,
                    cwd=cwd,
                    env=environment,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    start_new_session=True,
                )
            )
            try:
                process = await asyncio.shield(start_task)
            except asyncio.CancelledError as cancelled:
                try:
                    process = await start_task
                except (OSError, ValueError) as start_error:
                    raise cancelled from start_error
                await self._terminate(process)
                raise
            except (OSError, ValueError) as error:
                raise DomainOperationError(
                    code="command_start_failed",
                    message="the command could not be started",
                ) from error
            self._active.add(process)

        if process.stdout is None or process.stderr is None:
            await self._terminate(process)
            async with self._lifecycle_lock:
                self._active.discard(process)
            raise DomainOperationError(
                code="command_protocol_error",
                message="the command did not expose bounded output streams",
            )
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
        output_remaining = max_output_bytes
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
                                tail, tail_truncated = _fit_utf8(tail, output_remaining)
                                truncated = truncated or tail_truncated
                                output_remaining -= len(tail.encode("utf-8"))
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
                        text, text_truncated = _fit_utf8(text, output_remaining)
                        if text_truncated:
                            truncated = True
                            remaining = 0
                        output_remaining -= len(text.encode("utf-8"))
                        if text:
                            chunk = ProcessChunk(channel=channel, text=text)
                            if retain_output:
                                chunks.append(chunk)
                            if on_chunk is not None:
                                await on_chunk(chunk)
                        if truncated and process.returncode is None:
                            await self._terminate(process)
                        # Keep draining both bounded OS pipes after termination.
                        # Cancelling readers here can leave asyncio subprocess
                        # transports alive until after the event loop closes.
                    if not truncated:
                        await process.wait()
            except TimeoutError:
                timed_out = True
                await self._terminate(process)
            except asyncio.CancelledError:
                await self._terminate(process)
                raise
            except Exception:
                await self._terminate(process)
                raise
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            # asyncio.subprocess.Process exposes no public transport-close
            # method in Python 3.12. Closing the CPython transport explicitly
            # is necessary after output-overflow termination; otherwise its
            # pipe transports can survive until the event loop is gone.
            transport = getattr(process, "_transport", None)
            if transport is not None:
                transport.close()
            # CPython delivers subprocess pipe ``connection_lost`` callbacks
            # separately from reader EOF and process.wait(). Give those
            # callbacks a loop turn before the last Process reference leaves
            # this scope, otherwise an abruptly OOM-killed child can retain a
            # transport until after the pytest event loop has closed.
            await asyncio.sleep(0)
            async with self._lifecycle_lock:
                self._active.discard(process)

        return ProcessResult(
            chunks=tuple(chunks),
            exit_code=process.returncode if process.returncode is not None else -signal.SIGKILL,
            timed_out=timed_out,
            output_truncated=truncated,
        )

    async def cancel_all(self) -> None:
        await self._stop_active(close=False)

    async def close(self) -> None:
        await self._stop_active(close=True)

    async def _stop_active(self, *, close: bool) -> None:
        async with self._cancel_lock:
            async with self._lifecycle_lock:
                if close:
                    self._closed = True
                elif self._closed:
                    return
                self._cancelling = True
                active = tuple(self._active)
            stop_future = asyncio.gather(
                *(self._terminate(process) for process in active),
                return_exceptions=True,
            )
            cancelled: asyncio.CancelledError | None = None
            try:
                results = await asyncio.shield(stop_future)
            except asyncio.CancelledError as error:
                cancelled = error
                results = await stop_future
            failure = next(
                (result for result in results if isinstance(result, BaseException)),
                None,
            )
            if failure is not None:
                raise DomainOperationError(
                    code="command_cancel_failed",
                    message="one or more command processes could not be terminated",
                    details={"retryable": True},
                ) from failure
            async with self._lifecycle_lock:
                self._cancelling = False
            if cancelled is not None:
                raise cancelled

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
        except PermissionError:
            try:
                process.terminate()
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
        except PermissionError:
            try:
                process.kill()
            except ProcessLookupError:
                return
        await process.wait()

    @staticmethod
    def _validate_request(
        argv: Sequence[str],
        *,
        timeout_seconds: float,
        max_output_bytes: int,
        environment: Mapping[str, str] | None,
    ) -> None:
        if isinstance(argv, (str, bytes)) or not argv:
            raise DomainOperationError(
                code="command_invalid",
                message="the command must contain at least one argument",
            )
        if any(
            not isinstance(argument, str) or not argument or "\x00" in argument for argument in argv
        ):
            raise DomainOperationError(
                code="command_invalid",
                message="command arguments must be non-empty text without NUL bytes",
            )
        if sum(len(argument.encode("utf-8")) for argument in argv) > _MAX_ARGV_BYTES:
            raise DomainOperationError(
                code="command_invalid",
                message="the command arguments exceed the configured byte limit",
            )
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
            or timeout_seconds > _MAX_TIMEOUT_SECONDS
        ):
            raise DomainOperationError(
                code="command_invalid",
                message="the command timeout is outside the supported range",
            )
        if (
            type(max_output_bytes) is not int
            or max_output_bytes <= 0
            or max_output_bytes > _MAX_OUTPUT_BYTES
        ):
            raise DomainOperationError(
                code="command_invalid",
                message="the command output limit is outside the supported range",
            )
        if environment is not None:
            if len(environment) > _MAX_ENVIRONMENT_ENTRIES:
                raise DomainOperationError(
                    code="command_invalid",
                    message="the command environment contains too many entries",
                )
            environment_bytes = 0
            for name, value in environment.items():
                if (
                    not isinstance(name, str)
                    or not name
                    or "=" in name
                    or "\x00" in name
                    or not isinstance(value, str)
                    or "\x00" in value
                ):
                    raise DomainOperationError(
                        code="command_invalid",
                        message="the command environment is invalid",
                    )
                environment_bytes += len(name.encode("utf-8")) + len(value.encode("utf-8"))
                if environment_bytes > _MAX_ENVIRONMENT_BYTES:
                    raise DomainOperationError(
                        code="command_invalid",
                        message="the command environment exceeds its byte limit",
                    )


__all__ = ["BoundedProcessRunner", "ProcessChunk", "ProcessResult"]
