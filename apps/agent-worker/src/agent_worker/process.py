"""Spawn and gracefully stop a local fleet of independent worker processes."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import multiprocessing
import re
import signal
from typing import TYPE_CHECKING, cast

from agent_worker.service import WorkerService

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence
    from typing import Protocol

    class JoinableProcess(Protocol):
        @property
        def exitcode(self) -> int | None: ...

        def join(self, timeout: float | None = None) -> None: ...


type WorkerServiceFactory = Callable[[int], WorkerService | Awaitable[WorkerService]]

_FACTORY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_LOCAL_WORKER_PROCESSES = 3
MAX_LOCAL_WORKER_PROCESSES = 64


def load_worker_factory(specification: str) -> WorkerServiceFactory:
    """Load one trusted application composition root by ``module:attribute``."""

    if not _FACTORY_PATTERN.fullmatch(specification):
        raise ValueError("worker factory must use a bounded module:attribute reference")
    module_name, _, attribute = specification.partition(":")
    factory = getattr(importlib.import_module(module_name), attribute, None)
    if not callable(factory):
        raise TypeError("worker factory reference must be callable")
    return cast("WorkerServiceFactory", factory)


def run_worker_fleet(
    factory_specification: str,
    *,
    process_count: int = DEFAULT_LOCAL_WORKER_PROCESSES,
) -> None:
    """Run a local multi-process worker fleet until every child exits."""

    if type(process_count) is not int or not 1 <= process_count <= MAX_LOCAL_WORKER_PROCESSES:
        raise ValueError(f"process_count must be in [1, {MAX_LOCAL_WORKER_PROCESSES}]")
    load_worker_factory(factory_specification)
    context = multiprocessing.get_context("spawn")
    processes = [
        context.Process(
            target=_worker_process_main,
            args=(factory_specification, index),
            name=f"agent-worker-{index + 1}",
        )
        for index in range(process_count)
    ]
    try:
        for process in processes:
            process.start()
        _wait_for_processes(processes)
    except BaseException:
        for process in processes:
            if process.is_alive():
                process.terminate()
        for process in processes:
            if process.pid is not None:
                process.join(timeout=10)
        raise


def _wait_for_processes(
    processes: Sequence[JoinableProcess],
) -> None:
    pending = list(processes)
    while pending:
        still_running: list[JoinableProcess] = []
        for process in pending:
            process.join(timeout=0.05)
            if process.exitcode is None:
                still_running.append(process)
            elif process.exitcode != 0:
                raise RuntimeError("one or more worker processes exited unsuccessfully")
        pending = still_running


def _worker_process_main(factory_specification: str, index: int) -> None:
    asyncio.run(_serve_worker(load_worker_factory(factory_specification), index))


async def _serve_worker(factory: WorkerServiceFactory, index: int) -> None:
    service = factory(index)
    if inspect.isawaitable(service):
        service = await service
    if not isinstance(service, WorkerService):
        raise TypeError("worker factory must return WorkerService")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signal_number in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signal_number, stop.set)
    await service.serve(stop)


def main(arguments: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--factory", required=True)
    parser.add_argument(
        "--processes",
        type=int,
        default=DEFAULT_LOCAL_WORKER_PROCESSES,
    )
    parsed = parser.parse_args(arguments)
    run_worker_fleet(parsed.factory, process_count=parsed.processes)


__all__ = [
    "DEFAULT_LOCAL_WORKER_PROCESSES",
    "MAX_LOCAL_WORKER_PROCESSES",
    "WorkerServiceFactory",
    "load_worker_factory",
    "main",
    "run_worker_fleet",
]
