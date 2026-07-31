"""Run the lease-recovery scheduler from a trusted application factory."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import re
import signal
from typing import TYPE_CHECKING, cast

from agent_scheduler.service import SchedulerService

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

type SchedulerServiceFactory = Callable[[], SchedulerService | Awaitable[SchedulerService]]

_FACTORY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*$")


def load_scheduler_factory(specification: str) -> SchedulerServiceFactory:
    if not _FACTORY_PATTERN.fullmatch(specification):
        raise ValueError("scheduler factory must use a bounded module:attribute reference")
    module_name, _, attribute = specification.partition(":")
    factory = getattr(importlib.import_module(module_name), attribute, None)
    if not callable(factory):
        raise TypeError("scheduler factory reference must be callable")
    return cast("SchedulerServiceFactory", factory)


async def serve_scheduler(factory: SchedulerServiceFactory) -> None:
    service = factory()
    if inspect.isawaitable(service):
        service = await service
    if not isinstance(service, SchedulerService):
        raise TypeError("scheduler factory must return SchedulerService")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signal_number in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signal_number, stop.set)
    await service.serve(stop)


def main(arguments: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--factory", required=True)
    parsed = parser.parse_args(arguments)
    asyncio.run(serve_scheduler(load_scheduler_factory(parsed.factory)))


__all__ = [
    "SchedulerServiceFactory",
    "load_scheduler_factory",
    "main",
    "serve_scheduler",
]
