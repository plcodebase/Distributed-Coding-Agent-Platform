"""Targeted recovery scheduler for expired run leases."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Protocol

from pydantic import Field

from agent_core.domain.base import DomainModel

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agent_core.capacity import QueueMonitor
    from agent_core.distributed import RunQueue
    from agent_core.loop import Clock
    from agent_scheduler.memory import MemoryExtractionProcessor
    from platform_telemetry import PlatformTelemetry

type Sleep = Callable[[float], Awaitable[None]]
type CloseCallback = Callable[[], Awaitable[None]]


class BackgroundProcessor(Protocol):
    """One bounded, durable background-job processor."""

    async def run_once(self) -> bool: ...


class SchedulerConfig(DomainModel):
    """Bounded scheduler polling and recovery batch limits."""

    poll_seconds: float = Field(default=1, ge=0.05, le=60)
    recovery_batch_size: int = Field(default=100, ge=1, le=1000)


class SchedulerService:
    """Requeue expired attempts without touching unrelated queue rows."""

    def __init__(
        self,
        *,
        queue: RunQueue,
        clock: Clock,
        config: SchedulerConfig | None = None,
        sleep: Sleep = asyncio.sleep,
        memory_processor: MemoryExtractionProcessor | None = None,
        background_processors: tuple[BackgroundProcessor, ...] = (),
        queue_monitor: QueueMonitor | None = None,
        telemetry: PlatformTelemetry | None = None,
        close: CloseCallback | None = None,
    ) -> None:
        self._queue = queue
        self._clock = clock
        self._config = config or SchedulerConfig()
        self._sleep = sleep
        self._memory_processor = memory_processor
        self._background_processors = background_processors
        self._queue_monitor = queue_monitor
        self._telemetry = telemetry
        self._started = False
        self._close = close
        self._closed = False

    @property
    def ready(self) -> bool:
        return self._started

    def render_metrics(self) -> bytes:
        return self._telemetry.metrics.render() if self._telemetry is not None else b""

    async def recover_once(self) -> int:
        recovered = await self._queue.recover_expired(
            occurred_at=self._clock.now(),
            limit=self._config.recovery_batch_size,
        )
        if recovered and self._telemetry is not None:
            self._telemetry.metrics.run_recoveries.inc(len(recovered))
        if self._queue_monitor is not None and self._telemetry is not None:
            snapshot = await self._queue_monitor.snapshot(occurred_at=self._clock.now())
            self._telemetry.metrics.observe_queue(
                depth={
                    "interactive": snapshot.depth.interactive,
                    "background": snapshot.depth.background,
                    "evaluation": snapshot.depth.evaluation,
                },
                oldest_seconds=snapshot.oldest_age_seconds,
            )
        return len(recovered)

    async def serve(self, stop: asyncio.Event) -> None:
        self._started = True
        try:
            while not stop.is_set():
                recovered = await self.recover_once()
                memory_processed = (
                    await self._memory_processor.process_once()
                    if self._memory_processor is not None
                    else False
                )
                background_processed = False
                for processor in self._background_processors:
                    background_processed = await processor.run_once() or background_processed
                if recovered == 0 and not memory_processed and not background_processed:
                    await self._sleep(self._config.poll_seconds)
        finally:
            self._started = False

    async def aclose(self) -> None:
        """Close composition-owned dependencies exactly once."""

        if self._closed:
            return
        if self._close is not None:
            task: asyncio.Future[None] = asyncio.ensure_future(self._close())
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                await task
                raise
        self._closed = True


__all__ = ["BackgroundProcessor", "CloseCallback", "SchedulerConfig", "SchedulerService"]
