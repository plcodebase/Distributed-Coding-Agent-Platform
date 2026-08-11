"""Targeted recovery scheduler for expired run leases."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

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
        queue_monitor: QueueMonitor | None = None,
        telemetry: PlatformTelemetry | None = None,
    ) -> None:
        self._queue = queue
        self._clock = clock
        self._config = config or SchedulerConfig()
        self._sleep = sleep
        self._memory_processor = memory_processor
        self._queue_monitor = queue_monitor
        self._telemetry = telemetry

    async def recover_once(self) -> int:
        recovered = await self._queue.recover_expired(
            occurred_at=self._clock.now(),
            limit=self._config.recovery_batch_size,
        )
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
        while not stop.is_set():
            recovered = await self.recover_once()
            memory_processed = (
                await self._memory_processor.process_once()
                if self._memory_processor is not None
                else False
            )
            if recovered == 0 and not memory_processed:
                await self._sleep(self._config.poll_seconds)


__all__ = ["SchedulerConfig", "SchedulerService"]
