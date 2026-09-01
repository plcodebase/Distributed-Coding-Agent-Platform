"""Asynchronous long-term memory extraction from completed-run jobs."""

from __future__ import annotations

import asyncio
import math
import uuid
from datetime import timedelta
from typing import TYPE_CHECKING, Protocol

from agent_core.control import (
    MemoryExtractionJob,
    MemoryExtractionStatus,
    PersistedMemory,
    memory_content_hash,
)
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.memory import MAX_MEMORY_EXTRACTION_SOURCE_BYTES, MemoryExtractionInput

MAX_MEMORY_WORKER_ID_LENGTH = 255
MAX_MEMORY_LEASE_SECONDS = 3600.0

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import datetime

    from agent_core.loop import Clock
    from agent_core.memory import MemoryExtractor


class MemoryExtractionStore(Protocol):
    """Durable job/source/result boundary used by the scheduler."""

    async def claim_pending(
        self,
        *,
        worker_id: str,
        occurred_at: datetime,
        lease_duration: timedelta,
    ) -> MemoryExtractionJob | None: ...

    async def source_for_job(
        self,
        job: MemoryExtractionJob,
        *,
        max_bytes: int,
        occurred_at: datetime,
    ) -> str: ...

    async def complete(
        self,
        job: MemoryExtractionJob,
        memories: Sequence[PersistedMemory],
        *,
        completed_at: datetime,
    ) -> MemoryExtractionJob: ...

    async def fail(
        self,
        job: MemoryExtractionJob,
        *,
        error: ErrorDetail,
        completed_at: datetime,
    ) -> MemoryExtractionJob: ...


class MemoryExtractionProcessor:
    """Claim, extract, validate, and persist one bounded job at a time."""

    def __init__(
        self,
        *,
        store: MemoryExtractionStore,
        extractor: MemoryExtractor,
        clock: Clock,
        id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
        worker_id: str = "memory-scheduler",
        lease_duration_seconds: float = 300.0,
        extraction_timeout_seconds: float = 240.0,
    ) -> None:
        if not callable(id_factory):
            raise TypeError("memory ID factory must be callable")
        if (
            type(worker_id) is not str
            or not worker_id.strip()
            or len(worker_id) > MAX_MEMORY_WORKER_ID_LENGTH
        ):
            raise ValueError("memory extraction worker ID is invalid")
        if (
            isinstance(lease_duration_seconds, bool)
            or not isinstance(lease_duration_seconds, (int, float))
            or not math.isfinite(lease_duration_seconds)
            or not 0 < lease_duration_seconds <= MAX_MEMORY_LEASE_SECONDS
        ):
            raise ValueError("memory extraction lease duration must be in (0, 3600] seconds")
        if (
            isinstance(extraction_timeout_seconds, bool)
            or not isinstance(extraction_timeout_seconds, (int, float))
            or not math.isfinite(extraction_timeout_seconds)
            or not 0 < extraction_timeout_seconds < lease_duration_seconds
        ):
            raise ValueError(
                "memory extraction timeout must be positive and shorter than its lease"
            )
        self._store = store
        self._extractor = extractor
        self._clock = clock
        self._id_factory = id_factory
        self._worker_id = worker_id
        self._lease_duration = timedelta(seconds=lease_duration_seconds)
        self._extraction_timeout_seconds = float(extraction_timeout_seconds)

    async def process_once(self) -> bool:
        job = await self._store.claim_pending(
            worker_id=self._worker_id,
            occurred_at=self._clock.now(),
            lease_duration=self._lease_duration,
        )
        if job is None:
            return False
        if job.status in {MemoryExtractionStatus.COMPLETED, MemoryExtractionStatus.FAILED}:
            return True
        try:
            async with asyncio.timeout(self._extraction_timeout_seconds):
                source = await self._store.source_for_job(
                    job,
                    max_bytes=MAX_MEMORY_EXTRACTION_SOURCE_BYTES,
                    occurred_at=self._clock.now(),
                )
                if not source.strip():
                    await self._store.complete(job, (), completed_at=self._clock.now())
                    return True
                result = await self._extractor.extract(
                    MemoryExtractionInput(
                        tenant_id=job.tenant_id,
                        session_id=job.session_id,
                        run_id=job.run_id,
                        execution_epoch=job.execution_epoch,
                        transcript=source,
                    )
                )
                extracted_at = self._clock.now()
                memories = tuple(
                    PersistedMemory(
                        id=self._id_factory(),
                        tenant_id=job.tenant_id,
                        session_id=job.session_id,
                        source_run_id=job.run_id,
                        execution_epoch=job.execution_epoch,
                        kind=item.kind,
                        content=item.content,
                        content_hash=memory_content_hash(item.content),
                        extracted_at=extracted_at,
                    )
                    for item in result.memories
                )
                await self._store.complete(job, memories, completed_at=self._clock.now())
        except TimeoutError:
            await self._store.fail(
                job,
                error=ErrorDetail(
                    code="memory_extraction_timeout",
                    message="memory extraction exceeded its bounded deadline",
                    retryable=False,
                ),
                completed_at=self._clock.now(),
            )
        except DomainOperationError as error:
            if error.code == "memory_extraction_lease_lost":
                return True
            await self._store.fail(
                job,
                error=error.error,
                completed_at=self._clock.now(),
            )
        except Exception:
            await self._store.fail(
                job,
                error=ErrorDetail(
                    code="memory_extraction_failed",
                    message="memory extraction could not be completed",
                    retryable=False,
                ),
                completed_at=self._clock.now(),
            )
        return True


__all__ = ["MemoryExtractionProcessor", "MemoryExtractionStore"]
