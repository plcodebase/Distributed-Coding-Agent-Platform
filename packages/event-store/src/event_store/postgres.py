"""PostgreSQL-backed atomic event sequencing and replay."""

from __future__ import annotations

import asyncio
import math
from contextlib import suppress
from typing import TYPE_CHECKING

import anyio
from pydantic import TypeAdapter
from sqlalchemy import func, select, update

from agent_core.domain.errors import DomainOperationError
from agent_core.event_store import MAX_EVENT_PAGE_SIZE, EventDraft, EventPage, StoredEvent
from agent_core.events import EventType
from platform_persistence.models import AgentEventRecord, RunRecord

if TYPE_CHECKING:
    import uuid
    from collections.abc import AsyncIterator, Awaitable, Callable

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

_EVENT_TYPE_ADAPTER: TypeAdapter[EventType] = TypeAdapter(EventType)
MIN_POLL_INTERVAL_SECONDS = 0.01
MAX_POLL_INTERVAL_SECONDS = 10.0


class PostgresEventStore:
    """Append-only event store with run-row sequence allocation."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        poll_interval_seconds: float = 0.25,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if (
            isinstance(poll_interval_seconds, bool)
            or not isinstance(poll_interval_seconds, (int, float))
            or not math.isfinite(poll_interval_seconds)
            or not MIN_POLL_INTERVAL_SECONDS <= poll_interval_seconds <= MAX_POLL_INTERVAL_SECONDS
        ):
            raise ValueError("poll_interval_seconds must be in [0.01, 10]")
        self._sessions = sessions
        self._poll_interval = poll_interval_seconds
        self._sleep = sleep

    async def append(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        draft: EventDraft,
    ) -> StoredEvent:
        """Allocate and commit the next sequence in the event row transaction."""

        async with self._sessions() as database, database.begin():
            next_value = await database.scalar(
                update(RunRecord)
                .where(
                    RunRecord.tenant_id == tenant_id,
                    RunRecord.id == run_id,
                )
                .values(next_event_sequence=RunRecord.next_event_sequence + 1)
                .returning(RunRecord.next_event_sequence)
            )
            if next_value is None:
                raise DomainOperationError(
                    code="run_not_found",
                    message="the run does not exist for this tenant",
                    details={"run_id": str(run_id)},
                )
            sequence = int(next_value) - 1
            database.add(
                AgentEventRecord(
                    tenant_id=tenant_id,
                    run_id=run_id,
                    sequence=sequence,
                    event_type=draft.event_type.value,
                    payload=draft.payload.to_json_object(),
                    created_at=draft.created_at,
                )
            )
        return StoredEvent(
            run_id=run_id,
            sequence=sequence,
            event_type=draft.event_type,
            payload=draft.payload,
            created_at=draft.created_at,
        )

    async def read_page(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        *,
        after: int = 0,
        limit: int = 100,
    ) -> EventPage:
        """Read a deterministic bounded page after an exclusive sequence cursor."""

        if type(after) is not int or after < 0:
            raise ValueError("after may not be negative")
        if type(limit) is not int or not 1 <= limit <= MAX_EVENT_PAGE_SIZE:
            raise ValueError("limit must be in [1, 1000]")
        operation = asyncio.create_task(
            self._read_page(
                tenant_id,
                run_id,
                after=after,
                limit=limit,
            )
        )
        try:
            return await asyncio.shield(operation)
        except asyncio.CancelledError:
            with anyio.CancelScope(shield=True), suppress(Exception):
                await asyncio.shield(operation)
            raise

    async def _read_page(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        *,
        after: int,
        limit: int,
    ) -> EventPage:
        async with self._sessions() as database:
            rows = tuple(
                (
                    await database.scalars(
                        select(AgentEventRecord)
                        .where(
                            AgentEventRecord.tenant_id == tenant_id,
                            AgentEventRecord.run_id == run_id,
                            AgentEventRecord.sequence > after,
                        )
                        .order_by(AgentEventRecord.sequence)
                        .limit(limit + 1)
                    )
                ).all()
            )
        has_more = len(rows) > limit
        retained = rows[:limit]
        events = tuple(_stored_event(row) for row in retained)
        return EventPage(
            events=events,
            next_after=events[-1].sequence if events else after,
            has_more=has_more,
        )

    async def iter_after(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        *,
        after: int = 0,
        page_size: int = 100,
    ) -> AsyncIterator[StoredEvent]:
        """Replay every currently durable event without unbounded materialization."""

        cursor = after
        while True:
            page = await self.read_page(
                tenant_id,
                run_id,
                after=cursor,
                limit=page_size,
            )
            for event in page.events:
                cursor = event.sequence
                yield event
            if not page.has_more:
                return

    async def stream(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        *,
        after: int = 0,
        page_size: int = 100,
    ) -> AsyncIterator[StoredEvent]:
        """Replay then poll for live durable events until the consumer disconnects."""

        cursor = after
        while True:
            page = await self.read_page(
                tenant_id,
                run_id,
                after=cursor,
                limit=page_size,
            )
            if page.events:
                for event in page.events:
                    cursor = event.sequence
                    yield event
                continue
            await self._sleep(self._poll_interval)

    async def latest_sequence(self, tenant_id: uuid.UUID, run_id: uuid.UUID) -> int | None:
        """Return the last committed sequence, or ``None`` for an unknown run."""

        async with self._sessions() as database:
            run_exists = await database.scalar(
                select(RunRecord.id).where(
                    RunRecord.tenant_id == tenant_id,
                    RunRecord.id == run_id,
                )
            )
            if run_exists is None:
                return None
            value = await database.scalar(
                select(func.max(AgentEventRecord.sequence)).where(
                    AgentEventRecord.tenant_id == tenant_id,
                    AgentEventRecord.run_id == run_id,
                )
            )
        return int(value or 0)


def _stored_event(record: AgentEventRecord) -> StoredEvent:
    return StoredEvent(
        run_id=record.run_id,
        sequence=record.sequence,
        event_type=_EVENT_TYPE_ADAPTER.validate_python(record.event_type),
        payload=record.payload,
        created_at=record.created_at,
    )


__all__ = ["PostgresEventStore"]
