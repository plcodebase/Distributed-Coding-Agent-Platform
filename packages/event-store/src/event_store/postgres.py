"""PostgreSQL-backed atomic event sequencing and replay."""

from __future__ import annotations

import asyncio
import math
from contextlib import suppress
from typing import TYPE_CHECKING

import anyio
from pydantic import TypeAdapter, ValidationError
from sqlalchemy import func, select, update

from agent_core.domain.errors import DomainOperationError
from agent_core.event_store import (
    EVENT_PAGE_ENVELOPE_RESERVE_BYTES,
    MAX_EVENT_PAGE_BYTES,
    MAX_EVENT_PAGE_SIZE,
    MIN_EVENT_PAGE_BYTES,
    EventDeliveryKey,
    EventDraft,
    EventPage,
    StoredEvent,
)
from agent_core.events import EventType
from platform_persistence.models import AgentEventRecord, RunRecord

if TYPE_CHECKING:
    import uuid
    from collections.abc import AsyncIterator, Awaitable, Callable

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

_EVENT_TYPE_ADAPTER: TypeAdapter[EventType] = TypeAdapter(EventType)
_DELIVERY_KEY_ADAPTER: TypeAdapter[EventDeliveryKey] = TypeAdapter(EventDeliveryKey)
MIN_POLL_INTERVAL_SECONDS = 0.01
MAX_POLL_INTERVAL_SECONDS = 10.0


class PostgresEventStore:
    """Append-only event store with run-row sequence allocation."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        poll_interval_seconds: float = 0.25,
        max_page_bytes: int = MAX_EVENT_PAGE_BYTES,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if (
            isinstance(poll_interval_seconds, bool)
            or not isinstance(poll_interval_seconds, (int, float))
            or not math.isfinite(poll_interval_seconds)
            or not MIN_POLL_INTERVAL_SECONDS <= poll_interval_seconds <= MAX_POLL_INTERVAL_SECONDS
        ):
            raise ValueError("poll_interval_seconds must be in [0.01, 10]")
        if (
            type(max_page_bytes) is not int
            or not MIN_EVENT_PAGE_BYTES <= max_page_bytes <= MAX_EVENT_PAGE_BYTES
        ):
            raise ValueError(
                f"max_page_bytes must be in [{MIN_EVENT_PAGE_BYTES}, {MAX_EVENT_PAGE_BYTES}]"
            )
        self._sessions = sessions
        self._poll_interval = poll_interval_seconds
        self._max_page_bytes = max_page_bytes
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

    async def append_idempotent(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        delivery_key: EventDeliveryKey,
        draft: EventDraft,
    ) -> StoredEvent:
        """Append once for a stable worker delivery key."""

        try:
            validated_key = _DELIVERY_KEY_ADAPTER.validate_python(delivery_key)
        except ValidationError:
            raise DomainOperationError(
                code="event_delivery_key_invalid",
                message="the event delivery key is invalid",
            ) from None
        async with self._sessions() as database, database.begin():
            run_row = await database.scalar(
                select(RunRecord)
                .where(
                    RunRecord.tenant_id == tenant_id,
                    RunRecord.id == run_id,
                )
                .with_for_update()
            )
            if run_row is None:
                raise DomainOperationError(
                    code="run_not_found",
                    message="the run does not exist for this tenant",
                    details={"run_id": str(run_id)},
                )
            existing = await database.scalar(
                select(AgentEventRecord).where(
                    AgentEventRecord.tenant_id == tenant_id,
                    AgentEventRecord.run_id == run_id,
                    AgentEventRecord.delivery_key == validated_key,
                )
            )
            if existing is not None:
                if (
                    existing.event_type != draft.event_type.value
                    or existing.payload != draft.payload.to_json_object()
                ):
                    raise DomainOperationError(
                        code="event_delivery_conflict",
                        message="the event delivery key belongs to different event data",
                        details={"run_id": str(run_id), "delivery_key": validated_key},
                    )
                return _stored_event(existing)
            sequence = run_row.next_event_sequence
            run_row.next_event_sequence += 1
            row = AgentEventRecord(
                tenant_id=tenant_id,
                run_id=run_id,
                sequence=sequence,
                delivery_key=validated_key,
                event_type=draft.event_type.value,
                payload=draft.payload.to_json_object(),
                created_at=draft.created_at,
            )
            database.add(row)
            await database.flush()
            return _stored_event(row)

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
        retained: list[StoredEvent] = []
        retained_bytes = EVENT_PAGE_ENVELOPE_RESERVE_BYTES
        has_more = False
        expected_sequence = after + 1
        async with self._sessions() as database:
            result = await database.stream_scalars(
                select(AgentEventRecord)
                .where(
                    AgentEventRecord.tenant_id == tenant_id,
                    AgentEventRecord.run_id == run_id,
                    AgentEventRecord.sequence > after,
                )
                .order_by(AgentEventRecord.sequence)
                .limit(limit + 1)
                .execution_options(yield_per=1)
            )
            try:
                async for row in result:
                    event = _stored_event(row)
                    if event.sequence != expected_sequence:
                        raise DomainOperationError(
                            code="event_sequence_gap",
                            message="the durable event sequence is not contiguous",
                            retryable=True,
                            details={
                                "run_id": str(run_id),
                                "expected_sequence": expected_sequence,
                            },
                        )
                    expected_sequence += 1
                    event_bytes = event.serialized_size_bytes + 1
                    if (
                        len(retained) >= limit
                        or retained_bytes + event_bytes > self._max_page_bytes
                    ):
                        has_more = True
                        break
                    retained.append(event)
                    retained_bytes += event_bytes
            finally:
                await result.close()
        events = tuple(retained)
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
