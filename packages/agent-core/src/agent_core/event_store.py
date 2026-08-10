"""Validated durable-event contracts independent of HTTP and persistence adapters."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from itertools import pairwise
from typing import TYPE_CHECKING, Annotated, Protocol, Self

from pydantic import Field, StringConstraints, model_validator

from agent_core.domain.base import AwareTimestamp, DomainModel, FrozenJsonObject
from agent_core.events import (
    MAX_EVENT_PAYLOAD_BYTES,
    AnyAgentEvent,
    EventType,
    parse_agent_event,
)

if TYPE_CHECKING:
    from agent_core.distributed import RunLease

MAX_EVENT_PAGE_SIZE = 1000
MAX_EVENT_PAGE_BYTES = 4 * 1024 * 1024
MIN_EVENT_PAGE_BYTES = MAX_EVENT_PAYLOAD_BYTES + 64 * 1024
EVENT_PAGE_ENVELOPE_RESERVE_BYTES = 1024
type EventDeliveryKey = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=255,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    ),
]


class EventDraft(DomainModel):
    """One typed event awaiting a database-allocated sequence."""

    event_type: EventType
    payload: FrozenJsonObject
    created_at: AwareTimestamp = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def validate_typed_payload(self) -> Self:
        _ = parse_agent_event(
            {
                "run_id": uuid.UUID(int=0),
                "sequence": 1,
                "event_type": self.event_type,
                "payload": self.payload.to_json_object(),
                "created_at": self.created_at,
            }
        )
        return self


class StoredEvent(DomainModel):
    """Tenant-safe event returned by durable sequence and replay operations."""

    run_id: uuid.UUID
    sequence: int = Field(ge=1)
    event_type: EventType
    payload: FrozenJsonObject
    created_at: AwareTimestamp

    @model_validator(mode="after")
    def validate_typed_event(self) -> Self:
        _ = self.to_agent_event()
        return self

    def to_agent_event(self) -> AnyAgentEvent:
        """Rebuild the concrete core event for worker-side consumers."""

        return parse_agent_event(self.model_dump(mode="python"))

    @property
    def serialized_size_bytes(self) -> int:
        """Return the exact UTF-8 size of this event's compact JSON representation."""

        return len(self.model_dump_json().encode("utf-8"))


class EventPage(DomainModel):
    """Bounded reconnect page returned by the HTTP API."""

    events: tuple[StoredEvent, ...] = Field(max_length=MAX_EVENT_PAGE_SIZE)
    next_after: int = Field(ge=0)
    has_more: bool

    @model_validator(mode="after")
    def validate_cursor(self) -> Self:
        sequences = [event.sequence for event in self.events]
        if sequences != sorted(sequences) or len(sequences) != len(set(sequences)):
            raise ValueError("event page sequences must be strictly increasing")
        if any(current != previous + 1 for previous, current in pairwise(sequences)):
            raise ValueError("event page sequences must be contiguous")
        if self.events and len({event.run_id for event in self.events}) != 1:
            raise ValueError("event page entries must belong to one run")
        if self.events and self.next_after != self.events[-1].sequence:
            raise ValueError("next_after must equal the final event sequence")
        if not self.events and self.has_more:
            raise ValueError("an empty event page may not report more events")
        serialized = json.dumps(
            self.model_dump(mode="json"),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(serialized) > MAX_EVENT_PAGE_BYTES:
            raise ValueError(f"serialized event page exceeds {MAX_EVENT_PAGE_BYTES}-byte limit")
        return self


class IdempotentEventStore(Protocol):
    """Append worker events once under at-least-once delivery."""

    async def append_idempotent_fenced(
        self,
        lease: RunLease,
        delivery_key: EventDeliveryKey,
        draft: EventDraft,
    ) -> StoredEvent:
        """Append only for the active run lease, or return its identical prior event."""


__all__ = [
    "EVENT_PAGE_ENVELOPE_RESERVE_BYTES",
    "MAX_EVENT_PAGE_BYTES",
    "MAX_EVENT_PAGE_SIZE",
    "MIN_EVENT_PAGE_BYTES",
    "EventDeliveryKey",
    "EventDraft",
    "EventPage",
    "IdempotentEventStore",
    "StoredEvent",
]
