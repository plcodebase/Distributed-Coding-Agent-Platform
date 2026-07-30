"""Validated durable-event contracts independent of HTTP and persistence adapters."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Self

from pydantic import Field, model_validator

from agent_core.domain.base import AwareTimestamp, DomainModel, FrozenJsonObject
from agent_core.events import AnyAgentEvent, EventType, parse_agent_event

MAX_EVENT_PAGE_SIZE = 1000


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
        if self.events and len({event.run_id for event in self.events}) != 1:
            raise ValueError("event page entries must belong to one run")
        if self.events and self.next_after != self.events[-1].sequence:
            raise ValueError("next_after must equal the final event sequence")
        if not self.events and self.has_more:
            raise ValueError("an empty event page may not report more events")
        return self


__all__ = ["MAX_EVENT_PAGE_SIZE", "EventDraft", "EventPage", "StoredEvent"]
