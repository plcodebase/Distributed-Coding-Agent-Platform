"""Subprocess fixture for real-network event-gateway integration tests."""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

from agent_api import EventGatewayServices, Principal, StaticTokenAuthenticator
from agent_api.event_app import create_event_gateway_app
from agent_core.domain.base import FrozenJsonObject
from agent_core.domain.errors import DomainOperationError
from agent_core.domain.models import Run
from agent_core.domain.status import RunStatus
from agent_core.event_store import EventPage, StoredEvent

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from fastapi import FastAPI

    from agent_api.dependencies import EventStore, RunRepository

TENANT_ID = uuid.UUID("81000000-0000-0000-0000-000000000001")
OTHER_TENANT_ID = uuid.UUID("81000000-0000-0000-0000-000000000002")
RUN_ID = uuid.UUID("82000000-0000-0000-0000-000000000001")
TOKEN = "network-event-token"  # noqa: S105 - inert integration credential
OTHER_TOKEN = "network-other-token"  # noqa: S105 - inert integration credential
NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _write_marker(path: Path, value: str) -> None:
    with path.open("a", encoding="utf-8") as output:
        output.write(f"{value}\n")


def _event(sequence: int) -> StoredEvent:
    return StoredEvent(
        run_id=RUN_ID,
        sequence=sequence,
        event_type="context.build_started",
        payload=FrozenJsonObject({"message_count": sequence, "checkpoint_id": None}),
        created_at=NOW,
    )


class _Runs:
    def __init__(self) -> None:
        self._run = Run(
            id=RUN_ID,
            session_id=uuid.UUID("83000000-0000-0000-0000-000000000001"),
            workspace_id=uuid.UUID("84000000-0000-0000-0000-000000000001"),
            status=RunStatus.QUEUED,
            priority=0,
            attempt=1,
            created_at=NOW,
        )

    async def get(self, tenant_id: uuid.UUID, run_id: uuid.UUID) -> Run | None:
        if tenant_id == TENANT_ID and run_id == RUN_ID:
            return self._run
        return None


class _Events:
    def __init__(self, *, gap: bool, marker: Path) -> None:
        self._events = (_event(1), _event(2), _event(3))
        self._gap = gap
        self._marker = marker

    async def read_page(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        *,
        after: int = 0,
        limit: int = 100,
    ) -> EventPage:
        if tenant_id != TENANT_ID or run_id != RUN_ID:
            return EventPage(events=(), next_after=after, has_more=False)
        retained = tuple(event for event in self._events if event.sequence > after)[:limit]
        return EventPage(
            events=retained,
            next_after=retained[-1].sequence if retained else after,
            has_more=len(tuple(event for event in self._events if event.sequence > after)) > limit,
        )

    async def stream(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        *,
        after: int = 0,
        page_size: int = 100,
    ) -> AsyncIterator[StoredEvent]:
        del page_size
        expected = after + 1
        try:
            for event in self._events:
                if event.sequence <= after:
                    continue
                if self._gap and event.sequence == 2:
                    continue
                if event.sequence != expected:
                    raise DomainOperationError(
                        code="event_sequence_gap",
                        message="the durable event sequence is not contiguous",
                        retryable=True,
                        details={"expected_sequence": expected},
                    )
                expected += 1
                yield event
            await asyncio.Event().wait()
        finally:
            await asyncio.to_thread(_write_marker, self._marker, "stream-closed")


class _Ready:
    async def ready(self) -> bool:
        return True


def create_app() -> FastAPI:
    """Build a deterministic event-only graph in a fresh OS process."""

    marker = Path(os.environ["AGENT_EVENT_TEST_MARKER"])
    gap = os.environ.get("AGENT_EVENT_TEST_MODE") == "gap"
    events = _Events(gap=gap, marker=marker)

    async def close() -> None:
        await asyncio.to_thread(_write_marker, marker, "application-closed")

    return create_event_gateway_app(
        EventGatewayServices(
            authenticator=StaticTokenAuthenticator(
                {
                    TOKEN: Principal(tenant_id=TENANT_ID, subject="network-user"),
                    OTHER_TOKEN: Principal(
                        tenant_id=OTHER_TENANT_ID,
                        subject="network-other-user",
                    ),
                }
            ),
            runs=cast("RunRepository", _Runs()),
            events=cast("EventStore", events),
            readiness=_Ready(),
        ),
        close=close,
        max_request_body_bytes=128,
    )


__all__ = ["OTHER_TOKEN", "RUN_ID", "TOKEN", "create_app"]
