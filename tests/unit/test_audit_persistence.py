from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, Self, cast

import pytest

from agent_core.audit import AuditEntry
from platform_persistence.audit import PostgresAuditSink
from platform_persistence.models import AuditRecord

NOW = datetime(2026, 8, 20, tzinfo=UTC)


class _Database:
    def __init__(self) -> None:
        self.added: list[object] = []

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def begin(self) -> Self:
        return self

    def add(self, value: object) -> None:
        self.added.append(value)


@pytest.mark.asyncio
async def test_postgres_audit_sink_maps_immutable_entry() -> None:
    database = _Database()
    sink = PostgresAuditSink(cast("Any", lambda: database))
    entry = AuditEntry(
        id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        subject="user@example.invalid",
        method="POST",
        resource="/v1/runs",
        action="run.create",
        request_id="request-1",
        details={"run_id": str(uuid.uuid4())},
        occurred_at=NOW,
    )

    assert await sink.append(entry) == entry
    row = database.added[0]
    assert isinstance(row, AuditRecord)
    assert row.id == entry.id
    assert row.details == entry.details.to_json_object()
