from __future__ import annotations

import uuid
from collections import deque
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Self, cast

import pytest

from agent_core.control import (
    ContextCompactionStatus,
)
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from platform_persistence import (
    PostgresContextRepository,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

NOW = datetime(2026, 7, 31, 12, tzinfo=UTC)
TENANT_ID = uuid.UUID("10000000-0000-0000-0000-000000000001")
SESSION_ID = uuid.UUID("20000000-0000-0000-0000-000000000002")
COMPACTION_ID = uuid.UUID("40000000-0000-0000-0000-000000000004")


class _Scalars:
    def __init__(self, values: list[object]) -> None:
        self.values = values
        self.consumed = 0
        self.closed = False
        self._iterator: Iterator[object] | None = None

    def __iter__(self) -> object:
        return iter(self.values)

    def __aiter__(self) -> Self:
        self._iterator = iter(self.values)
        return self

    async def __anext__(self) -> object:
        if self._iterator is None:
            raise StopAsyncIteration
        try:
            value = next(self._iterator)
        except StopIteration as error:
            raise StopAsyncIteration from error
        self.consumed += 1
        return value

    async def close(self) -> None:
        self.closed = True


class _Database:
    def __init__(
        self,
        *,
        scalar_values: list[object] | None = None,
        scalar_sets: list[list[object]] | None = None,
    ) -> None:
        self.scalar_values = deque(scalar_values or [])
        self.scalar_sets = deque(scalar_sets or [])
        self.added: list[object] = []
        self.executed = 0
        self.streams: list[_Scalars] = []

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def begin(self) -> Self:
        return self

    async def scalar(self, _statement: object) -> object:
        if not self.scalar_values:
            raise AssertionError("unexpected scalar query")
        return self.scalar_values.popleft()

    async def scalars(self, _statement: object) -> _Scalars:
        if not self.scalar_sets:
            raise AssertionError("unexpected scalar-set query")
        return _Scalars(self.scalar_sets.popleft())

    async def stream_scalars(self, _statement: object) -> _Scalars:
        if not self.scalar_sets:
            raise AssertionError("unexpected streamed scalar-set query")
        result = _Scalars(self.scalar_sets.popleft())
        self.streams.append(result)
        return result

    async def execute(self, _statement: object) -> object:
        self.executed += 1
        return object()

    def add(self, value: object) -> None:
        self.added.append(value)


class _Sessions:
    def __init__(self, *databases: _Database) -> None:
        self.databases = deque(databases)

    def __call__(self) -> _Database:
        return self.databases.popleft()


def _sessions(*databases: _Database) -> Any:
    return cast("Any", _Sessions(*databases))


def _compaction_row(
    *,
    status: ContextCompactionStatus = ContextCompactionStatus.PENDING,
) -> SimpleNamespace:
    terminal = status is not ContextCompactionStatus.PENDING
    return SimpleNamespace(
        id=COMPACTION_ID,
        tenant_id=TENANT_ID,
        session_id=SESSION_ID,
        status=status.value,
        idempotency_key="compact-1",
        source_message_sequence=12,
        route_name="coding-default",
        summary="summary" if status is ContextCompactionStatus.COMPLETED else None,
        input_tokens=10 if status is ContextCompactionStatus.COMPLETED else None,
        output_tokens=4 if status is ContextCompactionStatus.COMPLETED else None,
        error=(
            {"code": "failed", "message": "failed", "retryable": True, "details": {}}
            if status is ContextCompactionStatus.FAILED
            else None
        ),
        requested_at=NOW,
        completed_at=NOW + timedelta(seconds=1) if terminal else None,
    )


@pytest.mark.asyncio
async def test_context_repository_requests_replays_and_finishes_compaction() -> None:
    inserted = _compaction_row()
    create_db = _Database(
        scalar_values=[SESSION_ID, None, None, 12, inserted],
    )
    repository = PostgresContextRepository(_sessions(create_db))
    created = await repository.request_compaction(
        TENANT_ID,
        SESSION_ID,
        compaction_id=COMPACTION_ID,
        idempotency_key="compact-1",
        route_name="coding-default",
        requested_at=NOW,
    )
    assert created is not None
    assert created.source_message_sequence == 12
    assert create_db.executed == 1

    fetched = await PostgresContextRepository(_sessions(_Database(scalar_values=[inserted]))).get(
        TENANT_ID, SESSION_ID, COMPACTION_ID
    )
    assert fetched == created

    replay_repository = PostgresContextRepository(
        _sessions(_Database(scalar_values=[SESSION_ID, inserted]))
    )
    replay = await replay_repository.request_compaction(
        TENANT_ID,
        SESSION_ID,
        compaction_id=uuid.uuid4(),
        idempotency_key="compact-1",
        route_name="coding-default",
        requested_at=NOW,
    )
    assert replay is not None and replay.id == COMPACTION_ID

    with pytest.raises(DomainOperationError) as pending:
        await PostgresContextRepository(
            _sessions(_Database(scalar_values=[SESSION_ID, None, inserted]))
        ).request_compaction(
            TENANT_ID,
            SESSION_ID,
            compaction_id=uuid.uuid4(),
            idempotency_key="compact-2",
            route_name="coding-default",
            requested_at=NOW,
        )
    assert pending.value.code == "context_compaction_in_progress"
    assert pending.value.details["compaction_id"] == str(COMPACTION_ID)

    completion_row = _compaction_row()
    completed = await PostgresContextRepository(
        _sessions(_Database(scalar_values=[completion_row]))
    ).complete(
        TENANT_ID,
        COMPACTION_ID,
        summary="new summary",
        input_tokens=20,
        output_tokens=5,
        completed_at=NOW + timedelta(seconds=2),
    )
    assert completed is not None
    assert completed.status is ContextCompactionStatus.COMPLETED
    assert completed.summary == "new summary"

    with pytest.raises(DomainOperationError) as conflict:
        await PostgresContextRepository(
            _sessions(_Database(scalar_values=[SESSION_ID, _compaction_row()]))
        ).request_compaction(
            TENANT_ID,
            SESSION_ID,
            compaction_id=uuid.uuid4(),
            idempotency_key="compact-1",
            route_name="another-route",
            requested_at=NOW,
        )
    assert conflict.value.code == "context_compaction_idempotency_conflict"


@pytest.mark.asyncio
async def test_context_repository_reads_pending_latest_and_failed_outcomes() -> None:
    repository = PostgresContextRepository(
        _sessions(
            _Database(scalar_values=[_compaction_row()]),
            _Database(scalar_values=[_compaction_row(status=ContextCompactionStatus.COMPLETED)]),
            _Database(scalar_values=[_compaction_row()]),
        )
    )
    assert await repository.pending_for_session(TENANT_ID, SESSION_ID) is not None
    latest = await repository.latest_completed(TENANT_ID, SESSION_ID)
    assert latest is not None and latest.summary == "summary"
    failed = await repository.fail(
        TENANT_ID,
        COMPACTION_ID,
        error=ErrorDetail(code="context_failed", message="failed", retryable=True),
        completed_at=NOW + timedelta(seconds=2),
    )
    assert failed is not None and failed.status is ContextCompactionStatus.FAILED
