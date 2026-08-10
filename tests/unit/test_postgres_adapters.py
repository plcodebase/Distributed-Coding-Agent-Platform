from __future__ import annotations

import uuid
from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import MethodType, SimpleNamespace
from typing import Any, Self, cast

import pytest

from agent_core.capacity import TenantQuota
from agent_core.control import (
    ApprovalDecision,
    ApprovalStatus,
    PersistedApproval,
    PersistedMessage,
    PersistedTaskPlan,
    run_creation_hash,
)
from agent_core.domain.base import FrozenJsonObject
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.domain.models import (
    Checkpoint,
    ModelCall,
    Run,
    Session,
    ToolCall,
    canonical_argument_hash,
)
from agent_core.domain.status import (
    ApprovalMode,
    ModelCallStatus,
    RunStatus,
    SessionStatus,
    ToolCallStatus,
)
from agent_core.event_store import MIN_EVENT_PAGE_BYTES, EventDraft, EventPage, StoredEvent
from agent_core.gateway import GatewayFinishReason, GatewayResponseCompleted, GatewayTextDelta
from agent_core.gateway_reliability import GatewayRequestClaimStatus
from agent_core.scheduling import QueueAdmissionPolicy
from event_store import PostgresEventStore
from platform_persistence import (
    PostgresApprovalRepository,
    PostgresExecutionRepository,
    PostgresGatewayCapacityStore,
    PostgresGatewayCircuitBreaker,
    PostgresGatewayRateLimiter,
    PostgresGatewayRequestStore,
    PostgresRunRepository,
    PostgresSessionRepository,
    PostgresTenantQuotaRepository,
)

TENANT_ID = uuid.uuid4()
NOW = datetime(2026, 1, 2, 3, 4, tzinfo=UTC)
REQUEST_HASH = "a" * 64


class _ScalarRows:
    def __init__(self, rows: list[object]) -> None:
        self._rows = rows
        self._iterator = iter(cast("tuple[object, ...]", ()))

    def all(self) -> list[object]:
        return self._rows

    def __aiter__(self) -> _ScalarRows:
        self._iterator = iter(self._rows)
        return self

    async def __anext__(self) -> object:
        try:
            return next(self._iterator)
        except StopIteration as error:
            raise StopAsyncIteration from error

    async def close(self) -> None:
        return None


class _FakeDatabase:
    def __init__(
        self,
        *,
        scalars: list[object] | None = None,
        row_pages: list[list[object]] | None = None,
    ) -> None:
        self.scalar_values = deque(scalars or [])
        self.row_pages = deque(row_pages or [])
        self.added: list[object] = []
        self.deleted: list[object] = []
        self.executed = 0
        self.flushes = 0

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

    async def stream_scalars(self, _statement: object) -> _ScalarRows:
        if not self.row_pages:
            raise AssertionError("unexpected scalars query")
        return _ScalarRows(self.row_pages.popleft())

    async def execute(self, _statement: object) -> None:
        self.executed += 1

    def add(self, value: object) -> None:
        self.added.append(value)

    async def delete(self, value: object) -> None:
        self.deleted.append(value)

    async def flush(self) -> None:
        self.flushes += 1


class _SessionFactory:
    def __init__(self, *databases: _FakeDatabase) -> None:
        self._databases = deque(databases)

    def __call__(self) -> _FakeDatabase:
        if not self._databases:
            raise AssertionError("unexpected database session")
        return self._databases.popleft()


def _sessions(*databases: _FakeDatabase) -> Any:
    return cast("Any", _SessionFactory(*databases))


def _session() -> Session:
    return Session(
        id=uuid.uuid4(),
        tenant_id=TENANT_ID,
        workspace_id=uuid.uuid4(),
        status=SessionStatus.ACTIVE,
        approval_mode=ApprovalMode.REQUIRE_SENSITIVE,
        model_route="primary",
        created_at=NOW,
        updated_at=NOW,
    )


def _run(
    *,
    status: RunStatus = RunStatus.QUEUED,
    run_id: uuid.UUID | None = None,
) -> Run:
    started = (
        NOW + timedelta(seconds=1)
        if status
        in {
            RunStatus.RUNNING,
            RunStatus.WAITING_APPROVAL,
            RunStatus.RETRY_PENDING,
            RunStatus.COMPLETED,
            RunStatus.FAILED,
        }
        else None
    )
    terminal = (
        NOW + timedelta(seconds=2)
        if status
        in {
            RunStatus.COMPLETED,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
        }
        else None
    )
    assigned = "worker-1" if status in {RunStatus.LEASED, RunStatus.RUNNING} else None
    lease = NOW + timedelta(minutes=5) if assigned is not None else None
    return Run(
        id=run_id or uuid.uuid4(),
        session_id=uuid.uuid4(),
        workspace_id=uuid.uuid4(),
        status=status,
        priority=0,
        attempt=1,
        assigned_worker_id=assigned,
        lease_expires_at=lease,
        created_at=NOW,
        started_at=started,
        completed_at=terminal,
    )


def _run_row(run: Run, *, creation_hash: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        id=run.id,
        tenant_id=TENANT_ID,
        session_id=run.session_id,
        workspace_id=run.workspace_id,
        status=run.status.value,
        priority_class=run.priority_class.value,
        priority=run.priority,
        attempt=run.attempt,
        assigned_worker_id=run.assigned_worker_id,
        lease_expires_at=run.lease_expires_at,
        last_checkpoint_id=run.last_checkpoint_id,
        cancellation_requested=run.cancellation_requested,
        idempotency_key="request-1",
        creation_hash=creation_hash
        or run_creation_hash(
            priority=run.priority,
            priority_class=run.priority_class,
        ),
        created_at=run.created_at,
        started_at=run.started_at,
        completed_at=run.completed_at,
    )


def _stored_event(run_id: uuid.UUID, sequence: int) -> StoredEvent:
    return StoredEvent(
        run_id=run_id,
        sequence=sequence,
        event_type="context.build_started",
        payload={"message_count": sequence, "checkpoint_id": None},
        created_at=NOW,
    )


def _event_row(run_id: uuid.UUID, sequence: int) -> SimpleNamespace:
    return SimpleNamespace(
        run_id=run_id,
        sequence=sequence,
        event_type="context.build_started",
        payload={"message_count": sequence, "checkpoint_id": None},
        created_at=NOW,
    )


@pytest.mark.parametrize(
    ("requests", "seconds"),
    [
        (0, 1.0),
        (True, 1.0),
        (1_000_001, 1.0),
        (1, 0.0),
        (1, float("inf")),
        (1, True),
    ],
)
def test_postgres_rate_limiter_rejects_invalid_configuration(
    requests: object,
    seconds: object,
) -> None:
    with pytest.raises(ValueError):
        PostgresGatewayRateLimiter(
            _sessions(),
            requests_per_window=cast("Any", requests),
            window_seconds=cast("Any", seconds),
        )


@pytest.mark.asyncio
async def test_postgres_rate_limiter_enforces_and_resets_shared_window() -> None:
    active = SimpleNamespace(window_started_at=NOW, request_count=0)
    limited = SimpleNamespace(window_started_at=NOW, request_count=2)
    expired = SimpleNamespace(window_started_at=NOW - timedelta(seconds=11), request_count=99)
    limiter = PostgresGatewayRateLimiter(
        _sessions(
            _FakeDatabase(scalars=[active]),
            _FakeDatabase(scalars=[limited]),
            _FakeDatabase(scalars=[expired]),
        ),
        requests_per_window=2,
        window_seconds=10,
        clock=lambda: NOW,
    )

    assert await limiter.acquire(TENANT_ID, "primary") is None
    assert active.request_count == 1
    assert await limiter.acquire(TENANT_ID, "primary") == 10
    assert await limiter.acquire(TENANT_ID, "primary") is None
    assert expired.request_count == 1
    assert expired.window_started_at == NOW

    missing = PostgresGatewayRateLimiter(
        _sessions(_FakeDatabase(scalars=[None])),
        requests_per_window=1,
        window_seconds=1,
        clock=lambda: NOW,
    )
    with pytest.raises(RuntimeError, match="disappeared"):
        await missing.acquire(TENANT_ID, "primary")

    naive = PostgresGatewayRateLimiter(
        _sessions(),
        requests_per_window=1,
        window_seconds=1,
        clock=lambda: NOW.replace(tzinfo=None),
    )
    with pytest.raises(ValueError, match="aware"):
        await naive.acquire(TENANT_ID, "primary")
    invalid_clock = PostgresGatewayRateLimiter(
        _sessions(),
        requests_per_window=1,
        window_seconds=1,
        clock=cast("Any", lambda: "not-a-datetime"),
    )
    with pytest.raises(TypeError, match="datetime"):
        await invalid_clock.acquire(TENANT_ID, "primary")
    invalid_route = PostgresGatewayRateLimiter(
        _sessions(),
        requests_per_window=1,
        window_seconds=1,
        clock=lambda: NOW,
    )
    with pytest.raises(ValueError, match="route"):
        await invalid_route.acquire(TENANT_ID, " ")


@pytest.mark.asyncio
async def test_postgres_capacity_store_acquires_reconciles_and_releases_atomically() -> None:
    quota = SimpleNamespace(
        tenant_id=TENANT_ID,
        max_active_runs=2,
        max_queued_runs=10,
        max_gateway_requests=2,
        memory_enabled=True,
        created_at=NOW,
        updated_at=NOW,
    )
    route = SimpleNamespace(
        route_name="coding-default",
        request_limit=9,
        token_limit=999,
        token_window_seconds=Decimal("30.000"),
        token_window_started_at=NOW,
        accounted_tokens=10,
        updated_at=NOW,
    )
    lease_id = uuid.UUID("40000000-0000-0000-0000-000000000001")
    acquire_database = _FakeDatabase(scalars=[quota, route, None, 0, 0])
    release_route = SimpleNamespace(**vars(route))
    store = PostgresGatewayCapacityStore(
        _sessions(
            acquire_database,
            _FakeDatabase(scalars=[None]),
        ),
        provider_request_limit=3,
        provider_token_limit=100,
        token_window_seconds=60,
        clock=lambda: NOW,
        id_factory=lambda: lease_id,
    )

    claim = await store.acquire(
        TENANT_ID,
        "coding-default",
        "request-capacity-1",
        reserved_tokens=20,
        lease_duration=timedelta(seconds=30),
    )
    assert claim.lease is not None
    assert claim.lease.id == lease_id
    assert route.accounted_tokens == 30
    assert route.request_limit == 3
    assert route.token_limit == 100
    assert route.token_window_seconds == Decimal("60.0")
    assert len(acquire_database.added) == 1

    lease_row = acquire_database.added[0]
    renewal_database = _FakeDatabase(scalars=[lease_row])
    renewing = PostgresGatewayCapacityStore(
        _sessions(renewal_database),
        provider_request_limit=3,
        provider_token_limit=100,
        token_window_seconds=60,
        clock=lambda: NOW + timedelta(seconds=1),
    )
    renewed = await renewing.renew(claim.lease, lease_duration=timedelta(seconds=30))
    assert renewed.expires_at == NOW + timedelta(seconds=31)
    assert renewal_database.flushes == 1

    release_route.accounted_tokens = route.accounted_tokens
    release_database = _FakeDatabase(scalars=[lease_row, release_route])
    releasing = PostgresGatewayCapacityStore(
        _sessions(release_database),
        provider_request_limit=3,
        provider_token_limit=100,
        token_window_seconds=60,
        clock=lambda: NOW + timedelta(seconds=1),
    )
    await releasing.release(renewed, consumed_tokens=7)
    assert release_route.accounted_tokens == 17
    assert release_database.deleted == [lease_row]

    await store.release(claim.lease, consumed_tokens=7)


@pytest.mark.asyncio
async def test_postgres_tenant_quota_repository_persists_validated_overrides() -> None:
    row = SimpleNamespace(
        tenant_id=TENANT_ID,
        max_active_runs=4,
        max_queued_runs=100,
        max_gateway_requests=4,
        memory_enabled=True,
        created_at=NOW,
        updated_at=NOW,
    )
    repository = PostgresTenantQuotaRepository(
        _sessions(_FakeDatabase(scalars=[row]), _FakeDatabase(scalars=[row])),
        clock=lambda: NOW,
    )
    assert await repository.get(TENANT_ID) == TenantQuota()
    updated = TenantQuota(
        max_active_runs=2,
        max_queued_runs=20,
        max_gateway_requests=3,
        memory_enabled=False,
    )
    assert (
        await repository.set(
            TENANT_ID,
            updated,
            occurred_at=NOW + timedelta(seconds=1),
        )
        == updated
    )
    assert row.max_active_runs == 2
    assert row.memory_enabled is False


@pytest.mark.parametrize(
    ("threshold", "seconds"),
    [
        (0, 1.0),
        (True, 1.0),
        (101, 1.0),
        (1, 0.0),
        (1, float("nan")),
        (1, True),
    ],
)
def test_postgres_circuit_breaker_rejects_invalid_configuration(
    threshold: object,
    seconds: object,
) -> None:
    with pytest.raises(ValueError):
        PostgresGatewayCircuitBreaker(
            _sessions(),
            failure_threshold=cast("Any", threshold),
            recovery_seconds=cast("Any", seconds),
        )


@pytest.mark.asyncio
async def test_postgres_circuit_breaker_transitions_and_probes() -> None:
    closed = SimpleNamespace(
        failure_count=0,
        opened_at=None,
        probe_in_flight=False,
        probe_started_at=None,
        updated_at=NOW,
    )
    open_recent = SimpleNamespace(
        failure_count=2,
        opened_at=NOW - timedelta(seconds=1),
        probe_in_flight=False,
        probe_started_at=None,
        updated_at=NOW,
    )
    probing = SimpleNamespace(
        failure_count=2,
        opened_at=NOW - timedelta(seconds=20),
        probe_in_flight=True,
        probe_started_at=NOW - timedelta(seconds=1),
        updated_at=NOW,
    )
    stale_probe = SimpleNamespace(
        failure_count=2,
        opened_at=NOW - timedelta(seconds=20),
        probe_in_flight=True,
        probe_started_at=NOW - timedelta(seconds=20),
        updated_at=NOW,
    )
    success = SimpleNamespace(
        failure_count=2,
        opened_at=NOW,
        probe_in_flight=True,
        probe_started_at=NOW,
        updated_at=NOW,
    )
    failure = SimpleNamespace(
        failure_count=1,
        opened_at=None,
        probe_in_flight=True,
        probe_started_at=NOW,
        updated_at=NOW,
    )
    breaker = PostgresGatewayCircuitBreaker(
        _sessions(
            *(
                _FakeDatabase(scalars=[row])
                for row in (closed, open_recent, probing, stale_probe, success, failure)
            )
        ),
        failure_threshold=2,
        recovery_seconds=10,
        clock=lambda: NOW,
    )

    assert await breaker.allow("primary") is True
    assert await breaker.allow("primary") is False
    assert await breaker.allow("primary") is False
    assert await breaker.allow("primary") is True
    assert stale_probe.probe_in_flight is True
    assert stale_probe.probe_started_at == NOW

    await breaker.record_success("primary")
    assert success.failure_count == 0
    assert success.opened_at is None
    assert success.probe_in_flight is False

    await breaker.record_failure("primary")
    assert failure.failure_count == 2
    assert failure.opened_at == NOW
    assert failure.probe_in_flight is False

    missing = PostgresGatewayCircuitBreaker(
        _sessions(_FakeDatabase(scalars=[None])),
        failure_threshold=1,
        recovery_seconds=1,
        clock=lambda: NOW,
    )
    with pytest.raises(RuntimeError, match="disappeared"):
        await missing.allow("primary")


@pytest.mark.asyncio
async def test_postgres_gateway_store_claim_outcomes_and_terminal_cas() -> None:
    completed_events = [
        GatewayTextDelta(delta="done").model_dump(mode="json"),
        GatewayResponseCompleted(
            finish_reason=GatewayFinishReason.STOP,
            model="model",
        ).model_dump(mode="json"),
    ]
    completed_row = SimpleNamespace(
        status=GatewayRequestClaimStatus.COMPLETED.value,
        request_hash=REQUEST_HASH,
        events=completed_events,
        error=None,
    )
    failed_row = SimpleNamespace(
        status=GatewayRequestClaimStatus.FAILED.value,
        request_hash=REQUEST_HASH,
        events=[],
        error={
            "code": "gateway_failed",
            "message": "upstream failed",
            "details": {},
            "retryable": True,
        },
    )
    store = PostgresGatewayRequestStore(
        _sessions(
            _FakeDatabase(scalars=["request-1"]),
            _FakeDatabase(scalars=[None, completed_row]),
            _FakeDatabase(
                scalars=[
                    None,
                    SimpleNamespace(
                        status=GatewayRequestClaimStatus.COMPLETED.value,
                        request_hash="b" * 64,
                        events=completed_events,
                        error=None,
                    ),
                ]
            ),
            _FakeDatabase(scalars=[None, failed_row]),
            _FakeDatabase(scalars=[None, None]),
            _FakeDatabase(scalars=["request-1"]),
            _FakeDatabase(scalars=["request-1"]),
            _FakeDatabase(scalars=["request-1"]),
        )
    )

    execute = await store.claim(TENANT_ID, "request-1", REQUEST_HASH)
    assert execute.status is GatewayRequestClaimStatus.EXECUTE
    replay = await store.claim(TENANT_ID, "request-1", REQUEST_HASH)
    assert replay.status is GatewayRequestClaimStatus.COMPLETED
    assert len(replay.events) == 2
    conflict = await store.claim(TENANT_ID, "request-1", REQUEST_HASH)
    assert conflict.status is GatewayRequestClaimStatus.CONFLICT
    failed = await store.claim(TENANT_ID, "request-1", REQUEST_HASH)
    assert failed.status is GatewayRequestClaimStatus.FAILED
    assert failed.error is not None and failed.error.code == "gateway_failed"
    with pytest.raises(RuntimeError, match="disappeared"):
        await store.claim(TENANT_ID, "request-1", REQUEST_HASH)

    events = (GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP, model="model"),)
    await store.complete(TENANT_ID, "request-1", REQUEST_HASH, events)
    await store.fail(
        TENANT_ID,
        "request-1",
        REQUEST_HASH,
        ErrorDetail(code="gateway_failed", message="failed", retryable=True),
    )
    await store.release(TENANT_ID, "request-1", REQUEST_HASH)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("request_id", "request_hash"),
    [("", REQUEST_HASH), ("bad request", REQUEST_HASH), ("request-1", "invalid")],
)
async def test_postgres_gateway_store_validates_request_identity(
    request_id: str,
    request_hash: str,
) -> None:
    store = PostgresGatewayRequestStore(_sessions())
    with pytest.raises(ValueError, match="identity"):
        await store.claim(
            TENANT_ID,
            cast("Any", request_id),
            cast("Any", request_hash),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["complete", "fail", "release"])
async def test_postgres_gateway_store_rejects_lost_claim(operation: str) -> None:
    store = PostgresGatewayRequestStore(_sessions(_FakeDatabase(scalars=[None])))
    with pytest.raises(RuntimeError, match="lost its claim"):
        if operation == "complete":
            await store.complete(
                TENANT_ID,
                "request-1",
                REQUEST_HASH,
                (GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP),),
            )
        elif operation == "fail":
            await store.fail(
                TENANT_ID,
                "request-1",
                REQUEST_HASH,
                ErrorDetail(code="gateway_failed", message="failed"),
            )
        else:
            await store.release(TENANT_ID, "request-1", REQUEST_HASH)


@pytest.mark.parametrize(
    ("poll_interval", "page_bytes"),
    [
        (True, MIN_EVENT_PAGE_BYTES),
        (0.0, MIN_EVENT_PAGE_BYTES),
        (10.1, MIN_EVENT_PAGE_BYTES),
        (float("nan"), MIN_EVENT_PAGE_BYTES),
        (0.25, True),
        (0.25, MIN_EVENT_PAGE_BYTES - 1),
        (0.25, 4 * 1024 * 1024 + 1),
    ],
)
def test_postgres_event_store_rejects_invalid_configuration(
    poll_interval: object,
    page_bytes: object,
) -> None:
    with pytest.raises(ValueError):
        PostgresEventStore(
            _sessions(),
            poll_interval_seconds=cast("Any", poll_interval),
            max_page_bytes=cast("Any", page_bytes),
        )


@pytest.mark.asyncio
async def test_postgres_event_store_append_page_and_latest_sequence() -> None:
    run_id = uuid.uuid4()
    draft = EventDraft(
        event_type="context.build_started",
        payload={"message_count": 1, "checkpoint_id": None},
        created_at=NOW,
    )
    append_database = _FakeDatabase(scalars=[2])
    page_database = _FakeDatabase(
        row_pages=[[_event_row(run_id, 1), _event_row(run_id, 2), _event_row(run_id, 3)]]
    )
    store = PostgresEventStore(
        _sessions(
            append_database,
            page_database,
            _FakeDatabase(scalars=[run_id, 3]),
            _FakeDatabase(scalars=[None]),
            _FakeDatabase(scalars=[run_id, None]),
        )
    )

    appended = await store.append(TENANT_ID, run_id, draft)
    assert appended.sequence == 1
    assert len(append_database.added) == 1

    page = await store.read_page(TENANT_ID, run_id, limit=2)
    assert [event.sequence for event in page.events] == [1, 2]
    assert page.next_after == 2
    assert page.has_more is True

    assert await store.latest_sequence(TENANT_ID, run_id) == 3
    assert await store.latest_sequence(TENANT_ID, uuid.uuid4()) is None
    assert await store.latest_sequence(TENANT_ID, run_id) == 0

    missing_store = PostgresEventStore(_sessions(_FakeDatabase(scalars=[None])))
    with pytest.raises(DomainOperationError) as error:
        await missing_store.append(TENANT_ID, run_id, draft)
    assert error.value.code == "run_not_found"


@pytest.mark.asyncio
async def test_postgres_event_store_limits_page_bytes_before_materializing_all_rows() -> None:
    run_id = uuid.uuid4()
    large_text = "x" * 600_000
    rows: list[object] = [
        SimpleNamespace(
            run_id=run_id,
            sequence=sequence,
            event_type="model.text_delta",
            payload={"model_call_id": "call", "delta": large_text},
            created_at=NOW,
        )
        for sequence in range(1, 4)
    ]
    store = PostgresEventStore(
        _sessions(_FakeDatabase(row_pages=[rows])),
        max_page_bytes=MIN_EVENT_PAGE_BYTES,
    )

    page = await store.read_page(TENANT_ID, run_id, limit=1000)

    assert [event.sequence for event in page.events] == [1]
    assert page.next_after == 1
    assert page.has_more is True


@pytest.mark.asyncio
async def test_postgres_event_store_fails_closed_on_sequence_gap() -> None:
    run_id = uuid.uuid4()
    store = PostgresEventStore(_sessions(_FakeDatabase(row_pages=[[_event_row(run_id, 2)]])))

    with pytest.raises(DomainOperationError) as error:
        await store.read_page(TENANT_ID, run_id)

    assert error.value.code == "event_sequence_gap"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("after", "limit"),
    [(-1, 1), (True, 1), (0, 0), (0, 1001), (0, True)],
)
async def test_postgres_event_store_rejects_invalid_page_arguments(
    after: object,
    limit: object,
) -> None:
    store = PostgresEventStore(_sessions())
    with pytest.raises(ValueError):
        await store.read_page(
            TENANT_ID,
            uuid.uuid4(),
            after=cast("Any", after),
            limit=cast("Any", limit),
        )


@pytest.mark.asyncio
async def test_postgres_event_store_replay_and_live_polling_are_cursor_safe() -> None:
    run_id = uuid.uuid4()
    first = _stored_event(run_id, 1)
    second = _stored_event(run_id, 2)
    store = PostgresEventStore(_sessions(), sleep=lambda _seconds: _immediate())
    replay_pages = deque(
        [
            EventPage(events=(first,), next_after=1, has_more=True),
            EventPage(events=(second,), next_after=2, has_more=False),
        ]
    )
    replay_cursors: list[int] = []

    async def replay_page(
        _self: PostgresEventStore,
        _tenant_id: uuid.UUID,
        _run_id: uuid.UUID,
        *,
        after: int = 0,
        limit: int = 100,
    ) -> EventPage:
        replay_cursors.append(after)
        assert limit == 1
        return replay_pages.popleft()

    cast("Any", store).read_page = MethodType(replay_page, store)
    assert [
        event.sequence
        async for event in store.iter_after(
            TENANT_ID,
            run_id,
            page_size=1,
        )
    ] == [1, 2]
    assert replay_cursors == [0, 1]

    live_pages = deque(
        [
            EventPage(events=(), next_after=2, has_more=False),
            EventPage(events=(_stored_event(run_id, 3),), next_after=3, has_more=False),
        ]
    )
    live_cursors: list[int] = []
    sleep_calls: list[float] = []

    async def live_page(
        _self: PostgresEventStore,
        _tenant_id: uuid.UUID,
        _run_id: uuid.UUID,
        *,
        after: int = 0,
        limit: int = 100,
    ) -> EventPage:
        live_cursors.append(after)
        assert limit == 1
        return live_pages.popleft()

    async def record_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    cast("Any", store).read_page = MethodType(live_page, store)
    cast("Any", store)._sleep = record_sleep
    stream = store.stream(TENANT_ID, run_id, after=2, page_size=1)
    assert (await anext(stream)).sequence == 3
    await cast("Any", stream).aclose()
    assert live_cursors == [2, 2]
    assert sleep_calls == [0.25]


async def _immediate() -> None:
    return None


@pytest.mark.asyncio
async def test_postgres_session_repository_maps_tenant_scoped_records() -> None:
    session = _session()
    row = SimpleNamespace(**session.model_dump(mode="python"))
    create_database = _FakeDatabase()
    repository = PostgresSessionRepository(
        _sessions(
            create_database,
            _FakeDatabase(scalars=[row]),
            _FakeDatabase(scalars=[session.id]),
            _FakeDatabase(scalars=[None]),
        )
    )

    assert await repository.create(session) == session
    assert len(create_database.added) == 1
    assert await repository.get(TENANT_ID, session.id) == session
    assert await repository.exists(
        cast("Any", _FakeDatabase(scalars=[session.id])),
        TENANT_ID,
        session.id,
    )
    assert not await repository.exists(
        cast("Any", _FakeDatabase(scalars=[None])),
        TENANT_ID,
        session.id,
    )


@pytest.mark.asyncio
async def test_postgres_run_repository_creation_transitions_cancel_and_rewind() -> None:
    run = _run()
    creation_hash = run_creation_hash(priority=0)
    replay_row = _run_row(run, creation_hash=creation_hash)
    conflict_row = _run_row(run, creation_hash="b" * 64)
    transition_row = _run_row(run)
    cancel_row = _run_row(_run(run_id=uuid.uuid4()))
    active_row = _run_row(_run(status=RunStatus.LEASED, run_id=uuid.uuid4()))
    completed_row = _run_row(_run(status=RunStatus.COMPLETED, run_id=uuid.uuid4()))
    rewind_row = _run_row(_run(run_id=uuid.uuid4()))
    checkpoint_id = uuid.uuid4()
    quota_row = SimpleNamespace(
        tenant_id=TENANT_ID,
        max_active_runs=4,
        max_queued_runs=100,
        max_gateway_requests=4,
        memory_enabled=True,
        created_at=NOW,
        updated_at=NOW,
    )
    admission_row = SimpleNamespace(
        id=1,
        global_queue_limit=11,
        retry_after_seconds=Decimal("9.000"),
        created_at=NOW,
        updated_at=NOW,
    )
    repository = PostgresRunRepository(
        _sessions(
            _FakeDatabase(scalars=[None, quota_row, None, admission_row, 0, 0, run.id]),
            _FakeDatabase(scalars=[replay_row]),
            _FakeDatabase(scalars=[conflict_row]),
            _FakeDatabase(scalars=[None, quota_row, None, admission_row, 0, 0, None, None]),
            _FakeDatabase(scalars=[transition_row]),
            _FakeDatabase(scalars=[None]),
            _FakeDatabase(scalars=[cancel_row]),
            _FakeDatabase(scalars=[active_row]),
            _FakeDatabase(scalars=[completed_row]),
            _FakeDatabase(scalars=[checkpoint_id, rewind_row]),
            _FakeDatabase(scalars=[None]),
            _FakeDatabase(scalars=[checkpoint_id, None]),
        )
    )

    created = await repository.create_idempotent(
        TENANT_ID,
        run,
        idempotency_key="request-1",
        creation_hash=creation_hash,
    )
    assert created.created is True and created.run == run
    assert admission_row.global_queue_limit == 10_000
    assert admission_row.retry_after_seconds == Decimal("1.0")
    replay = await repository.create_idempotent(
        TENANT_ID,
        run,
        idempotency_key="request-1",
        creation_hash=creation_hash,
    )
    assert replay.created is False and replay.run == run
    with pytest.raises(DomainOperationError) as conflict:
        await repository.create_idempotent(
            TENANT_ID,
            run,
            idempotency_key="request-1",
            creation_hash=creation_hash,
        )
    assert conflict.value.code == "run_idempotency_conflict"
    with pytest.raises(DomainOperationError) as lost:
        await repository.create_idempotent(
            TENANT_ID,
            run,
            idempotency_key="request-1",
            creation_hash=creation_hash,
        )
    assert lost.value.code == "persistence_conflict"

    assert await repository.transition(
        TENANT_ID,
        run.id,
        RunStatus.QUEUED,
        RunStatus.LEASED,
        occurred_at=NOW + timedelta(seconds=1),
        worker_id="worker-1",
        lease_expires_at=NOW + timedelta(minutes=5),
    )
    assert transition_row.status == RunStatus.LEASED.value
    assert not await repository.transition(
        TENANT_ID,
        run.id,
        RunStatus.QUEUED,
        RunStatus.LEASED,
        occurred_at=NOW + timedelta(seconds=1),
        worker_id="worker-1",
        lease_expires_at=NOW + timedelta(minutes=5),
    )

    cancelled = await repository.request_cancel(
        TENANT_ID,
        cancel_row.id,
        occurred_at=NOW + timedelta(seconds=1),
    )
    assert cancelled is not None and cancelled.status is RunStatus.CANCELLED
    requested = await repository.request_cancel(
        TENANT_ID,
        active_row.id,
        occurred_at=NOW + timedelta(seconds=1),
    )
    assert requested is not None and requested.cancellation_requested is True
    terminal = await repository.request_cancel(
        TENANT_ID,
        completed_row.id,
        occurred_at=NOW + timedelta(seconds=3),
    )
    assert terminal is not None and terminal.status is RunStatus.COMPLETED

    rewound = await repository.rewind(TENANT_ID, rewind_row.id, checkpoint_id)
    assert rewound is not None and rewound.last_checkpoint_id == checkpoint_id
    assert await repository.rewind(TENANT_ID, rewind_row.id, checkpoint_id) is None
    assert await repository.rewind(TENANT_ID, rewind_row.id, checkpoint_id) is None


@pytest.mark.asyncio
async def test_postgres_run_repository_rejects_new_work_at_tenant_queue_quota() -> None:
    run = _run()
    quota_row = SimpleNamespace(
        tenant_id=TENANT_ID,
        max_active_runs=1,
        max_queued_runs=1,
        max_gateway_requests=1,
        memory_enabled=True,
        created_at=NOW,
        updated_at=NOW,
    )
    admission_row = SimpleNamespace(
        id=1,
        global_queue_limit=10_000,
        retry_after_seconds=Decimal("1.000"),
        created_at=NOW,
        updated_at=NOW,
    )
    repository = PostgresRunRepository(
        _sessions(_FakeDatabase(scalars=[None, quota_row, None, admission_row, 1]))
    )

    with pytest.raises(DomainOperationError) as rejected:
        await repository.create_idempotent(
            TENANT_ID,
            run,
            idempotency_key="queue-full-1",
            creation_hash=run_creation_hash(priority=run.priority),
        )
    assert rejected.value.code == "tenant_queue_quota_exceeded"
    assert rejected.value.retryable is True
    assert rejected.value.details["scope"] == "tenant_queued_runs"


@pytest.mark.asyncio
async def test_postgres_run_repository_rejects_global_overload_after_tenant_admission() -> None:
    run = _run()
    quota_row = SimpleNamespace(
        tenant_id=TENANT_ID,
        max_active_runs=1,
        max_queued_runs=10,
        max_gateway_requests=1,
        memory_enabled=True,
        created_at=NOW,
        updated_at=NOW,
    )
    admission_row = SimpleNamespace(
        id=1,
        global_queue_limit=1,
        retry_after_seconds=Decimal("2.250"),
        created_at=NOW,
        updated_at=NOW,
    )
    repository = PostgresRunRepository(
        _sessions(_FakeDatabase(scalars=[None, quota_row, None, admission_row, 0, 1])),
        admission_policy=QueueAdmissionPolicy(
            global_queue_limit=1,
            retry_after_seconds=2.25,
        ),
    )

    with pytest.raises(DomainOperationError) as rejected:
        await repository.create_idempotent(
            TENANT_ID,
            run,
            idempotency_key="globally-full-1",
            creation_hash=run_creation_hash(priority=run.priority),
        )
    assert rejected.value.code == "queue_overloaded"
    assert rejected.value.details["retry_after_seconds"] == 2.25


@pytest.mark.asyncio
async def test_queue_admission_allows_historical_work_but_rejects_stale_reconfiguration() -> None:
    run = _run()
    quota_row = SimpleNamespace(
        tenant_id=TENANT_ID,
        max_active_runs=4,
        max_queued_runs=100,
        max_gateway_requests=4,
        memory_enabled=True,
        created_at=NOW,
        updated_at=NOW,
    )
    current = SimpleNamespace(
        id=1,
        global_queue_limit=10_000,
        retry_after_seconds=Decimal("1.000"),
        created_at=NOW,
        updated_at=NOW + timedelta(seconds=1),
    )
    repository = PostgresRunRepository(
        _sessions(_FakeDatabase(scalars=[None, quota_row, None, current, 0, 0, run.id]))
    )
    created = await repository.create_idempotent(
        TENANT_ID,
        run,
        idempotency_key="historical-run",
        creation_hash=run_creation_hash(priority=run.priority),
    )
    assert created.created is True

    stale = SimpleNamespace(**vars(current))
    stale.global_queue_limit = 99
    with pytest.raises(DomainOperationError) as rejected:
        await PostgresRunRepository(
            _sessions(_FakeDatabase(scalars=[None, quota_row, None, stale])),
        ).create_idempotent(
            TENANT_ID,
            _run(run_id=uuid.uuid4()),
            idempotency_key="stale-config",
            creation_hash=run_creation_hash(priority=run.priority),
        )
    assert rejected.value.code == "queue_admission_clock_invalid"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("idempotency_key", "creation_hash"),
    [("bad key", run_creation_hash(priority=0)), ("request-1", "b" * 64)],
)
async def test_postgres_run_repository_validates_creation_identity(
    idempotency_key: str,
    creation_hash: str,
) -> None:
    repository = PostgresRunRepository(_sessions())
    with pytest.raises(DomainOperationError) as error:
        await repository.create_idempotent(
            TENANT_ID,
            _run(),
            idempotency_key=cast("Any", idempotency_key),
            creation_hash=creation_hash,
        )
    assert error.value.code == "invalid_run_creation"


@pytest.mark.asyncio
async def test_postgres_approval_repository_is_idempotent_and_resumes_run() -> None:
    run_id = uuid.uuid4()
    approval_id = uuid.uuid4()
    pending = SimpleNamespace(
        id=approval_id,
        run_id=run_id,
        status=ApprovalStatus.PENDING.value,
        reason="sensitive",
        arguments={},
        decided_by=None,
        requested_at=NOW,
        decided_at=None,
    )
    waiting = _run_row(_run(status=RunStatus.WAITING_APPROVAL, run_id=run_id))
    already_approved = SimpleNamespace(
        **{
            **pending.__dict__,
            "status": ApprovalStatus.APPROVED.value,
            "decided_by": "operator",
            "decided_at": NOW + timedelta(seconds=2),
        }
    )
    already_rejected = SimpleNamespace(
        **{
            **pending.__dict__,
            "status": ApprovalStatus.REJECTED.value,
            "decided_by": "operator",
            "decided_at": NOW + timedelta(seconds=2),
        }
    )
    repository = PostgresApprovalRepository(
        _sessions(
            _FakeDatabase(scalars=[None]),
            _FakeDatabase(scalars=[pending, waiting]),
            _FakeDatabase(scalars=[already_approved]),
            _FakeDatabase(scalars=[already_rejected]),
            _FakeDatabase(scalars=[SimpleNamespace(**pending.__dict__), None]),
        )
    )
    decision = ApprovalDecision(
        approved=True,
        decided_by="operator",
        decided_at=NOW + timedelta(seconds=2),
    )

    assert await repository.decide(TENANT_ID, run_id, approval_id, decision) is None
    approved = await repository.decide(TENANT_ID, run_id, approval_id, decision)
    assert approved is not None and approved.status is ApprovalStatus.APPROVED
    assert waiting.status == RunStatus.QUEUED.value
    assert await repository.decide(TENANT_ID, run_id, approval_id, decision) is not None
    with pytest.raises(DomainOperationError) as conflict:
        await repository.decide(TENANT_ID, run_id, approval_id, decision)
    assert conflict.value.code == "approval_decision_conflict"
    with pytest.raises(DomainOperationError) as lost:
        await repository.decide(TENANT_ID, run_id, approval_id, decision)
    assert lost.value.code == "persistence_conflict"


@pytest.mark.asyncio
async def test_postgres_execution_repository_serializes_entities_and_detects_conflicts() -> None:
    run = _run()
    message = PersistedMessage(
        id=uuid.uuid4(),
        session_id=run.session_id,
        run_id=run.id,
        sequence=1,
        role="user",
        content="hello",
        metadata={"source": "test"},
        created_at=NOW,
    )
    plan = PersistedTaskPlan(
        id=uuid.uuid4(),
        run_id=run.id,
        version=1,
        plan={"steps": []},
        created_at=NOW,
    )
    approval = PersistedApproval(
        id=uuid.uuid4(),
        run_id=run.id,
        status=ApprovalStatus.PENDING,
        reason="sensitive",
        requested_at=NOW,
    )
    checkpoint = Checkpoint(
        id=uuid.uuid4(),
        run_id=run.id,
        session_id=run.session_id,
        message_sequence=1,
        workspace_snapshot_uri="object://snapshot",
        workspace_revision="revision-1",
        task_plan=FrozenJsonObject({"steps": []}),
        created_at=NOW,
    )
    arguments = FrozenJsonObject({"path": "README.md"})
    tool_call = ToolCall(
        id="tool-1",
        run_id=run.id,
        turn_number=1,
        tool_name="read_file",
        arguments=arguments,
        argument_hash=canonical_argument_hash(arguments),
        status=ToolCallStatus.RECEIVED,
    )
    existing_tool = SimpleNamespace(
        tool_call_id=tool_call.id,
        run_id=tool_call.run_id,
        argument_hash=tool_call.argument_hash,
        tool_name=tool_call.tool_name,
        turn_number=tool_call.turn_number,
        arguments=tool_call.arguments.to_json_object(),
        status=tool_call.status.value,
        workspace_version=None,
        result=None,
        error=None,
        started_at=None,
        completed_at=None,
    )
    conflicting_tool = SimpleNamespace(
        **{
            **existing_tool.__dict__,
            "argument_hash": "b" * 64,
        }
    )
    model_call = ModelCall(
        id="model-1",
        run_id=run.id,
        request_id="request-1",
        route_name="primary",
        status=ModelCallStatus.STARTED,
        retry_count=0,
        fallback_count=0,
        started_at=NOW,
    )
    existing_model = SimpleNamespace(
        request_id=model_call.request_id,
        route_name=model_call.route_name,
        started_at=model_call.started_at,
        provider=None,
        model=None,
        status=model_call.status.value,
        input_tokens=None,
        output_tokens=None,
        cached_tokens=None,
        estimated_cost_usd=None,
        retry_count=0,
        fallback_count=0,
        first_token_at=None,
        completed_at=None,
    )
    conflicting_model = SimpleNamespace(
        **{
            **existing_model.__dict__,
            "request_id": "other-request",
        }
    )
    databases = [
        _FakeDatabase(),
        _FakeDatabase(),
        _FakeDatabase(scalars=[None]),
        _FakeDatabase(scalars=[existing_tool]),
        _FakeDatabase(scalars=[conflicting_tool]),
        _FakeDatabase(),
        _FakeDatabase(),
        _FakeDatabase(scalars=[None]),
        _FakeDatabase(scalars=[existing_model]),
        _FakeDatabase(scalars=[conflicting_model]),
    ]
    repository = PostgresExecutionRepository(_sessions(*databases))

    assert await repository.append_message(TENANT_ID, message) == message
    assert await repository.save_task_plan(TENANT_ID, plan) == plan
    assert await repository.save_tool_call(TENANT_ID, tool_call) == tool_call
    assert await repository.save_tool_call(TENANT_ID, tool_call) == tool_call
    with pytest.raises(DomainOperationError) as tool_conflict:
        await repository.save_tool_call(TENANT_ID, tool_call)
    assert tool_conflict.value.code == "tool_call_id_conflict"
    assert await repository.create_approval(TENANT_ID, approval) == approval
    assert await repository.create_checkpoint(TENANT_ID, checkpoint) == checkpoint
    assert await repository.save_model_call(TENANT_ID, model_call) == model_call
    assert await repository.save_model_call(TENANT_ID, model_call) == model_call
    with pytest.raises(DomainOperationError) as model_conflict:
        await repository.save_model_call(TENANT_ID, model_call)
    assert model_conflict.value.code == "model_call_id_conflict"

    assert len(databases[0].added) == 1
    assert len(databases[1].added) == 1
    assert len(databases[2].added) == 1
    assert len(databases[5].added) == 1
    assert len(databases[6].added) == 1
    assert len(databases[7].added) == 1


@pytest.mark.asyncio
async def test_tool_call_replay_returns_later_durable_state_and_rejects_divergence() -> None:
    run = _run()
    arguments = FrozenJsonObject({"path": "README.md"})
    received = ToolCall(
        id="tool-replay-1",
        run_id=run.id,
        turn_number=1,
        tool_name="read_file",
        arguments=arguments,
        argument_hash=canonical_argument_hash(arguments),
        status=ToolCallStatus.RECEIVED,
    )
    completed = received.model_copy(
        update={
            "status": ToolCallStatus.COMPLETED,
            "result": FrozenJsonObject({"content": "durable"}),
            "started_at": NOW,
            "completed_at": NOW + timedelta(seconds=1),
        }
    )
    durable_row = SimpleNamespace(
        tool_call_id=completed.id,
        run_id=completed.run_id,
        turn_number=completed.turn_number,
        tool_name=completed.tool_name,
        arguments=completed.arguments.to_json_object(),
        argument_hash=completed.argument_hash,
        status=completed.status.value,
        workspace_version=None,
        result=completed.result.to_json_object() if completed.result is not None else None,
        error=None,
        started_at=completed.started_at,
        completed_at=completed.completed_at,
    )
    repository = PostgresExecutionRepository(
        _sessions(
            _FakeDatabase(scalars=[durable_row]),
            _FakeDatabase(scalars=[durable_row]),
            _FakeDatabase(scalars=[durable_row]),
            _FakeDatabase(scalars=[durable_row]),
        )
    )

    assert (await repository.save_tool_call(TENANT_ID, received)).status is (
        ToolCallStatus.COMPLETED
    )
    running_replay = received.model_copy(
        update={"status": ToolCallStatus.RUNNING, "started_at": NOW}
    )
    assert (await repository.save_tool_call(TENANT_ID, running_replay)).status is (
        ToolCallStatus.COMPLETED
    )
    timestamp_replay = completed.model_copy(
        update={
            "started_at": NOW + timedelta(milliseconds=1),
            "completed_at": NOW + timedelta(seconds=2),
        }
    )
    assert await repository.save_tool_call(TENANT_ID, timestamp_replay) == completed
    conflicting = completed.model_copy(
        update={"result": FrozenJsonObject({"content": "different"})}
    )
    with pytest.raises(DomainOperationError) as error:
        await repository.save_tool_call(TENANT_ID, conflicting)
    assert error.value.code == "tool_call_state_conflict"
