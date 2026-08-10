from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from agent_core.capacity import (
    CapacityScope,
    GatewayCapacityClaim,
    GatewayCapacityLease,
    TenantQuota,
)
from agent_core.domain.errors import DomainOperationError
from agent_core.fakes import ScriptedGatewayTurn, ScriptedModelGateway
from agent_core.gateway import (
    GatewayEvent,
    GatewayFinishReason,
    GatewayMessage,
    GatewayRequest,
    GatewayResponseCompleted,
    MessageRole,
)
from gateway_client import GatewayClient, GatewayClientConfig, InMemoryGatewayCapacityStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

NOW = datetime(2026, 7, 31, 12, tzinfo=UTC)
TENANT_A = uuid.UUID("10000000-0000-0000-0000-000000000001")
TENANT_B = uuid.UUID("10000000-0000-0000-0000-000000000002")


class MutableClock:
    def __init__(self) -> None:
        self.value = NOW

    def __call__(self) -> datetime:
        return self.value


class BlockingGateway:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.closed = asyncio.Event()

    async def stream(self, _request: GatewayRequest) -> AsyncIterator[GatewayEvent]:
        self.started.set()
        try:
            await self.release.wait()
            yield GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP)
        finally:
            self.closed.set()


class TriggeredSleep:
    def __init__(self) -> None:
        self.trigger = asyncio.Event()

    async def __call__(self, _seconds: float) -> None:
        await self.trigger.wait()
        self.trigger.clear()


class TrackingCapacityStore(InMemoryGatewayCapacityStore):
    def __init__(
        self,
        *,
        tenant_request_limit: int,
        provider_request_limit: int,
        provider_token_limit: int,
        token_window_seconds: float,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
    ) -> None:
        super().__init__(
            tenant_request_limit=tenant_request_limit,
            provider_request_limit=provider_request_limit,
            provider_token_limit=provider_token_limit,
            token_window_seconds=token_window_seconds,
            clock=clock,
            id_factory=id_factory,
        )
        self.renewed = asyncio.Event()
        self.renewal_error: Exception | None = None

    async def renew(
        self,
        lease: GatewayCapacityLease,
        *,
        lease_duration: timedelta,
    ) -> GatewayCapacityLease:
        if self.renewal_error is not None:
            raise self.renewal_error
        renewed = await super().renew(lease, lease_duration=lease_duration)
        self.renewed.set()
        return renewed


def _request(*, request_id: str = "request-1") -> GatewayRequest:
    return GatewayRequest(
        tenant_id=TENANT_A,
        session_id=uuid.UUID("20000000-0000-0000-0000-000000000001"),
        run_id=uuid.UUID("30000000-0000-0000-0000-000000000001"),
        turn_number=1,
        model_call_id=f"model-{request_id}",
        request_id=request_id,
        route_name="coding-default",
        messages=(GatewayMessage(role=MessageRole.USER, content="respond"),),
    )


def test_capacity_contracts_are_closed_and_bounded() -> None:
    quota = TenantQuota()
    assert quota.max_active_runs == 4
    assert quota.max_queued_runs == 100
    with pytest.raises(ValidationError):
        TenantQuota(max_active_runs=0)
    with pytest.raises(ValidationError):
        GatewayCapacityClaim()
    with pytest.raises(ValidationError):
        GatewayCapacityClaim(
            lease=GatewayCapacityLease(
                id=uuid.uuid4(),
                tenant_id=TENANT_A,
                route_name="coding-default",
                request_id="request-1",
                reserved_tokens=1,
                acquired_at=NOW,
                expires_at=NOW + timedelta(seconds=1),
            ),
            rejection={"scope": "provider_tokens", "retry_after_seconds": 1},
        )


@pytest.mark.asyncio
async def test_in_memory_capacity_is_atomic_idempotent_and_reconciles_tokens() -> None:
    clock = MutableClock()
    identifiers = iter(
        (
            uuid.UUID("40000000-0000-0000-0000-000000000001"),
            uuid.UUID("40000000-0000-0000-0000-000000000002"),
            uuid.UUID("40000000-0000-0000-0000-000000000003"),
        )
    )
    store = InMemoryGatewayCapacityStore(
        tenant_request_limit=1,
        provider_request_limit=2,
        provider_token_limit=100,
        token_window_seconds=60,
        clock=clock,
        id_factory=lambda: next(identifiers),
    )
    first = await store.acquire(
        TENANT_A,
        "coding-default",
        "request-1",
        reserved_tokens=20,
        lease_duration=timedelta(seconds=30),
    )
    assert first.lease is not None
    replay = await store.acquire(
        TENANT_A,
        "coding-default",
        "request-1",
        reserved_tokens=20,
        lease_duration=timedelta(seconds=30),
    )
    assert replay.lease == first.lease

    tenant_full = await store.acquire(
        TENANT_A,
        "coding-default",
        "request-2",
        reserved_tokens=1,
        lease_duration=timedelta(seconds=30),
    )
    assert tenant_full.rejection is not None
    assert tenant_full.rejection.scope is CapacityScope.TENANT_GATEWAY_REQUESTS

    await store.release(first.lease, consumed_tokens=5)
    second = await store.acquire(
        TENANT_B,
        "coding-default",
        "request-2",
        reserved_tokens=90,
        lease_duration=timedelta(seconds=30),
    )
    assert second.lease is not None
    await store.release(second.lease, consumed_tokens=95)
    token_full = await store.acquire(
        TENANT_A,
        "coding-default",
        "request-3",
        reserved_tokens=1,
        lease_duration=timedelta(seconds=30),
    )
    assert token_full.rejection is not None
    assert token_full.rejection.scope is CapacityScope.PROVIDER_TOKENS

    clock.value += timedelta(seconds=61)
    after_window = await store.acquire(
        TENANT_A,
        "coding-default",
        "request-3",
        reserved_tokens=100,
        lease_duration=timedelta(seconds=30),
    )
    assert after_window.lease is not None


@pytest.mark.asyncio
async def test_capacity_expiry_recovers_request_slots_without_refunding_tokens() -> None:
    clock = MutableClock()
    store = InMemoryGatewayCapacityStore(
        tenant_request_limit=1,
        provider_request_limit=1,
        provider_token_limit=100,
        token_window_seconds=60,
        clock=clock,
    )
    first = await store.acquire(
        TENANT_A,
        "coding-default",
        "request-1",
        reserved_tokens=60,
        lease_duration=timedelta(seconds=5),
    )
    assert first.lease is not None
    clock.value += timedelta(seconds=6)
    recovered = await store.acquire(
        TENANT_B,
        "coding-default",
        "request-2",
        reserved_tokens=40,
        lease_duration=timedelta(seconds=5),
    )
    assert recovered.lease is not None
    await store.release(recovered.lease, consumed_tokens=40)
    denied = await store.acquire(
        TENANT_A,
        "coding-default",
        "request-3",
        reserved_tokens=1,
        lease_duration=timedelta(seconds=5),
    )
    assert denied.rejection is not None
    assert denied.rejection.scope is CapacityScope.PROVIDER_TOKENS


@pytest.mark.asyncio
async def test_capacity_renewal_extends_exact_ownership_and_rejects_stale_release() -> None:
    clock = MutableClock()
    store = InMemoryGatewayCapacityStore(
        tenant_request_limit=1,
        provider_request_limit=1,
        provider_token_limit=100,
        token_window_seconds=60,
        clock=clock,
    )
    acquired = await store.acquire(
        TENANT_A,
        "coding-default",
        "request-1",
        reserved_tokens=10,
        lease_duration=timedelta(seconds=5),
    )
    assert acquired.lease is not None

    clock.value += timedelta(seconds=4)
    renewed = await store.renew(acquired.lease, lease_duration=timedelta(seconds=5))
    assert renewed.expires_at == NOW + timedelta(seconds=9)

    clock.value += timedelta(seconds=2)
    blocked = await store.acquire(
        TENANT_B,
        "coding-default",
        "request-2",
        reserved_tokens=1,
        lease_duration=timedelta(seconds=5),
    )
    assert blocked.rejection is not None
    assert blocked.rejection.scope is CapacityScope.PROVIDER_REQUESTS
    with pytest.raises(ValueError, match="durable ownership"):
        await store.release(acquired.lease, consumed_tokens=0)
    await store.release(renewed, consumed_tokens=0)


@pytest.mark.asyncio
async def test_gateway_client_renews_capacity_while_provider_stream_is_live() -> None:
    clock = MutableClock()
    capacity = TrackingCapacityStore(
        tenant_request_limit=1,
        provider_request_limit=1,
        provider_token_limit=1_000_000,
        token_window_seconds=60,
        clock=clock,
    )
    gateway = BlockingGateway()
    heartbeat = TriggeredSleep()
    client = GatewayClient(
        gateway,
        capacity_store=capacity,
        capacity_sleep=heartbeat,
        config=GatewayClientConfig(
            capacity_lease_seconds=5,
            capacity_heartbeat_seconds=1,
            provider_output_token_reservation=1,
        ),
    )
    collecting = asyncio.create_task(_collect(client, _request()))
    await gateway.started.wait()

    clock.value += timedelta(seconds=4)
    heartbeat.trigger.set()
    await capacity.renewed.wait()
    clock.value += timedelta(seconds=2)
    blocked = await capacity.acquire(
        TENANT_B,
        "coding-default",
        "request-2",
        reserved_tokens=1,
        lease_duration=timedelta(seconds=5),
    )
    assert blocked.rejection is not None
    assert blocked.rejection.scope is CapacityScope.PROVIDER_REQUESTS

    gateway.release.set()
    events = await collecting
    assert isinstance(events[-1], GatewayResponseCompleted)
    assert gateway.closed.is_set()


@pytest.mark.asyncio
async def test_gateway_client_fails_closed_when_capacity_renewal_is_lost() -> None:
    capacity = TrackingCapacityStore(
        tenant_request_limit=1,
        provider_request_limit=1,
        provider_token_limit=1_000_000,
        token_window_seconds=60,
    )
    capacity.renewal_error = RuntimeError("secret-provider-capacity-state")
    gateway = BlockingGateway()
    heartbeat = TriggeredSleep()
    client = GatewayClient(
        gateway,
        capacity_store=capacity,
        capacity_sleep=heartbeat,
        config=GatewayClientConfig(
            capacity_lease_seconds=5,
            capacity_heartbeat_seconds=1,
            provider_output_token_reservation=1,
        ),
    )
    collecting = asyncio.create_task(_collect(client, _request()))
    await gateway.started.wait()
    heartbeat.trigger.set()

    with pytest.raises(DomainOperationError) as lost:
        await collecting
    assert lost.value.code == "gateway_capacity_lease_lost"
    assert lost.value.retryable is True
    assert "secret-provider-capacity-state" not in str(lost.value)
    assert gateway.closed.is_set()


def test_gateway_capacity_heartbeat_must_precede_lease_expiry() -> None:
    with pytest.raises(ValidationError, match="capacity heartbeat"):
        GatewayClientConfig(capacity_lease_seconds=5, capacity_heartbeat_seconds=5)


@pytest.mark.asyncio
async def test_gateway_client_rejects_capacity_before_provider_contact_and_releases_claim() -> None:
    clock = MutableClock()
    capacity = InMemoryGatewayCapacityStore(
        tenant_request_limit=1,
        provider_request_limit=2,
        provider_token_limit=1_000_000,
        token_window_seconds=60,
        clock=clock,
    )
    blocker = await capacity.acquire(
        TENANT_A,
        "coding-default",
        "blocking-request",
        reserved_tokens=1,
        lease_duration=timedelta(seconds=30),
    )
    assert blocker.lease is not None
    gateway = ScriptedModelGateway((ScriptedGatewayTurn.text("done"),))
    client = GatewayClient(
        gateway,
        capacity_store=capacity,
        config=GatewayClientConfig(provider_output_token_reservation=1),
    )
    request = _request()

    with pytest.raises(DomainOperationError) as rejected:
        _ = [event async for event in client.stream(request)]
    assert rejected.value.code == "gateway_capacity_exhausted"
    assert gateway.requests == ()

    await capacity.release(blocker.lease, consumed_tokens=0)
    events = [event async for event in client.stream(request)]
    assert events[-1].kind == "response_completed"
    assert len(gateway.requests) == 1


async def _collect(client: GatewayClient, request: GatewayRequest) -> list[GatewayEvent]:
    return [event async for event in client.stream(request)]
