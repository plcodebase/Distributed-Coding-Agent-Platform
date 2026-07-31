from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, cast
from uuid import UUID

import pytest
from pydantic import ValidationError

from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.gateway import (
    GatewayEvent,
    GatewayFinishReason,
    GatewayMessage,
    GatewayRequest,
    GatewayRequestIdentifier,
    GatewayResponseCompleted,
    GatewayTextDelta,
    MessageRole,
)
from gateway_client import (
    GatewayClient,
    GatewayClientConfig,
    GatewayRequestClaim,
    GatewayRequestClaimStatus,
    InMemoryGatewayCircuitBreaker,
    InMemoryGatewayRateLimiter,
    InMemoryGatewayRequestStore,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator

    from agent_core.domain.models import Sha256Hex


TENANT_ID = UUID("00000000-0000-0000-0000-000000000010")


def request(
    request_id: str,
    *,
    content: str = "hello",
    tenant_id: UUID = TENANT_ID,
    route_name: str = "coding-default",
) -> GatewayRequest:
    return GatewayRequest(
        tenant_id=tenant_id,
        session_id=UUID("00000000-0000-0000-0000-000000000020"),
        run_id=UUID("00000000-0000-0000-0000-000000000030"),
        turn_number=1,
        model_call_id=f"call-{request_id}",
        request_id=request_id,
        route_name=route_name,
        messages=(GatewayMessage(role=MessageRole.USER, content=content),),
    )


class AttemptGateway:
    def __init__(self, attempts: list[list[GatewayEvent] | Exception]) -> None:
        self.attempts = attempts
        self.requests: list[GatewayRequest] = []
        self.closed_attempts = 0

    async def stream(self, gateway_request: GatewayRequest) -> AsyncIterator[GatewayEvent]:
        self.requests.append(gateway_request)
        script = self.attempts[len(self.requests) - 1]
        try:
            if isinstance(script, Exception):
                raise script
            for event in script:
                yield event
        finally:
            self.closed_attempts += 1


class BlockingGateway:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.requests = 0

    async def stream(self, gateway_request: GatewayRequest) -> AsyncIterator[GatewayEvent]:
        del gateway_request
        self.requests += 1
        self.started.set()
        await self.release.wait()
        yield GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP)


class PartialFailureGateway:
    def __init__(self) -> None:
        self.requests: list[GatewayRequest] = []
        self.closed_attempts = 0

    async def stream(self, gateway_request: GatewayRequest) -> AsyncIterator[GatewayEvent]:
        self.requests.append(gateway_request)
        try:
            yield GatewayTextDelta(delta="partial")
            raise RuntimeError("opaque")
        finally:
            self.closed_attempts += 1


class MutableClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


class BrokenRateLimiter:
    def __init__(self) -> None:
        self.calls = 0

    async def acquire(self, tenant_id: UUID, route_name: str) -> float | None:
        del tenant_id, route_name
        self.calls += 1
        raise RuntimeError("rate-policy-secret")


class BrokenCircuitBreaker:
    def __init__(self, *, fail_on: str = "allow") -> None:
        self.fail_on = fail_on

    async def allow(self, route_name: str) -> bool:
        del route_name
        if self.fail_on == "allow":
            raise RuntimeError("circuit-policy-secret")
        return True

    async def record_success(self, route_name: str) -> None:
        del route_name
        if self.fail_on == "success":
            raise RuntimeError("circuit-policy-secret")

    async def record_failure(self, route_name: str) -> None:
        del route_name
        if self.fail_on == "failure":
            raise RuntimeError("circuit-policy-secret")


class StrictCompletionStore(InMemoryGatewayRequestStore):
    """Model a durable store that rejects a failed write after completion."""

    def __init__(self) -> None:
        super().__init__()
        self.completed: set[tuple[UUID, GatewayRequestIdentifier]] = set()
        self.fail_calls = 0

    async def complete(
        self,
        tenant_id: UUID,
        request_id: GatewayRequestIdentifier,
        request_hash: Sha256Hex,
        events: tuple[GatewayEvent, ...],
    ) -> None:
        await super().complete(tenant_id, request_id, request_hash, events)
        self.completed.add((tenant_id, request_id))

    async def fail(
        self,
        tenant_id: UUID,
        request_id: GatewayRequestIdentifier,
        request_hash: Sha256Hex,
        error: ErrorDetail,
    ) -> None:
        self.fail_calls += 1
        if (tenant_id, request_id) in self.completed:
            raise RuntimeError("a completed durable claim cannot be failed")
        await super().fail(tenant_id, request_id, request_hash, error)


class BlockingFailureStore(InMemoryGatewayRequestStore):
    """Expose whether cancellation waits for its durable failure transition."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_started = asyncio.Event()
        self.allow_fail = asyncio.Event()

    async def fail(
        self,
        tenant_id: UUID,
        request_id: GatewayRequestIdentifier,
        request_hash: Sha256Hex,
        error: ErrorDetail,
    ) -> None:
        self.fail_started.set()
        await self.allow_fail.wait()
        await super().fail(tenant_id, request_id, request_hash, error)


class BlockingCompletionStore(StrictCompletionStore):
    """Expose whether cancellation can interrupt the durable terminal commit."""

    def __init__(self) -> None:
        super().__init__()
        self.complete_started = asyncio.Event()
        self.allow_complete = asyncio.Event()

    async def complete(
        self,
        tenant_id: UUID,
        request_id: GatewayRequestIdentifier,
        request_hash: Sha256Hex,
        events: tuple[GatewayEvent, ...],
    ) -> None:
        self.complete_started.set()
        await self.allow_complete.wait()
        await super().complete(tenant_id, request_id, request_hash, events)


@pytest.mark.asyncio
async def test_completed_request_is_replayed_without_another_provider_call() -> None:
    events: list[GatewayEvent] = [
        GatewayTextDelta(delta="done"),
        GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP),
    ]
    gateway = AttemptGateway([events])
    client = GatewayClient(gateway)
    logical_request = request("stable-request")

    first = [event async for event in client.stream(logical_request)]
    second = [event async for event in client.stream(logical_request)]

    assert first == events
    assert second == events
    assert gateway.requests == [logical_request]


@pytest.mark.asyncio
async def test_closing_after_durable_terminal_does_not_fail_or_poison_the_client() -> None:
    terminal = GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP)
    gateway = AttemptGateway([[terminal]])
    store = StrictCompletionStore()
    client = GatewayClient(gateway, request_store=store)
    logical_request = request("terminal-consumer-close")
    stream = client.stream(logical_request)

    assert await anext(stream) == terminal
    await cast("AsyncGenerator[GatewayEvent, None]", stream).aclose()
    replayed = [event async for event in client.stream(logical_request)]

    assert replayed == [terminal]
    assert gateway.requests == [logical_request]
    assert store.fail_calls == 0


@pytest.mark.asyncio
async def test_cancellation_waits_for_durable_failure_before_propagating() -> None:
    gateway = BlockingGateway()
    store = BlockingFailureStore()
    client = GatewayClient(gateway, request_store=store)
    logical_request = request("cancelled-request")
    execution = asyncio.create_task(_collect(client.stream(logical_request)))
    await gateway.started.wait()

    execution.cancel()
    await store.fail_started.wait()
    await asyncio.sleep(0)
    assert not execution.done()

    store.allow_fail.set()
    with pytest.raises(asyncio.CancelledError):
        await execution
    with pytest.raises(DomainOperationError) as stored:
        _ = [event async for event in client.stream(logical_request)]
    assert stored.value.code == "gateway_request_failed"
    assert gateway.requests == 1


@pytest.mark.asyncio
async def test_cancellation_waits_for_terminal_commit_without_rewriting_it_failed() -> None:
    terminal = GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP)
    gateway = AttemptGateway([[terminal]])
    store = BlockingCompletionStore()
    client = GatewayClient(gateway, request_store=store)
    logical_request = request("cancelled-terminal-commit")
    execution = asyncio.create_task(_collect(client.stream(logical_request)))
    await store.complete_started.wait()

    execution.cancel()
    await asyncio.sleep(0)
    assert not execution.done()

    store.allow_complete.set()
    with pytest.raises(asyncio.CancelledError):
        await execution

    replayed = [event async for event in client.stream(logical_request)]
    assert replayed == [terminal]
    assert gateway.requests == [logical_request]
    assert store.fail_calls == 0


@pytest.mark.asyncio
async def test_request_id_rejects_conflicting_payload_and_running_duplicate() -> None:
    gateway = BlockingGateway()
    client = GatewayClient(gateway)
    logical_request = request("running-request")
    execution = asyncio.create_task(_collect(client.stream(logical_request)))
    await gateway.started.wait()

    with pytest.raises(DomainOperationError) as running:
        _ = [event async for event in client.stream(logical_request)]
    assert running.value.code == "gateway_request_in_progress"

    with pytest.raises(DomainOperationError) as conflict:
        _ = [
            event
            async for event in client.stream(
                request("running-request", content="different payload")
            )
        ]
    assert conflict.value.code == "gateway_request_conflict"
    assert gateway.requests == 1

    gateway.release.set()
    assert len(await execution) == 1


@pytest.mark.asyncio
async def test_retry_uses_bounded_exponential_backoff_before_any_stream_event() -> None:
    gateway = AttemptGateway(
        [
            RuntimeError("opaque one"),
            RuntimeError("opaque two"),
            [GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP)],
        ]
    )
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    client = GatewayClient(
        gateway,
        config=GatewayClientConfig(
            max_attempts=3,
            retry_base_delay_seconds=0.25,
            retry_max_delay_seconds=1,
            retry_jitter_ratio=0,
        ),
        sleep=sleep,
    )

    result = [event async for event in client.stream(request("retry-request"))]

    assert len(result) == 1
    assert len(gateway.requests) == 3
    assert gateway.closed_attempts == 3
    assert delays == [0.25, 0.5]


@pytest.mark.asyncio
async def test_retry_stops_after_partial_stream_to_avoid_duplicate_deltas() -> None:
    gateway = PartialFailureGateway()
    client = GatewayClient(gateway, config=GatewayClientConfig(max_attempts=3))
    stream = client.stream(request("partial-request"))

    assert await anext(stream) == GatewayTextDelta(delta="partial")
    with pytest.raises(DomainOperationError) as failure:
        await anext(stream)
    assert failure.value.code == "model_gateway_failure"
    assert len(gateway.requests) == 1


@pytest.mark.asyncio
async def test_circuit_opens_and_half_open_probe_is_exclusive() -> None:
    clock = MutableClock()
    circuit = InMemoryGatewayCircuitBreaker(
        failure_threshold=2,
        recovery_seconds=10,
        clock=clock,
    )
    assert await circuit.allow("coding-default") is True
    await circuit.record_failure("coding-default")
    assert await circuit.allow("coding-default") is True
    await circuit.record_failure("coding-default")

    assert await circuit.allow("coding-default") is False
    clock.value = 10
    assert await circuit.allow("coding-default") is True
    assert await circuit.allow("coding-default") is False

    clock.value = 20
    assert await circuit.allow("coding-default") is True
    assert await circuit.allow("coding-default") is False

    await circuit.record_success("coding-default")
    assert await circuit.allow("coding-default") is True


@pytest.mark.asyncio
async def test_rate_limit_is_scoped_by_tenant_and_route() -> None:
    clock = MutableClock()
    limiter = InMemoryGatewayRateLimiter(
        requests_per_window=1,
        window_seconds=10,
        clock=clock,
    )
    other_tenant = UUID("00000000-0000-0000-0000-000000000011")

    assert await limiter.acquire(TENANT_ID, "coding-default") is None
    assert await limiter.acquire(TENANT_ID, "coding-default") == 10
    assert await limiter.acquire(other_tenant, "coding-default") is None
    assert await limiter.acquire(TENANT_ID, "coding-fast") is None

    clock.value = 10
    assert await limiter.acquire(TENANT_ID, "coding-default") is None


@pytest.mark.asyncio
async def test_request_store_atomically_detects_running_completion_and_conflict() -> None:
    store = InMemoryGatewayRequestStore()
    request_hash = "0" * 64
    other_hash = "1" * 64
    first, second = await asyncio.gather(
        store.claim(TENANT_ID, "request-claim", request_hash),
        store.claim(TENANT_ID, "request-claim", request_hash),
    )

    assert {first.status, second.status} == {
        GatewayRequestClaimStatus.EXECUTE,
        GatewayRequestClaimStatus.IN_PROGRESS,
    }
    conflict = await store.claim(TENANT_ID, "request-claim", other_hash)
    assert conflict.status is GatewayRequestClaimStatus.CONFLICT
    other_tenant = await store.claim(
        UUID("00000000-0000-0000-0000-000000000011"),
        "request-claim",
        other_hash,
    )
    assert other_tenant.status is GatewayRequestClaimStatus.EXECUTE
    await store.release(
        UUID("00000000-0000-0000-0000-000000000011"),
        "request-claim",
        other_hash,
    )
    with pytest.raises(ValueError):
        await store.release(
            UUID("00000000-0000-0000-0000-000000000011"),
            "request-claim",
            other_hash,
        )

    terminal = GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP)
    await store.complete(TENANT_ID, "request-claim", request_hash, (terminal,))
    replay = await store.claim(TENANT_ID, "request-claim", request_hash)
    assert replay.status is GatewayRequestClaimStatus.COMPLETED
    assert replay.events == (terminal,)


def test_reliability_configuration_is_closed_and_bounded() -> None:
    with pytest.raises(ValidationError):
        GatewayClientConfig(max_attempts=0)
    with pytest.raises(ValidationError):
        GatewayClientConfig(retry_base_delay_seconds=2, retry_max_delay_seconds=1)
    with pytest.raises(ValidationError):
        GatewayClientConfig(retry_jitter_ratio=1.01)
    with pytest.raises(ValidationError):
        GatewayClientConfig(circuit_failure_threshold=0)
    with pytest.raises(ValidationError):
        GatewayClientConfig(rate_limit_requests=0)
    with pytest.raises(ValueError):
        InMemoryGatewayRequestStore(max_requests=0)
    with pytest.raises(ValueError):
        InMemoryGatewayRequestStore(max_requests=True)
    with pytest.raises(ValueError):
        InMemoryGatewayRateLimiter(
            requests_per_window=1,
            window_seconds=1,
            max_keys=0,
        )
    with pytest.raises(ValueError):
        InMemoryGatewayCircuitBreaker(
            failure_threshold=1,
            recovery_seconds=1,
            max_routes=0,
        )
    with pytest.raises(ValueError):
        InMemoryGatewayRateLimiter(
            requests_per_window=True,
            window_seconds=1,
        )
    with pytest.raises(ValueError):
        InMemoryGatewayCircuitBreaker(
            failure_threshold=True,
            recovery_seconds=1,
        )
    with pytest.raises(ValueError):
        InMemoryGatewayRateLimiter(
            requests_per_window=1,
            window_seconds=1,
            max_keys=True,
        )
    with pytest.raises(ValueError):
        InMemoryGatewayCircuitBreaker(
            failure_threshold=1,
            recovery_seconds=1,
            max_routes=True,
        )


def test_completed_claim_requires_exactly_one_final_terminal_event() -> None:
    terminal = GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP)
    completed = GatewayRequestClaim(
        status=GatewayRequestClaimStatus.COMPLETED,
        request_hash="0" * 64,
        events=(GatewayTextDelta(delta="done"), terminal),
    )
    assert completed.events[-1] == terminal

    with pytest.raises(ValidationError):
        GatewayRequestClaim(
            status=GatewayRequestClaimStatus.COMPLETED,
            request_hash="0" * 64,
            events=(GatewayTextDelta(delta="incomplete"),),
        )
    with pytest.raises(ValidationError):
        GatewayRequestClaim(
            status=GatewayRequestClaimStatus.COMPLETED,
            request_hash="0" * 64,
            events=(terminal, terminal),
        )


@pytest.mark.asyncio
async def test_policy_outages_are_opaque_structured_and_release_precontact_claims() -> None:
    limiter = BrokenRateLimiter()
    gateway = AttemptGateway([[GatewayResponseCompleted(finish_reason=GatewayFinishReason.STOP)]])
    client = GatewayClient(gateway, rate_limiter=limiter)
    logical_request = request("rate-policy-outage")

    for _ in range(2):
        with pytest.raises(DomainOperationError) as unavailable:
            _ = [event async for event in client.stream(logical_request)]
        assert unavailable.value.code == "gateway_rate_limit_unavailable"
        assert "rate-policy-secret" not in repr(unavailable.value.as_dict())
    assert limiter.calls == 2
    assert gateway.requests == []

    circuit_client = GatewayClient(
        gateway,
        circuit_breaker=BrokenCircuitBreaker(),
    )
    with pytest.raises(DomainOperationError) as circuit:
        _ = [event async for event in circuit_client.stream(request("circuit-outage"))]
    assert circuit.value.code == "gateway_circuit_unavailable"
    assert "circuit-policy-secret" not in repr(circuit.value.as_dict())


@pytest.mark.asyncio
async def test_retry_jitter_source_must_return_a_finite_unit_value() -> None:
    client = GatewayClient(
        AttemptGateway([RuntimeError("opaque")]),
        config=GatewayClientConfig(max_attempts=2),
        random_value=lambda: float("nan"),
    )

    with pytest.raises(DomainOperationError) as invalid:
        _ = [event async for event in client.stream(request("invalid-jitter"))]
    assert invalid.value.code == "gateway_retry_policy_invalid"


@pytest.mark.asyncio
async def test_local_policy_state_has_fail_closed_capacity_limits() -> None:
    other_tenant = UUID("00000000-0000-0000-0000-000000000011")
    store = InMemoryGatewayRequestStore(max_requests=1)
    _ = await store.claim(TENANT_ID, "one", "0" * 64)
    with pytest.raises(RuntimeError):
        _ = await store.claim(other_tenant, "two", "1" * 64)

    limiter = InMemoryGatewayRateLimiter(
        requests_per_window=1,
        window_seconds=1,
        max_keys=1,
    )
    assert await limiter.acquire(TENANT_ID, "coding-default") is None
    with pytest.raises(RuntimeError):
        _ = await limiter.acquire(other_tenant, "coding-default")

    circuit = InMemoryGatewayCircuitBreaker(
        failure_threshold=1,
        recovery_seconds=1,
        max_routes=1,
    )
    await circuit.record_failure("coding-default")
    with pytest.raises(RuntimeError):
        await circuit.record_success("coding-fast")


@pytest.mark.asyncio
async def test_local_policy_clocks_must_return_finite_numbers() -> None:
    limiter = InMemoryGatewayRateLimiter(
        requests_per_window=1,
        window_seconds=1,
        clock=lambda: float("nan"),
    )
    with pytest.raises(ValueError, match="finite"):
        await limiter.acquire(TENANT_ID, "coding-default")

    circuit = InMemoryGatewayCircuitBreaker(
        failure_threshold=1,
        recovery_seconds=1,
        clock=lambda: float("inf"),
    )
    with pytest.raises(ValueError, match="finite"):
        await circuit.allow("coding-default")


async def _collect(stream: AsyncIterator[GatewayEvent]) -> list[GatewayEvent]:
    return [event async for event in stream]
