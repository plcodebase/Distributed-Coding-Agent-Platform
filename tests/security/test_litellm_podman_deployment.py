import os
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path
from uuid import UUID

import pytest
from scripts.verify_local_stack import _check_gateway_route

from agent_core.gateway import (
    GatewayFinishReason,
    GatewayMessage,
    GatewayRequest,
    GatewayResponseCompleted,
    GatewayTextDelta,
    MessageRole,
)
from agent_core.settings import PlatformSettings
from gateway_client import (
    GatewayClientConfig,
    InMemoryGatewayCapacityStore,
    InMemoryGatewayCircuitBreaker,
    InMemoryGatewayRateLimiter,
    InMemoryGatewayRequestStore,
)
from gateway_client.factory import create_gateway_client

ROOT = Path(__file__).parents[2]
COMPOSE = ROOT / ".venv" / "bin" / "podman-compose"
PODMAN = shutil.which("podman") or "/usr/bin/podman"
ENV_FILE = Path(os.getenv("AGENT_PLATFORM_PODMAN_ENV_FILE", ROOT / ".env"))
ROUTES = {
    "coding-default": "fake-primary",
    "coding-fast": "fake-primary",
    "coding-strong": "fake-secondary",
    "summarization": "fake-primary",
    "code-review": "fake-secondary",
}

pytestmark = [
    pytest.mark.security,
    pytest.mark.skipif(
        os.getenv("AGENT_PLATFORM_RUN_PODMAN_GATEWAY") != "1",
        reason="set AGENT_PLATFORM_RUN_PODMAN_GATEWAY=1 for the Podman gateway deployment",
    ),
]


def _env_file_value(name: str) -> str:
    raw = ENV_FILE.read_bytes()
    if len(raw) > 64 * 1024 or b"\x00" in raw:
        raise AssertionError("gateway security-test env file is invalid")
    found: str | None = None
    for line in raw.decode("utf-8").splitlines():
        candidate = line.strip()
        if not candidate or candidate.startswith("#") or "=" not in candidate:
            continue
        key, value = candidate.split("=", 1)
        if key.strip() != name:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        if not value or (found is not None and found != value):
            raise AssertionError(f"{name} must have one nonempty value")
        found = value
    if found is None:
        raise AssertionError(f"{name} is required in the gateway security-test env file")
    return found


def _gateway_key() -> str:
    gateway_key = os.getenv("AGENT_PLATFORM_GATEWAY_API_KEY") or _env_file_value(
        "AGENT_PLATFORM_GATEWAY_API_KEY"
    )
    if gateway_key != _env_file_value("LITELLM_MASTER_KEY"):
        raise AssertionError("the platform gateway key must match the LiteLLM master key")
    return gateway_key


def _compose(*arguments: str) -> None:
    result = subprocess.run(  # noqa: S603 - fixed local executable and test-controlled argv
        (
            os.fspath(COMPOSE),
            "--podman-path",
            PODMAN,
            "--env-file",
            os.fspath(ENV_FILE),
            *arguments,
        ),
        cwd=ROOT,
        check=False,
        capture_output=True,
        timeout=90,
    )
    if result.returncode != 0:
        raise AssertionError("Podman Compose operation failed during gateway verification")


def _remove_test_containers() -> None:
    for names in (
        ("agent-platform_litellm_1",),
        (
            "agent-platform_fake-llm-primary_1",
            "agent-platform_fake-llm-secondary_1",
        ),
    ):
        result = subprocess.run(  # noqa: S603 - exact platform-owned test containers
            (PODMAN, "rm", "--force", "--ignore", *names),
            check=False,
            capture_output=True,
            timeout=30,
        )
        if result.returncode != 0:
            raise AssertionError("Podman could not remove gateway security-test containers")


def _remove_test_network() -> None:
    for network_name in (
        "agent-platform_llm-upstreams",
        "agent-platform_llm-egress",
    ):
        exists = subprocess.run(  # noqa: S603 - exact platform-owned test network
            (PODMAN, "network", "exists", network_name),
            check=False,
            capture_output=True,
            timeout=30,
        )
        if exists.returncode == 1:
            continue
        if exists.returncode != 0:
            raise AssertionError("Podman could not inspect a gateway security-test network")
        removed = subprocess.run(  # noqa: S603 - exact platform-owned test network
            (PODMAN, "network", "rm", network_name),
            check=False,
            capture_output=True,
            timeout=30,
        )
        if removed.returncode != 0:
            raise AssertionError("Podman could not remove a gateway security-test network")


@pytest.fixture(scope="module", autouse=True)
def gateway_stack() -> Iterator[None]:
    assert ENV_FILE.is_file()
    try:
        _compose(
            "up",
            "--detach",
            "--force-recreate",
            "--wait",
            "--wait-timeout",
            "90",
            "fake-llm-primary",
            "fake-llm-secondary",
            "litellm",
        )
        yield
    finally:
        try:
            _compose("stop", "litellm", "fake-llm-primary", "fake-llm-secondary")
        finally:
            try:
                _remove_test_containers()
            finally:
                _remove_test_network()


def test_all_logical_routes_reach_the_expected_deployments() -> None:
    gateway_key = _gateway_key()
    for route, expected_provider in ROUTES.items():
        _check_gateway_route(gateway_key, route, expected_provider)


def test_primary_outage_uses_configured_secondary_fallback() -> None:
    gateway_key = _gateway_key()
    _compose("stop", "fake-llm-primary")
    try:
        _check_gateway_route(
            gateway_key,
            "coding-default",
            "fake-secondary",
            timeout_seconds=15,
        )
    finally:
        _compose(
            "up",
            "--detach",
            "--wait",
            "--wait-timeout",
            "60",
            "fake-llm-primary",
        )


async def test_typed_gateway_client_normalizes_live_litellm_stream() -> None:
    gateway_key = _gateway_key()
    config = GatewayClientConfig()
    client = create_gateway_client(
        PlatformSettings(
            gateway_url="http://127.0.0.1:4000",
            gateway_api_key=gateway_key,
        ),
        config=config,
        request_store=InMemoryGatewayRequestStore(),
        rate_limiter=InMemoryGatewayRateLimiter(
            requests_per_window=config.rate_limit_requests,
            window_seconds=config.rate_limit_window_seconds,
        ),
        capacity_store=InMemoryGatewayCapacityStore(
            tenant_request_limit=config.tenant_concurrent_requests,
            provider_request_limit=config.provider_concurrent_requests,
            provider_token_limit=config.provider_tokens_per_window,
            token_window_seconds=config.provider_token_window_seconds,
        ),
        circuit_breaker=InMemoryGatewayCircuitBreaker(
            failure_threshold=config.circuit_failure_threshold,
            recovery_seconds=config.circuit_recovery_seconds,
        ),
    )
    request = GatewayRequest(
        tenant_id=UUID("00000000-0000-0000-0000-000000000010"),
        session_id=UUID("00000000-0000-0000-0000-000000000020"),
        run_id=UUID("00000000-0000-0000-0000-000000000030"),
        turn_number=1,
        model_call_id="model-call-live-1",
        request_id="request-live-1",
        route_name="coding-default",
        messages=(
            GatewayMessage(
                role=MessageRole.USER,
                content="return the deterministic response",
            ),
        ),
    )

    async with client:
        events = [event async for event in client.stream(request)]

    text = "".join(event.delta for event in events if isinstance(event, GatewayTextDelta))
    assert text in {
        "deterministic response from fake-primary",
        "deterministic response from fake-secondary",
    }
    terminal = events[-1]
    assert isinstance(terminal, GatewayResponseCompleted)
    assert terminal.finish_reason is GatewayFinishReason.STOP
    assert terminal.input_tokens == 1
    assert terminal.output_tokens == 1
    assert terminal.provider is None
