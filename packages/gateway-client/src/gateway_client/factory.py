"""Production composition for the typed centralized gateway client."""

from __future__ import annotations

from typing import TYPE_CHECKING

from agents_sdk_adapter import create_openai_compatible_agents_gateway
from gateway_client.client import GatewayClient, GatewayClientConfig

if TYPE_CHECKING:
    from httpx import AsyncClient

    from agent_core.capacity import GatewayCapacityStore
    from agent_core.settings import PlatformSettings
    from gateway_client.reliability import (
        GatewayCircuitBreaker,
        GatewayRateLimiter,
        GatewayRequestStore,
    )


def create_gateway_client(
    settings: PlatformSettings,
    *,
    request_store: GatewayRequestStore,
    rate_limiter: GatewayRateLimiter,
    capacity_store: GatewayCapacityStore,
    circuit_breaker: GatewayCircuitBreaker,
    config: GatewayClientConfig | None = None,
    http_client: AsyncClient | None = None,
) -> GatewayClient:
    """Create the sole worker-to-LiteLLM model access path."""

    adapter = create_openai_compatible_agents_gateway(
        settings,
        http_client=http_client,
    )
    return GatewayClient(
        adapter,
        config=config,
        close=adapter.aclose,
        request_store=request_store,
        rate_limiter=rate_limiter,
        capacity_store=capacity_store,
        circuit_breaker=circuit_breaker,
    )


__all__ = ["create_gateway_client"]
