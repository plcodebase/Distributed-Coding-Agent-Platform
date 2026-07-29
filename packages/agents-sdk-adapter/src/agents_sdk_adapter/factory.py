"""Production composition for the OpenAI-compatible Agents SDK gateway."""

from __future__ import annotations

from typing import TYPE_CHECKING

from agents.models.openai_provider import OpenAIProvider
from openai import AsyncOpenAI

from agents_sdk_adapter.gateway import OpenAIAgentsGateway

if TYPE_CHECKING:
    from httpx import AsyncClient

    from agent_core.settings import PlatformSettings


def create_openai_compatible_agents_gateway(
    settings: PlatformSettings,
    *,
    http_client: AsyncClient | None = None,
) -> OpenAIAgentsGateway:
    """Create a Chat Completions SDK adapter for the centralized gateway."""

    base_url = _versioned_base_url(settings.gateway_url)
    client = AsyncOpenAI(
        api_key=settings.gateway_api_key.get_secret_value(),
        base_url=base_url,
        http_client=http_client,
        max_retries=0,
    )
    provider = OpenAIProvider(
        openai_client=client,
        use_responses=False,
        strict_feature_validation=True,
        buffer_streamed_tool_calls=True,
    )
    if http_client is None:
        return OpenAIAgentsGateway._with_owned_client(provider, client)
    return OpenAIAgentsGateway(provider)


def _versioned_base_url(gateway_url: str) -> str:
    normalized = gateway_url.rstrip("/")
    return normalized if normalized.endswith("/v1") else f"{normalized}/v1"


__all__ = ["create_openai_compatible_agents_gateway"]
