"""OpenAI Agents SDK integration for the provider-neutral agent core."""

from agents_sdk_adapter.factory import create_openai_compatible_agents_gateway
from agents_sdk_adapter.gateway import OpenAIAgentsGateway

__all__ = [
    "OpenAIAgentsGateway",
    "create_openai_compatible_agents_gateway",
]
