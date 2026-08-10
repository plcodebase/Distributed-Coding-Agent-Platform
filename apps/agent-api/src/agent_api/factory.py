"""Production composition for the durable API control plane."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from agent_api.app import create_app
from agent_api.auth import Principal, StaticTokenAuthenticator
from agent_api.dependencies import ApiServices
from agent_core.capacity import TenantQuota
from agent_core.scheduling import QueueAdmissionPolicy
from event_store import PostgresEventStore
from platform_persistence import (
    Database,
    DatabaseSettings,
    PostgresApprovalRepository,
    PostgresContextRepository,
    PostgresRunRepository,
    PostgresSessionRepository,
)

if TYPE_CHECKING:
    from fastapi import FastAPI

MIN_CREDENTIALS_JSON_BYTES = 2
MAX_CREDENTIALS_JSON_BYTES = 64 * 1024


class _DuplicateJsonKeyError(ValueError):
    """Internal marker for ambiguous credential configuration."""


class AgentApiSettings(BaseSettings):
    """Closed API authentication configuration."""

    model_config = SettingsConfigDict(
        env_prefix="AGENT_PLATFORM_",
        extra="ignore",
        frozen=True,
    )

    api_credentials_json: SecretStr = Field(
        description="JSON map from bearer tokens to tenant_id and subject"
    )
    tenant_active_run_limit: int = Field(default=4, ge=1, le=10_000)
    tenant_queued_run_limit: int = Field(default=100, ge=1, le=100_000)
    tenant_gateway_request_limit: int = Field(default=4, ge=1, le=10_000)
    global_queue_limit: int = Field(default=10_000, ge=1, le=1_000_000)
    overload_retry_after_seconds: float = Field(default=1, gt=0, le=3600)


def create_production_app(
    *,
    api_settings: AgentApiSettings | None = None,
    database_settings: DatabaseSettings | None = None,
) -> FastAPI:
    """Create an independently owned API and PostgreSQL dependency graph."""

    resolved_api = api_settings or AgentApiSettings()
    authenticator = StaticTokenAuthenticator(_credentials(resolved_api))
    database = Database(database_settings or DatabaseSettings())
    default_quota = TenantQuota(
        max_active_runs=resolved_api.tenant_active_run_limit,
        max_queued_runs=resolved_api.tenant_queued_run_limit,
        max_gateway_requests=resolved_api.tenant_gateway_request_limit,
    )
    sessions = PostgresSessionRepository(database.sessions)
    runs = PostgresRunRepository(
        database.sessions,
        default_quota=default_quota,
        admission_policy=QueueAdmissionPolicy(
            global_queue_limit=resolved_api.global_queue_limit,
            retry_after_seconds=resolved_api.overload_retry_after_seconds,
        ),
    )
    approvals = PostgresApprovalRepository(database.sessions)
    context = PostgresContextRepository(database.sessions)
    events = PostgresEventStore(database.sessions)
    services = ApiServices(
        authenticator=authenticator,
        sessions=sessions,
        runs=runs,
        approvals=approvals,
        events=events,
        readiness=database,
        context=context,
    )
    return create_app(services, close=database.aclose)


def _credentials(settings: AgentApiSettings) -> dict[str, Principal]:
    raw = settings.api_credentials_json.get_secret_value()
    if not MIN_CREDENTIALS_JSON_BYTES <= len(raw.encode("utf-8")) <= MAX_CREDENTIALS_JSON_BYTES:
        raise ValueError("api credentials JSON must be between 2 bytes and 64 KiB")
    try:
        value = json.loads(raw, object_pairs_hook=_unique_json_object)
    except _DuplicateJsonKeyError as error:
        raise ValueError("api credentials JSON contains a duplicate object key") from error
    except (TypeError, ValueError) as error:
        raise ValueError("api credentials JSON is invalid") from error
    if not isinstance(value, dict):
        raise TypeError("api credentials JSON must be an object")
    credentials: dict[str, Principal] = {}
    for token, principal_value in value.items():
        if not isinstance(token, str) or not isinstance(principal_value, dict):
            raise TypeError("api credential entries are invalid")
        credentials[token] = Principal.model_validate(_json_object(principal_value))
    return credentials


def _json_object(value: dict[Any, Any]) -> dict[str, object]:
    if any(not isinstance(key, str) for key in value):
        raise ValueError("api credential principal keys must be strings")
    return {str(key): item for key, item in value.items()}


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateJsonKeyError
        value[key] = item
    return value


__all__ = ["AgentApiSettings", "create_production_app"]
