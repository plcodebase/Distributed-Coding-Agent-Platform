"""Production composition for the durable API control plane."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Literal, Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from agent_api.app import create_app
from agent_api.auth import OidcAuthenticator, Principal, StaticTokenAuthenticator
from agent_api.dependencies import ApiServices
from agent_core.capacity import TenantQuota
from agent_core.scheduling import QueueAdmissionPolicy
from artifact_store import S3ObjectStoreSettings, create_s3_object_store
from event_store import PostgresEventStore
from platform_persistence import (
    Database,
    DatabaseSettings,
    PostgresApprovalRepository,
    PostgresAuditSink,
    PostgresContextRepository,
    PostgresMemoryRepository,
    PostgresRunRepository,
    PostgresSessionRepository,
    PostgresTaskRepository,
    PostgresWorkspaceRepository,
)
from platform_telemetry import PlatformTelemetry, TelemetrySettings
from queue_wakeup import RedisRunWakeup, RedisWakeupSettings

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

    auth_mode: Literal["static", "oidc"] = "static"
    api_credentials_json: SecretStr | None = Field(
        default=None, description="JSON map from bearer tokens to tenant_id and subject"
    )
    oidc_issuer: str | None = Field(default=None, max_length=2048)
    oidc_audience: str | None = Field(default=None, max_length=1024)
    oidc_jwks_url: str | None = Field(default=None, max_length=2048)
    oidc_tenant_claim: str = Field(
        default="tenant_id",
        pattern=r"^[A-Za-z_][A-Za-z0-9_.-]{0,127}$",
    )
    oidc_jwks_cache_seconds: float = Field(default=300, ge=0, le=3600)
    oidc_clock_skew_seconds: float = Field(default=30, ge=0, le=300)
    environment: Literal["development", "test", "production"] = "development"
    tenant_active_run_limit: int = Field(default=4, ge=1, le=10_000)
    tenant_queued_run_limit: int = Field(default=100, ge=1, le=100_000)
    tenant_gateway_request_limit: int = Field(default=4, ge=1, le=10_000)
    global_queue_limit: int = Field(default=10_000, ge=1, le=1_000_000)
    overload_retry_after_seconds: float = Field(default=1, gt=0, le=3600)
    telemetry_environment: str = Field(default="development", min_length=1, max_length=128)
    otlp_http_endpoint: str | None = Field(default=None, max_length=2_048)
    metrics_token: SecretStr | None = None
    max_source_snapshot_bytes: int = Field(
        default=256 * 1024 * 1024,
        ge=1,
        le=1024 * 1024 * 1024,
    )
    snapshot_upload_ttl_seconds: int = Field(default=900, ge=1, le=3600)
    artifact_download_ttl_seconds: int = Field(default=300, ge=1, le=3600)

    @model_validator(mode="after")
    def validate_authentication(self) -> Self:
        oidc = (self.oidc_issuer, self.oidc_audience, self.oidc_jwks_url)
        if self.auth_mode == "static":
            if self.api_credentials_json is None:
                raise ValueError("static authentication requires api_credentials_json")
            if any(value is not None for value in oidc):
                raise ValueError("static authentication may not include OIDC settings")
            if self.environment == "production":
                raise ValueError("production API authentication requires OIDC")
        else:
            if self.api_credentials_json is not None or any(value is None for value in oidc):
                raise ValueError("OIDC authentication requires only issuer, audience, and JWKS URL")
            issuer = self.oidc_issuer or ""
            jwks_url = self.oidc_jwks_url or ""
            if self.environment == "production" and not (
                issuer.startswith("https://") and jwks_url.startswith("https://")
            ):
                raise ValueError("production OIDC endpoints must use HTTPS")
        return self


def create_production_app(
    *,
    api_settings: AgentApiSettings | None = None,
    database_settings: DatabaseSettings | None = None,
    object_store_settings: S3ObjectStoreSettings | None = None,
) -> FastAPI:
    """Create an independently owned API and PostgreSQL dependency graph."""

    resolved_api = api_settings or AgentApiSettings()
    authenticator = _authenticator(resolved_api)
    database = Database(database_settings or DatabaseSettings())
    object_store = create_s3_object_store(
        object_store_settings or S3ObjectStoreSettings(environment=resolved_api.environment)
    )
    wakeup = RedisRunWakeup.create(RedisWakeupSettings())
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
        wakeup=wakeup.publish,
    )
    approvals = PostgresApprovalRepository(database.sessions, wakeup=wakeup.publish)
    context = PostgresContextRepository(database.sessions)
    tasks = PostgresTaskRepository(database.sessions)
    memories = PostgresMemoryRepository(database.sessions)
    workspaces = PostgresWorkspaceRepository(database.sessions)
    events = PostgresEventStore(database.sessions)
    telemetry = PlatformTelemetry(
        TelemetrySettings(
            service_name="agent-api",
            environment=resolved_api.telemetry_environment,
            otlp_http_endpoint=resolved_api.otlp_http_endpoint,
        )
    )
    services = ApiServices(
        authenticator=authenticator,
        sessions=sessions,
        runs=runs,
        approvals=approvals,
        events=events,
        readiness=_CompositeReadiness(database, object_store, wakeup),
        context=context,
        tasks=tasks,
        memories=memories,
        workspaces=workspaces,
        object_store=object_store,
        audit=PostgresAuditSink(database.sessions),
    )

    async def close() -> None:
        try:
            if isinstance(authenticator, OidcAuthenticator):
                await authenticator.aclose()
        finally:
            try:
                await wakeup.aclose()
            finally:
                try:
                    await object_store.aclose()
                finally:
                    try:
                        await database.aclose()
                    finally:
                        telemetry.shutdown()

    return create_app(
        services,
        close=close,
        telemetry=telemetry,
        metrics_token=(
            resolved_api.metrics_token.get_secret_value()
            if resolved_api.metrics_token is not None
            else None
        ),
        max_snapshot_upload_bytes=resolved_api.max_source_snapshot_bytes,
        snapshot_upload_ttl_seconds=resolved_api.snapshot_upload_ttl_seconds,
        artifact_download_ttl_seconds=resolved_api.artifact_download_ttl_seconds,
    )


class _CompositeReadiness:
    """Require both durable metadata and immutable object storage."""

    def __init__(self, *dependencies: Any) -> None:
        self._dependencies = dependencies

    async def ready(self) -> bool:
        for dependency in self._dependencies:
            try:
                if not await dependency.ready():
                    return False
            except Exception:
                return False
        return True


def _credentials(settings: AgentApiSettings) -> dict[str, Principal]:
    if settings.api_credentials_json is None:
        raise ValueError("static credentials are not configured")
    return _credentials_json(settings.api_credentials_json.get_secret_value())


def _authenticator(
    settings: AgentApiSettings,
) -> StaticTokenAuthenticator | OidcAuthenticator:
    if settings.auth_mode == "static":
        return StaticTokenAuthenticator(_credentials(settings))
    if (
        settings.oidc_issuer is None
        or settings.oidc_audience is None
        or settings.oidc_jwks_url is None
    ):
        raise ValueError("OIDC settings are incomplete")
    return OidcAuthenticator(
        issuer=settings.oidc_issuer,
        audience=settings.oidc_audience,
        jwks_url=settings.oidc_jwks_url,
        tenant_claim=settings.oidc_tenant_claim,
        cache_seconds=settings.oidc_jwks_cache_seconds,
        clock_skew_seconds=settings.oidc_clock_skew_seconds,
    )


def _credentials_json(raw: str) -> dict[str, Principal]:
    """Parse one bounded credential map without consulting ambient settings."""

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
