"""Production composition for the least-privilege event gateway."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from agent_api.auth import OidcAuthenticator, StaticTokenAuthenticator
from agent_api.dependencies import EventGatewayServices
from agent_api.event_app import create_event_gateway_app
from agent_api.factory import _credentials_json
from event_store import PostgresEventStore
from platform_persistence import (
    Database,
    DatabaseSettings,
    PostgresLifecycleRepository,
    PostgresRunRepository,
)
from platform_telemetry import PlatformTelemetry, TelemetrySettings

if TYPE_CHECKING:
    from fastapi import FastAPI


class EventGatewaySettings(BaseSettings):
    """Closed event-gateway authentication and telemetry settings."""

    model_config = SettingsConfigDict(
        env_prefix="AGENT_PLATFORM_",
        extra="ignore",
        frozen=True,
    )

    auth_mode: Literal["static", "oidc"] = "static"
    event_credentials_json: SecretStr | None = Field(
        default=None, description="JSON map from event bearer tokens to tenant principals"
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
    telemetry_environment: str = Field(default="development", min_length=1, max_length=128)
    otlp_http_endpoint: str | None = Field(default=None, max_length=2_048)
    metrics_token: SecretStr | None = None

    @model_validator(mode="after")
    def validate_authentication(self) -> Self:
        oidc = (self.oidc_issuer, self.oidc_audience, self.oidc_jwks_url)
        if self.auth_mode == "static":
            if self.event_credentials_json is None:
                raise ValueError("static event authentication requires event credentials")
            if any(value is not None for value in oidc):
                raise ValueError("static event authentication may not include OIDC settings")
            if self.environment == "production":
                raise ValueError("production event authentication requires OIDC")
        else:
            if self.event_credentials_json is not None or any(value is None for value in oidc):
                raise ValueError(
                    "OIDC event authentication requires issuer, audience, and JWKS URL"
                )
            if self.environment == "production" and not (
                (self.oidc_issuer or "").startswith("https://")
                and (self.oidc_jwks_url or "").startswith("https://")
            ):
                raise ValueError("production OIDC endpoints must use HTTPS")
        return self


def create_production_event_gateway_app(
    *,
    event_settings: EventGatewaySettings | None = None,
    database_settings: DatabaseSettings | None = None,
) -> FastAPI:
    """Create a graph that can authenticate, read runs, and read events only."""

    resolved = event_settings or EventGatewaySettings()
    database = Database(database_settings or DatabaseSettings())
    telemetry = PlatformTelemetry(
        TelemetrySettings(
            service_name="event-gateway",
            environment=resolved.telemetry_environment,
            otlp_http_endpoint=resolved.otlp_http_endpoint,
        )
    )
    authenticator = _event_authenticator(resolved)
    services = EventGatewayServices(
        authenticator=authenticator,
        runs=PostgresRunRepository(database.sessions),
        events=PostgresEventStore(database.sessions),
        readiness=database,
        tenant_access=PostgresLifecycleRepository(database.sessions),
    )

    async def close() -> None:
        try:
            if isinstance(authenticator, OidcAuthenticator):
                await authenticator.aclose()
        finally:
            try:
                await database.aclose()
            finally:
                telemetry.shutdown()

    return create_event_gateway_app(
        services,
        close=close,
        telemetry=telemetry,
        metrics_token=(
            resolved.metrics_token.get_secret_value()
            if resolved.metrics_token is not None
            else None
        ),
    )


def _event_authenticator(
    settings: EventGatewaySettings,
) -> StaticTokenAuthenticator | OidcAuthenticator:
    if settings.auth_mode == "static":
        if settings.event_credentials_json is None:
            raise ValueError("static event credentials are not configured")
        return StaticTokenAuthenticator(
            _credentials_json(settings.event_credentials_json.get_secret_value())
        )
    if (
        settings.oidc_issuer is None
        or settings.oidc_audience is None
        or settings.oidc_jwks_url is None
    ):
        raise ValueError("OIDC event settings are incomplete")
    return OidcAuthenticator(
        issuer=settings.oidc_issuer,
        audience=settings.oidc_audience,
        jwks_url=settings.oidc_jwks_url,
        tenant_claim=settings.oidc_tenant_claim,
        cache_seconds=settings.oidc_jwks_cache_seconds,
        clock_skew_seconds=settings.oidc_clock_skew_seconds,
    )


__all__ = ["EventGatewaySettings", "create_production_event_gateway_app"]
