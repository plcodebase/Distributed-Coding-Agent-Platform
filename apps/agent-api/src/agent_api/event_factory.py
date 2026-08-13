"""Production composition for the least-privilege event gateway."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from agent_api.auth import StaticTokenAuthenticator
from agent_api.dependencies import EventGatewayServices
from agent_api.event_app import create_event_gateway_app
from agent_api.factory import _credentials_json
from event_store import PostgresEventStore
from platform_persistence import Database, DatabaseSettings, PostgresRunRepository
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

    event_credentials_json: SecretStr = Field(
        description="JSON map from event bearer tokens to tenant principals"
    )
    telemetry_environment: str = Field(default="development", min_length=1, max_length=128)
    otlp_http_endpoint: str | None = Field(default=None, max_length=2_048)
    metrics_token: SecretStr | None = None


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
    services = EventGatewayServices(
        authenticator=StaticTokenAuthenticator(
            _credentials_json(resolved.event_credentials_json.get_secret_value())
        ),
        runs=PostgresRunRepository(database.sessions),
        events=PostgresEventStore(database.sessions),
        readiness=database,
    )

    async def close() -> None:
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


__all__ = ["EventGatewaySettings", "create_production_event_gateway_app"]
