"""Production scheduler composition over durable platform dependencies."""

from __future__ import annotations

import hashlib
import json
from typing import Annotated

from pydantic import Field, StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict

from agent_core.domain.errors import DomainOperationError
from agent_core.loop import UtcClock, UuidIdGenerator
from agent_core.memory import GatewayMemoryExtractor
from agent_core.settings import PlatformSettings
from agent_scheduler.memory import MemoryExtractionProcessor
from agent_scheduler.service import SchedulerConfig, SchedulerService
from agents_sdk_adapter import create_openai_compatible_agents_gateway
from artifact_store import (
    S3ObjectStoreSettings,
    SnapshotValidationWorker,
    SnapshotValidator,
    create_s3_object_store,
)
from gateway_client import (
    ConfiguredCostCalculator,
    GatewayClient,
    GatewayClientConfig,
    RoutePrice,
)
from platform_persistence import (
    Database,
    DatabaseSettings,
    PostgresExecutionRepository,
    PostgresGatewayCapacityStore,
    PostgresGatewayCircuitBreaker,
    PostgresGatewayRateLimiter,
    PostgresGatewayRequestStore,
    PostgresMemoryRepository,
    PostgresRunQueue,
    PostgresWorkspaceRepository,
)
from platform_telemetry import PlatformTelemetry, Redactor, TelemetrySettings
from queue_wakeup import RedisRunWakeup, RedisWakeupSettings


class _DuplicateJsonKeyError(ValueError):
    pass


class ProductionSchedulerSettings(BaseSettings):
    """Bounded identifiers and polling intervals for one scheduler process."""

    model_config = SettingsConfigDict(
        env_prefix="AGENT_SCHEDULER_",
        case_sensitive=False,
        extra="ignore",
        frozen=True,
    )

    worker_id: Annotated[
        str,
        StringConstraints(pattern=r"^[a-z][a-z0-9-]{0,63}$"),
    ] = "scheduler-1"
    instance_id: Annotated[
        str,
        StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"),
    ] = "local"
    poll_seconds: float = Field(default=1, ge=0.05, le=60)
    recovery_batch_size: int = Field(default=100, ge=1, le=1000)
    memory_lease_seconds: float = Field(default=300, gt=1, le=3600)
    memory_timeout_seconds: float = Field(default=240, gt=0, le=3599)
    snapshot_lease_seconds: int = Field(default=300, ge=1, le=300)
    route_prices_json: str = Field(min_length=2, max_length=64 * 1024)


def _route_prices(raw: str, route_names: tuple[str, ...]) -> dict[str, RoutePrice]:
    try:
        value = json.loads(raw, object_pairs_hook=_unique_json_object)
    except (TypeError, ValueError):
        raise ValueError("route_prices_json must be valid JSON") from None
    if not isinstance(value, dict) or set(value) != set(route_names):
        raise ValueError("route_prices_json must define every configured route exactly once")
    try:
        prices = {
            route_name: RoutePrice.model_validate(price)
            for route_name, price in value.items()
            if isinstance(route_name, str)
        }
    except ValueError:
        raise ValueError("route_prices_json contains an invalid price") from None
    if len(prices) != len(value):
        raise ValueError("route_prices_json route names must be strings")
    return prices


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateJsonKeyError
        value[key] = item
    return value


async def create_production_scheduler(
    *,
    platform_settings: PlatformSettings | None = None,
    scheduler_settings: ProductionSchedulerSettings | None = None,
    database_settings: DatabaseSettings | None = None,
    object_store_settings: S3ObjectStoreSettings | None = None,
) -> SchedulerService:
    """Build a scheduler that owns recovery, snapshot validation, and memory jobs."""

    platform = platform_settings or PlatformSettings()
    configured = scheduler_settings or ProductionSchedulerSettings()
    if configured.memory_timeout_seconds >= configured.memory_lease_seconds:
        raise ValueError("memory timeout must be shorter than its lease")
    database = Database(database_settings or DatabaseSettings())
    wakeup = RedisRunWakeup.create(RedisWakeupSettings(url=platform.redis_url))
    telemetry = PlatformTelemetry(
        TelemetrySettings(service_name="agent-scheduler", environment=platform.environment)
    )
    object_store = create_s3_object_store(
        object_store_settings or S3ObjectStoreSettings(environment=platform.environment)
    )
    sdk_gateway = create_openai_compatible_agents_gateway(platform)
    gateway_config = GatewayClientConfig()
    execution = PostgresExecutionRepository(database.sessions)
    gateway = GatewayClient(
        sdk_gateway,
        config=gateway_config,
        close=sdk_gateway.aclose,
        request_store=PostgresGatewayRequestStore(database.sessions),
        rate_limiter=PostgresGatewayRateLimiter(
            database.sessions,
            requests_per_window=gateway_config.rate_limit_requests,
            window_seconds=gateway_config.rate_limit_window_seconds,
        ),
        capacity_store=PostgresGatewayCapacityStore(
            database.sessions,
            provider_request_limit=gateway_config.provider_concurrent_requests,
            provider_token_limit=gateway_config.provider_tokens_per_window,
            token_window_seconds=gateway_config.provider_token_window_seconds,
        ),
        circuit_breaker=PostgresGatewayCircuitBreaker(
            database.sessions,
            failure_threshold=gateway_config.circuit_failure_threshold,
            recovery_seconds=gateway_config.circuit_recovery_seconds,
        ),
        telemetry=telemetry,
        cost_calculator=ConfiguredCostCalculator(
            _route_prices(configured.route_prices_json, gateway_config.route_names)
        ),
        model_calls=execution,
    )
    queue = PostgresRunQueue(database.sessions, wakeup=wakeup.publish)
    workspaces = PostgresWorkspaceRepository(database.sessions)
    memory = MemoryExtractionProcessor(
        store=PostgresMemoryRepository(database.sessions),
        extractor=GatewayMemoryExtractor(
            gateway=gateway,
            id_generator=UuidIdGenerator(),
            redactor=Redactor(platform.redaction_values()),
        ),
        clock=UtcClock(),
        worker_id=f"{_scheduler_id(configured)}-memory",
        lease_duration_seconds=configured.memory_lease_seconds,
        extraction_timeout_seconds=configured.memory_timeout_seconds,
    )
    snapshots = SnapshotValidationWorker(
        workspaces,
        SnapshotValidator(object_store),
        worker_id=f"{_scheduler_id(configured)}-snapshot",
        lease_seconds=configured.snapshot_lease_seconds,
    )

    async def close() -> None:
        try:
            await gateway.aclose()
        finally:
            try:
                await object_store.aclose()
            finally:
                try:
                    await database.aclose()
                finally:
                    await wakeup.aclose()
                    telemetry.shutdown()

    try:
        if not await database.ready() or not await object_store.ready() or not await wakeup.ready():
            _raise_dependencies_unavailable()
        return SchedulerService(
            queue=queue,
            clock=UtcClock(),
            config=SchedulerConfig(
                poll_seconds=configured.poll_seconds,
                recovery_batch_size=configured.recovery_batch_size,
            ),
            memory_processor=memory,
            background_processors=(snapshots,),
            queue_monitor=queue,
            telemetry=telemetry,
            close=close,
        )
    except BaseException:
        await close()
        raise


__all__ = ["ProductionSchedulerSettings", "create_production_scheduler"]


def _scheduler_id(settings: ProductionSchedulerSettings) -> str:
    instance_token = hashlib.sha256(settings.instance_id.encode("utf-8")).hexdigest()[:12]
    return f"{settings.worker_id}-{instance_token}"


def _raise_dependencies_unavailable() -> None:
    raise DomainOperationError(
        code="scheduler_dependency_unavailable",
        message="one or more scheduler dependencies are not ready",
        retryable=True,
    )
