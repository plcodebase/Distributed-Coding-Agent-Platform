"""PostgreSQL system-of-record adapters for the agent platform."""

from platform_persistence.base import Base
from platform_persistence.capacity import (
    PostgresGatewayCapacityStore,
    PostgresTenantQuotaRepository,
)
from platform_persistence.database import Database, DatabaseSettings, PostgreSQLUrl
from platform_persistence.distributed import (
    PostgresRecoveryStore,
    PostgresRunQueue,
    PostgresWorkspaceLeaseStore,
)
from platform_persistence.gateway_policies import (
    PostgresGatewayCircuitBreaker,
    PostgresGatewayRateLimiter,
)
from platform_persistence.gateway_store import PostgresGatewayRequestStore
from platform_persistence.repositories import (
    ApprovalDecision,
    ApprovalStatus,
    IdempotencyKey,
    PersistedApproval,
    PersistedMessage,
    PersistedTaskPlan,
    PostgresApprovalRepository,
    PostgresExecutionRepository,
    PostgresRunRepository,
    PostgresSessionRepository,
    RunCreationResult,
    run_creation_hash,
)

__all__ = [
    "ApprovalDecision",
    "ApprovalStatus",
    "Base",
    "Database",
    "DatabaseSettings",
    "IdempotencyKey",
    "PersistedApproval",
    "PersistedMessage",
    "PersistedTaskPlan",
    "PostgreSQLUrl",
    "PostgresApprovalRepository",
    "PostgresExecutionRepository",
    "PostgresGatewayCapacityStore",
    "PostgresTenantQuotaRepository",
    "PostgresGatewayCircuitBreaker",
    "PostgresGatewayRateLimiter",
    "PostgresGatewayRequestStore",
    "PostgresRecoveryStore",
    "PostgresRunQueue",
    "PostgresRunRepository",
    "PostgresSessionRepository",
    "PostgresWorkspaceLeaseStore",
    "RunCreationResult",
    "run_creation_hash",
]
