"""PostgreSQL system-of-record adapters for the agent platform."""

from platform_persistence.audit import PostgresAuditSink
from platform_persistence.base import Base
from platform_persistence.capacity import (
    PostgresGatewayCapacityStore,
    PostgresTenantQuotaRepository,
)
from platform_persistence.context_management import (
    PostgresContextRepository,
    PostgresMemoryRepository,
    PostgresTaskRepository,
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
from platform_persistence.workspaces import (
    PostgresWorkspaceRepository,
    source_snapshot_object_key,
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
    "PostgresAuditSink",
    "PostgresContextRepository",
    "PostgresExecutionRepository",
    "PostgresGatewayCapacityStore",
    "PostgresGatewayCircuitBreaker",
    "PostgresGatewayRateLimiter",
    "PostgresGatewayRequestStore",
    "PostgresMemoryRepository",
    "PostgresRecoveryStore",
    "PostgresRunQueue",
    "PostgresRunRepository",
    "PostgresSessionRepository",
    "PostgresTaskRepository",
    "PostgresTenantQuotaRepository",
    "PostgresWorkspaceLeaseStore",
    "PostgresWorkspaceRepository",
    "RunCreationResult",
    "run_creation_hash",
    "source_snapshot_object_key",
]
