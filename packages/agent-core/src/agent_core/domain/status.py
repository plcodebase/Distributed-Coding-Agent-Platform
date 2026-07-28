"""Finite domain statuses used by the control and execution planes."""

from enum import StrEnum


class SessionStatus(StrEnum):
    """Lifecycle of an ongoing workspace conversation."""

    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class ApprovalMode(StrEnum):
    """Human-confirmation policy; safety validation remains mandatory in every mode."""

    REQUIRE_ALL = "require_all"
    REQUIRE_SENSITIVE = "require_sensitive"
    AUTO_APPROVE = "auto_approve"


class RunStatus(StrEnum):
    """Durable run states from the design state machine."""

    QUEUED = "queued"
    LEASED = "leased"
    RUNNING = "running"
    WAITING_APPROVAL = "waiting_approval"
    RETRY_PENDING = "retry_pending"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    LOST = "lost"


class ToolCallStatus(StrEnum):
    """Execution state of a stable logical tool call."""

    RECEIVED = "received"
    WAITING_APPROVAL = "waiting_approval"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ModelCallStatus(StrEnum):
    """Lifecycle of a normalized model gateway request."""

    STARTED = "started"
    STREAMING = "streaming"
    COMPLETED = "completed"
    FAILED = "failed"
