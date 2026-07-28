"""Finite domain statuses used by the control and execution planes."""

from enum import StrEnum


class SessionStatus(StrEnum):
    """Lifecycle of an ongoing workspace conversation."""

    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class ApprovalMode(StrEnum):
    """When a session requires human approval for a proposed action."""

    ALWAYS = "always"
    ON_REQUEST = "on_request"
    NEVER = "never"


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
