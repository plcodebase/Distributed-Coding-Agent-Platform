"""Core domain models and centrally validated state transitions."""

from agent_core.domain.base import AwareTimestamp, DomainModel, JsonObject
from agent_core.domain.errors import DomainOperationError, InvalidRunTransitionError
from agent_core.domain.models import (
    Checkpoint,
    ModelCall,
    Run,
    Session,
    ToolCall,
    canonical_argument_hash,
)
from agent_core.domain.status import (
    ApprovalMode,
    ModelCallStatus,
    RunStatus,
    SessionStatus,
    ToolCallStatus,
)
from agent_core.domain.transitions import (
    RUN_STATUS_TRANSITIONS,
    allowed_run_transitions,
    transition_run,
)

__all__ = [
    "RUN_STATUS_TRANSITIONS",
    "ApprovalMode",
    "AwareTimestamp",
    "Checkpoint",
    "DomainModel",
    "DomainOperationError",
    "InvalidRunTransitionError",
    "JsonObject",
    "ModelCall",
    "ModelCallStatus",
    "Run",
    "RunStatus",
    "Session",
    "SessionStatus",
    "ToolCall",
    "ToolCallStatus",
    "allowed_run_transitions",
    "canonical_argument_hash",
    "transition_run",
]
