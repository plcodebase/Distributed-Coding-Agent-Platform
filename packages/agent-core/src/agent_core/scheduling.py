"""Provider-neutral scheduling priorities and bounded admission policy."""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field

from agent_core.domain.base import DomainModel


class RunPriorityClass(StrEnum):
    """Closed queue classes exposed by the control plane."""

    INTERACTIVE = "interactive"
    BACKGROUND = "background"
    EVALUATION = "evaluation"


class QueueAdmissionPolicy(DomainModel):
    """Platform-controlled global queue and scheduling bounds."""

    global_queue_limit: int = Field(default=10_000, ge=1, le=1_000_000)
    retry_after_seconds: float = Field(default=1, gt=0, le=3600)
    priority_aging_seconds: float = Field(default=300, gt=0, le=86_400)


__all__ = ["QueueAdmissionPolicy", "RunPriorityClass"]
