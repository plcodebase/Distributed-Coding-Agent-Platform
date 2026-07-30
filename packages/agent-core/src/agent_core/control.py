"""Provider-neutral durable control-plane values shared by API and persistence."""

from __future__ import annotations

import hashlib
import json
import uuid  # noqa: TC003 - Pydantic resolves identifiers at runtime
from enum import StrEnum
from typing import Annotated, Self

from pydantic import Field, StringConstraints, model_validator

from agent_core.domain.base import AwareTimestamp, DomainModel, FrozenJsonObject
from agent_core.domain.models import Run  # noqa: TC001 - Pydantic resolves run at runtime
from agent_core.gateway import MessageRole  # noqa: TC001 - Pydantic resolves role at runtime

type IdempotencyKey = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=255,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    ),
]


class RunCreationResult(DomainModel):
    """Outcome of one API-idempotent run creation transaction."""

    run: Run
    created: bool


class ApprovalStatus(StrEnum):
    """Durable approval decision state."""

    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class ApprovalDecision(DomainModel):
    """Validated approval mutation supplied by an authenticated principal."""

    approved: bool
    decided_by: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
    ]
    decided_at: AwareTimestamp


class PersistedApproval(DomainModel):
    """Tenant-safe approval representation returned to the API."""

    id: uuid.UUID
    run_id: uuid.UUID
    status: ApprovalStatus
    reason: str
    arguments: FrozenJsonObject = Field(default_factory=lambda: FrozenJsonObject({}))
    decided_by: str | None = None
    requested_at: AwareTimestamp
    decided_at: AwareTimestamp | None = None

    @model_validator(mode="after")
    def validate_decision(self) -> Self:
        if self.status is ApprovalStatus.PENDING:
            if self.decided_by is not None or self.decided_at is not None:
                raise ValueError("pending approval may not contain decision metadata")
        elif self.decided_by is None or self.decided_at is None:
            raise ValueError("decided approval requires subject and timestamp")
        if self.decided_at is not None and self.decided_at < self.requested_at:
            raise ValueError("approval decision may not precede its request")
        return self


class PersistedMessage(DomainModel):
    """Validated durable conversation message."""

    id: uuid.UUID
    session_id: uuid.UUID
    run_id: uuid.UUID
    sequence: int = Field(ge=1)
    role: MessageRole
    content: str
    metadata: FrozenJsonObject = Field(default_factory=lambda: FrozenJsonObject({}))
    created_at: AwareTimestamp


class PersistedTaskPlan(DomainModel):
    """Validated versioned task plan."""

    id: uuid.UUID
    run_id: uuid.UUID
    version: int = Field(ge=1)
    plan: FrozenJsonObject
    created_at: AwareTimestamp


def run_creation_hash(*, priority: int) -> str:
    """Return the canonical payload identity for API run idempotency."""

    encoded = json.dumps(
        {"priority": priority},
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "ApprovalDecision",
    "ApprovalStatus",
    "IdempotencyKey",
    "PersistedApproval",
    "PersistedMessage",
    "PersistedTaskPlan",
    "RunCreationResult",
    "run_creation_hash",
]
