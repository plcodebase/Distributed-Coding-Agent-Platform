"""Structured errors raised by domain operations."""

from collections.abc import Iterable
from typing import Annotated

from pydantic import Field, StringConstraints

from agent_core.domain.base import DomainModel, FrozenJsonObject, JsonObject
from agent_core.domain.status import RunStatus

type ErrorCode = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=100,
        pattern=r"^[a-z][a-z0-9_]*$",
    ),
]
type ErrorMessage = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class ErrorDetail(DomainModel):
    """Validated, transport-safe failure detail shared by errors and events."""

    code: ErrorCode
    message: ErrorMessage
    retryable: bool = False
    details: FrozenJsonObject = Field(default_factory=lambda: FrozenJsonObject({}))


class DomainOperationError(ValueError):
    """A validated error detail suitable for stable boundary translation."""

    def __init__(
        self,
        *,
        code: ErrorCode,
        message: ErrorMessage,
        retryable: bool = False,
        details: JsonObject | FrozenJsonObject | None = None,
    ) -> None:
        self.error = ErrorDetail(
            code=code,
            message=message,
            retryable=retryable,
            details=details or {},
        )
        self.code = self.error.code
        self.message = self.error.message
        self.retryable = self.error.retryable
        self.details = self.error.details
        super().__init__(self.message)

    def as_dict(self) -> JsonObject:
        """Return a defensive transport-safe representation of this error."""

        return self.error.model_dump(mode="json")


class InvalidRunTransitionError(DomainOperationError):
    """Raised when a run state change is absent from the central policy."""

    def __init__(
        self,
        current_status: RunStatus,
        requested_status: RunStatus,
        allowed_statuses: Iterable[RunStatus],
    ) -> None:
        allowed = sorted(status.value for status in allowed_statuses)
        details: JsonObject = {
            "current_status": current_status.value,
            "requested_status": requested_status.value,
            "allowed_statuses": list(allowed),
        }
        super().__init__(
            code="invalid_run_transition",
            message=(
                f"run cannot transition from {current_status.value} to {requested_status.value}"
            ),
            details=details,
        )
        self.current_status = current_status
        self.requested_status = requested_status
        self.allowed_statuses = tuple(allowed)
