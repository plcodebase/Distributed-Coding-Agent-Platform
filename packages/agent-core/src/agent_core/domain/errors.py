"""Structured errors raised by domain operations."""

from collections.abc import Iterable
from typing import cast

from agent_core.domain.base import JsonObject
from agent_core.domain.status import RunStatus


class DomainOperationError(ValueError):
    """A stable error code plus JSON-safe context for boundary translation."""

    def __init__(
        self,
        *,
        code: str,
        message: str,
        details: JsonObject | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.details = dict(details or {})
        super().__init__(message)

    def as_dict(self) -> JsonObject:
        """Return the transport-safe representation of this error."""

        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


class InvalidRunTransitionError(DomainOperationError):
    """Raised when a run state change is absent from the central policy."""

    def __init__(
        self,
        current_status: RunStatus,
        requested_status: RunStatus,
        allowed_statuses: Iterable[RunStatus],
    ) -> None:
        allowed = sorted(status.value for status in allowed_statuses)
        super().__init__(
            code="invalid_run_transition",
            message=(
                f"run cannot transition from {current_status.value} to {requested_status.value}"
            ),
            details=cast(
                "JsonObject",
                {
                    "current_status": current_status.value,
                    "requested_status": requested_status.value,
                    "allowed_statuses": allowed,
                },
            ),
        )
        self.current_status = current_status
        self.requested_status = requested_status
        self.allowed_statuses = tuple(allowed)
