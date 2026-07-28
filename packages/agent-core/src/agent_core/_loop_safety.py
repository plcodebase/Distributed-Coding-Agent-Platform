"""Secret-safe serialization helpers used at agent-loop boundaries."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from agent_core.domain.base import FrozenJsonObject, JsonObject
from agent_core.domain.errors import ErrorDetail

if TYPE_CHECKING:
    from platform_telemetry import Redactor

MAX_SAFE_ERROR_BYTES = 64 * 1024
MAX_SAFE_ERROR_MESSAGE_BYTES = 4096


def _utf8_prefix(value: str, byte_limit: int) -> str:
    return value.encode("utf-8")[:byte_limit].decode("utf-8", errors="ignore")


def redact_json(value: FrozenJsonObject, redactor: Redactor) -> FrozenJsonObject:
    """Return an immutable recursively redacted JSON object."""

    redacted = redactor.redact(value.to_json_object())
    if not isinstance(redacted, dict):
        raise TypeError("redacting a JSON object must preserve its object shape")
    return FrozenJsonObject(cast("JsonObject", redacted))


def contains_secret(value: FrozenJsonObject, redactor: Redactor) -> bool:
    """Return whether recursive redaction would change a model argument object."""

    original = value.to_json_object()
    redacted = cast("object", redactor.redact(original))
    return redacted != original


def redact_error(error: ErrorDetail, redactor: Redactor) -> ErrorDetail:
    """Redact and revalidate an externally supplied structured error."""

    redacted = redactor.redact(error.model_dump(mode="json"))
    if not isinstance(redacted, dict):
        raise TypeError("redacting an error must preserve its object shape")
    safe_error = ErrorDetail.model_validate(redacted)
    if len(safe_error.model_dump_json().encode("utf-8")) <= MAX_SAFE_ERROR_BYTES:
        return safe_error
    return ErrorDetail(
        code=safe_error.code,
        message=_utf8_prefix(safe_error.message, MAX_SAFE_ERROR_MESSAGE_BYTES),
        retryable=safe_error.retryable,
        details={"details_truncated": True},
    )


__all__ = [
    "MAX_SAFE_ERROR_BYTES",
    "MAX_SAFE_ERROR_MESSAGE_BYTES",
    "contains_secret",
    "redact_error",
    "redact_json",
]
