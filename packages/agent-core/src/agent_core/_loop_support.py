"""Small serialization and feedback helpers shared by loop components."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from agent_core.domain.errors import ErrorDetail
from agent_core.gateway import GatewayMessage, MessageRole

if TYPE_CHECKING:
    from agent_core.domain.base import FrozenJsonObject, JsonObject


def loop_error(
    code: str,
    message: str,
    *,
    retryable: bool = False,
    details: JsonObject | None = None,
) -> ErrorDetail:
    """Build one internal error whose fields are safe for transport."""

    return ErrorDetail(
        code=code,
        message=message,
        retryable=retryable,
        details=details or {},
    )


def json_size(value: FrozenJsonObject) -> int:
    """Return the canonical UTF-8 wire size of an immutable JSON object."""

    return len(
        json.dumps(
            value.to_json_object(),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    )


def tool_message(
    tool_call_id: str,
    *,
    result: FrozenJsonObject | None = None,
    error: ErrorDetail | None = None,
) -> GatewayMessage:
    """Create bounded, structured tool feedback for the next model turn."""

    body: JsonObject
    if error is not None:
        body = {"ok": False, "error": error.model_dump(mode="json")}
    else:
        body = {
            "ok": True,
            "result": result.to_json_object() if result is not None else {},
        }
    return GatewayMessage(
        role=MessageRole.TOOL,
        tool_call_id=tool_call_id,
        content=json.dumps(
            body,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ),
    )


def invalid_call_feedback(
    invalid_calls: tuple[tuple[str, str, ErrorDetail], ...],
) -> GatewayMessage:
    """Return safe system feedback for calls that had no valid JSON argument object."""

    failures: list[JsonObject] = [
        {
            "tool_call_id": tool_call_id,
            "tool_name": tool_name,
            "error": error.model_dump(mode="json"),
        }
        for tool_call_id, tool_name, error in invalid_calls
    ]
    content = json.dumps(
        {"tool_calls_rejected": failures},
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return GatewayMessage(role=MessageRole.SYSTEM, content=content)


__all__ = ["invalid_call_feedback", "json_size", "loop_error", "tool_message"]
