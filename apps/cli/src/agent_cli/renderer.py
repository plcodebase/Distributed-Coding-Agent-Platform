"""Safe terminal rendering for the platform's typed event protocol."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from agent_core.events import (
    AnyAgentEvent,
    CheckpointCreatedEvent,
    ContextBuildStartedEvent,
    ModelRequestStartedEvent,
    ModelTextDeltaEvent,
    ModelToolCallReceivedEvent,
    RunCompletedEvent,
    RunFailedEvent,
    RunRetryScheduledEvent,
    RunStartedEvent,
    ToolApprovalRequiredEvent,
    ToolCompletedEvent,
    ToolStartedEvent,
    ToolStderrEvent,
    ToolStdoutEvent,
    parse_agent_event,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import BinaryIO

MAX_EVENT_LINE_BYTES = 2 * 1024 * 1024


class DuplicateJsonKeyError(ValueError):
    """Raised when an untrusted JSON event contains an ambiguous object."""


def render_event(event: AnyAgentEvent) -> str:  # noqa: PLR0911, PLR0912 - closed event union
    """Render one validated event without allowing terminal control injection."""

    prefix = f"[{event.sequence} {event.event_type.value}]"
    if isinstance(event, RunStartedEvent):
        return f"{prefix} attempt={event.payload.attempt} worker={_safe(event.payload.worker_id)}"
    if isinstance(event, ContextBuildStartedEvent):
        return f"{prefix} messages={event.payload.message_count}"
    if isinstance(event, ModelRequestStartedEvent):
        return (
            f"{prefix} route={_safe(event.payload.route_name)} "
            f"call={_safe(event.payload.model_call_id)}"
        )
    if isinstance(event, ModelTextDeltaEvent):
        return f"{prefix} {_safe(event.payload.delta)}"
    if isinstance(event, ModelToolCallReceivedEvent):
        detail = event.payload.error
        outcome = f"rejected={_safe(detail.code)}" if detail is not None else "accepted"
        return (
            f"{prefix} tool={_safe(event.payload.tool_name)} "
            f"id={_safe(event.payload.tool_call_id)} {outcome}"
        )
    if isinstance(event, ToolApprovalRequiredEvent):
        return (
            f"{prefix} tool={_safe(event.payload.tool_name)} "
            f"id={_safe(event.payload.tool_call_id)} reason={_safe(event.payload.reason)}"
        )
    if isinstance(event, ToolStartedEvent):
        return (
            f"{prefix} tool={_safe(event.payload.tool_name)} id={_safe(event.payload.tool_call_id)}"
        )
    if isinstance(event, ToolStdoutEvent):
        suffix = " truncated=true" if event.payload.truncated else ""
        return f"{prefix} {_safe(event.payload.chunk)}{suffix}"
    if isinstance(event, ToolStderrEvent):
        suffix = " truncated=true" if event.payload.truncated else ""
        return f"{prefix} {_safe(event.payload.chunk)}{suffix}"
    if isinstance(event, ToolCompletedEvent):
        detail = event.payload.error
        error = f" error={_safe(detail.code)}" if detail is not None else ""
        return (
            f"{prefix} id={_safe(event.payload.tool_call_id)} "
            f"status={event.payload.status.value}{error}"
        )
    if isinstance(event, CheckpointCreatedEvent):
        return (
            f"{prefix} id={event.payload.checkpoint_id} "
            f"tool={_safe(event.payload.tool_call_id)} "
            f"revision={_safe(event.payload.workspace_revision)}"
        )
    if isinstance(event, RunRetryScheduledEvent):
        return (
            f"{prefix} attempt={event.payload.attempt} "
            f"delay={event.payload.delay_seconds:g}s error={_safe(event.payload.error.code)}"
        )
    if isinstance(event, RunCompletedEvent):
        return f"{prefix} {_safe(event.payload.final_text)}"
    if isinstance(event, RunFailedEvent):
        return (
            f"{prefix} error={_safe(event.payload.error.code)} "
            f"message={_safe(event.payload.error.message)}"
        )
    raise TypeError("unsupported agent event")


def parse_event_json(value: bytes | str) -> AnyAgentEvent:
    """Parse one bounded strict JSON event and reject duplicate keys."""

    encoded = value.encode("utf-8") if isinstance(value, str) else value
    if not encoded or len(encoded) > MAX_EVENT_LINE_BYTES:
        raise ValueError("event JSON must be non-empty and within the byte limit")
    try:
        decoded = encoded.decode("utf-8", errors="strict")
        document = json.loads(decoded, object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("event input must be valid UTF-8 JSON") from error
    return parse_agent_event(document)


def iter_rendered_events(stream: BinaryIO) -> Iterator[str]:
    """Read bounded JSON Lines from a binary stream and render validated events."""

    while line := stream.readline(MAX_EVENT_LINE_BYTES + 1):
        if len(line) > MAX_EVENT_LINE_BYTES:
            raise ValueError("event line exceeds the byte limit")
        if not line.strip():
            continue
        yield render_event(parse_event_json(line))


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise DuplicateJsonKeyError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _safe(value: str) -> str:
    """Escape all control characters while preserving readable Unicode."""

    return json.dumps(value, ensure_ascii=False)[1:-1]


__all__ = [
    "MAX_EVENT_LINE_BYTES",
    "DuplicateJsonKeyError",
    "iter_rendered_events",
    "parse_event_json",
    "render_event",
]
