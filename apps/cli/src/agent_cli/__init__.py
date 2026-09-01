"""Local CLI for the distributed coding-agent platform."""

from agent_cli.renderer import (
    MAX_EVENT_LINE_BYTES,
    DuplicateJsonKeyError,
    iter_rendered_events,
    parse_event_json,
    render_event,
)

__all__ = [
    "MAX_EVENT_LINE_BYTES",
    "DuplicateJsonKeyError",
    "iter_rendered_events",
    "parse_event_json",
    "render_event",
]
