from __future__ import annotations

import hashlib
import io
import json
import sys
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from agent_cli.main import _write_exclusive, main
from agent_cli.renderer import (
    MAX_EVENT_LINE_BYTES,
    DuplicateJsonKeyError,
    iter_rendered_events,
    parse_event_json,
    render_event,
)

RUN_ID = uuid.UUID("10000000-0000-0000-0000-000000000001")
CHECKPOINT_ID = uuid.UUID("20000000-0000-0000-0000-000000000002")
APPROVAL_ID = uuid.UUID("30000000-0000-0000-0000-000000000003")
NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _event(event_type: str, payload: dict[str, Any], sequence: int) -> dict[str, Any]:
    return {
        "run_id": str(RUN_ID),
        "sequence": sequence,
        "event_type": event_type,
        "payload": payload,
        "created_at": NOW.isoformat(),
    }


def _all_event_documents() -> tuple[dict[str, Any], ...]:
    empty_hash = hashlib.sha256(b"{}").hexdigest()
    error = {
        "code": "sample_failure",
        "message": "safe failure",
        "retryable": False,
        "details": {},
    }
    return (
        _event("run.started", {"attempt": 1, "worker_id": "worker-1"}, 1),
        _event(
            "context.build_started",
            {"message_count": 2, "checkpoint_id": None},
            2,
        ),
        _event(
            "model.request_started",
            {"model_call_id": "call-1", "request_id": "request-1", "route_name": "route"},
            3,
        ),
        _event("model.text_delta", {"model_call_id": "call-1", "delta": "hi\n\x1b[31m"}, 4),
        _event(
            "model.tool_call_received",
            {
                "model_call_id": "call-1",
                "tool_call_id": "tool-1",
                "tool_name": "read_file",
                "arguments": {},
                "argument_hash": empty_hash,
                "error": None,
            },
            5,
        ),
        _event(
            "tool.approval_required",
            {
                "approval_id": str(APPROVAL_ID),
                "tool_call_id": "tool-1",
                "tool_name": "edit_file",
                "arguments": {},
                "argument_hash": empty_hash,
                "reason": "workspace mutation",
            },
            6,
        ),
        _event("tool.started", {"tool_call_id": "tool-1", "tool_name": "read_file"}, 7),
        _event("tool.stdout", {"tool_call_id": "tool-1", "chunk": "output", "truncated": False}, 8),
        _event("tool.stderr", {"tool_call_id": "tool-1", "chunk": "warning", "truncated": True}, 9),
        _event(
            "tool.completed",
            {"tool_call_id": "tool-1", "status": "completed", "result": {}, "error": None},
            10,
        ),
        _event(
            "checkpoint.created",
            {
                "checkpoint_id": str(CHECKPOINT_ID),
                "tool_call_id": "tool-1",
                "message_sequence": 2,
                "workspace_revision": "a" * 40,
            },
            11,
        ),
        _event(
            "run.retry_scheduled",
            {"attempt": 2, "delay_seconds": 1.5, "error": error},
            12,
        ),
        _event(
            "run.completed",
            {"final_text": "done", "checkpoint_id": str(CHECKPOINT_ID)},
            13,
        ),
        _event("run.failed", {"error": error}, 14),
    )


def test_renderer_supports_every_typed_event_and_escapes_controls() -> None:
    rendered = [
        render_event(parse_event_json(json.dumps(value))) for value in _all_event_documents()
    ]

    assert len(rendered) == 14
    assert all(line.startswith(f"[{index} ") for index, line in enumerate(rendered, start=1))
    assert "\\n\\u001b[31m" in rendered[3]
    assert "\n" not in rendered[3]


def test_event_json_and_json_lines_fail_closed() -> None:
    first = json.dumps(_all_event_documents()[0]).encode("utf-8")
    assert list(iter_rendered_events(io.BytesIO(first + b"\n\n"))) == [
        render_event(parse_event_json(first))
    ]
    with pytest.raises(DuplicateJsonKeyError):
        parse_event_json(b'{"run_id":"10000000-0000-0000-0000-000000000001","run_id":"x"}')
    with pytest.raises(ValueError, match="byte limit"):
        list(iter_rendered_events(io.BytesIO(b"x" * (MAX_EVENT_LINE_BYTES + 1))))


def test_cli_render_command_and_exclusive_patch_write(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    document = json.dumps(_all_event_documents()[0]).encode("utf-8")
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(document + b"\n")))

    with pytest.raises(SystemExit) as exit_status:
        main(["render"])

    assert exit_status.value.code == 0
    assert "run.started" in capsys.readouterr().out
    patch = tmp_path / "result.patch"
    _write_exclusive(patch, b"diff")
    assert patch.read_bytes() == b"diff"
    assert patch.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        _write_exclusive(patch, b"replacement")
