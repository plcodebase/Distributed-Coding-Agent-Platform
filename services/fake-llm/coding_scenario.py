"""Fail-closed deterministic coding scenario for the Podman E2E fixture."""

from __future__ import annotations

import json
from typing import Any

MARKER = "[fixture:calculator-bug-v1]"
BROKEN_CALCULATOR_SHA256 = "41d93f17b5543d1ec2c885c4df2a217613fae28347441374a5f4db9cf9d30b44"
type CodingAction = tuple[str, str, dict[str, object]] | str


def is_request(request: object) -> bool:
    if not isinstance(request, dict):
        return False
    messages = request.get("messages")
    if not isinstance(messages, list):
        return False
    return any(
        isinstance(message, dict)
        and isinstance(message.get("content"), str)
        and MARKER in message["content"]
        for message in messages
    )


def next_action(request: dict[str, Any]) -> CodingAction:
    """Return the next action only for the exact successful fixture transcript."""

    completed = _successful_tool_result_ids(request)
    if completed == ():
        return "e2e-read-calculator", "read_file", {"path": "calculator.py"}
    if completed == ("e2e-read-calculator",):
        return "e2e-read-test", "read_file", {"path": "test_calculator.py"}
    if completed == ("e2e-read-calculator", "e2e-read-test"):
        return (
            "e2e-edit-calculator",
            "edit_file",
            {
                "path": "calculator.py",
                "expected_sha256": BROKEN_CALCULATOR_SHA256,
                "old_text": "return left - right",
                "new_text": "return left + right",
            },
        )
    if completed == (
        "e2e-read-calculator",
        "e2e-read-test",
        "e2e-edit-calculator",
    ):
        return (
            "e2e-run-tests",
            "run_command",
            {
                "argv": ["python", "-m", "unittest", "-v"],
                "timeout_seconds": 30,
            },
        )
    if completed == (
        "e2e-read-calculator",
        "e2e-read-test",
        "e2e-edit-calculator",
        "e2e-run-tests",
    ):
        return "Fixed calculator.add and verified the repository test suite in the sandbox."
    raise ValueError("coding scenario received an unexpected tool-result sequence")


def text_chunks(*, response_id: str, model: str, text: str) -> tuple[dict[str, object], ...]:
    return (
        _stream_chunk(response_id=response_id, model=model, delta={"role": "assistant"}),
        _stream_chunk(response_id=response_id, model=model, delta={"content": text}),
        _stream_chunk(
            response_id=response_id,
            model=model,
            delta={},
            finish_reason="stop",
            usage={"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        ),
    )


def tool_chunks(
    *,
    response_id: str,
    model: str,
    call_id: str,
    tool_name: str,
    arguments: dict[str, object],
) -> tuple[dict[str, object], ...]:
    encoded = json.dumps(
        arguments,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    split_at = max(1, len(encoded) // 2)
    return (
        _stream_chunk(
            response_id=response_id,
            model=model,
            delta={
                "role": "assistant",
                "tool_calls": [
                    {
                        "index": 0,
                        "id": call_id,
                        "type": "function",
                        "function": {
                            "name": tool_name,
                            "arguments": encoded[:split_at],
                        },
                    }
                ],
            },
        ),
        _stream_chunk(
            response_id=response_id,
            model=model,
            delta={
                "tool_calls": [
                    {
                        "index": 0,
                        "function": {"arguments": encoded[split_at:]},
                    }
                ]
            },
        ),
        _stream_chunk(
            response_id=response_id,
            model=model,
            delta={},
            finish_reason="tool_calls",
            usage={"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        ),
    )


def _successful_tool_result_ids(request: dict[str, Any]) -> tuple[str, ...]:
    messages = request.get("messages")
    if not isinstance(messages, list):
        return ()
    identifiers: list[str] = []
    for message in messages:
        if not isinstance(message, dict) or message.get("role") != "tool":
            continue
        tool_call_id = message.get("tool_call_id")
        content = message.get("content")
        if not isinstance(tool_call_id, str) or not isinstance(content, str):
            raise TypeError("coding scenario received malformed tool feedback")
        try:
            feedback = json.loads(content)
        except json.JSONDecodeError as error:
            raise ValueError("coding scenario received malformed tool feedback") from error
        if not isinstance(feedback, dict) or feedback.get("ok") is not True:
            raise ValueError("coding scenario tool execution did not succeed")
        identifiers.append(tool_call_id)
    return tuple(identifiers)


def _stream_chunk(
    *,
    response_id: str,
    model: str,
    delta: dict[str, object],
    finish_reason: str | None = None,
    usage: dict[str, int] | None = None,
) -> dict[str, object]:
    chunk: dict[str, object] = {
        "id": response_id,
        "object": "chat.completion.chunk",
        "created": 1,
        "model": model,
        "choices": [
            {
                "index": 0,
                "delta": delta,
                "finish_reason": finish_reason,
            }
        ],
    }
    if usage is not None:
        chunk["usage"] = usage
    return chunk
