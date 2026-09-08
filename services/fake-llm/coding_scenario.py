"""Fail-closed deterministic coding scenario for the Podman E2E fixture."""

from __future__ import annotations

import json
from typing import Any

MARKER = "[fixture:calculator-bug-v1]"
RETRY_MARKER = "[fixture:calculator-retry-v1]"
BROKEN_CALCULATOR_SHA256 = "41d93f17b5543d1ec2c885c4df2a217613fae28347441374a5f4db9cf9d30b44"
INCORRECT_CALCULATOR_SHA256 = "d7831cb0fa01a051d7923a3bece40d7ef8b91c1bb81ce38792958663eb235fb3"
type CodingAction = tuple[str, str, dict[str, object]] | str
type ToolFeedback = tuple[str, bool, str | None]


def is_request(request: object) -> bool:
    if not isinstance(request, dict):
        return False
    messages = request.get("messages")
    if not isinstance(messages, list):
        return False
    return any(
        isinstance(message, dict)
        and isinstance(message.get("content"), str)
        and (MARKER in message["content"] or RETRY_MARKER in message["content"])
        for message in messages
    )


def next_action(request: dict[str, Any]) -> CodingAction:
    """Return the next action only for the exact successful fixture transcript."""

    if _contains_marker(request, RETRY_MARKER):
        return _retry_action(_tool_feedback(request))
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


def _retry_action(feedback: tuple[ToolFeedback, ...]) -> CodingAction:  # noqa: PLR0911
    signature = tuple((tool_call_id, ok, error_code) for tool_call_id, ok, error_code in feedback)
    if signature == ():
        return "e2e-retry-read-calculator", "read_file", {"path": "calculator.py"}
    if signature == (("e2e-retry-read-calculator", True, None),):
        return "e2e-retry-read-test", "read_file", {"path": "test_calculator.py"}
    if signature == (
        ("e2e-retry-read-calculator", True, None),
        ("e2e-retry-read-test", True, None),
    ):
        return (
            "e2e-retry-edit-incorrect",
            "edit_file",
            {
                "path": "calculator.py",
                "expected_sha256": BROKEN_CALCULATOR_SHA256,
                "old_text": "return left - right",
                "new_text": "return left * right",
            },
        )
    if signature == (
        ("e2e-retry-read-calculator", True, None),
        ("e2e-retry-read-test", True, None),
        ("e2e-retry-edit-incorrect", True, None),
    ):
        return (
            "e2e-retry-tests-fail",
            "run_command",
            {
                "argv": ["python", "-m", "unittest", "-v"],
                "timeout_seconds": 30,
            },
        )
    if signature == (
        ("e2e-retry-read-calculator", True, None),
        ("e2e-retry-read-test", True, None),
        ("e2e-retry-edit-incorrect", True, None),
        ("e2e-retry-tests-fail", False, "command_failed"),
    ):
        return (
            "e2e-retry-edit-correct",
            "edit_file",
            {
                "path": "calculator.py",
                "expected_sha256": INCORRECT_CALCULATOR_SHA256,
                "old_text": "return left * right",
                "new_text": "return left + right",
            },
        )
    if signature == (
        ("e2e-retry-read-calculator", True, None),
        ("e2e-retry-read-test", True, None),
        ("e2e-retry-edit-incorrect", True, None),
        ("e2e-retry-tests-fail", False, "command_failed"),
        ("e2e-retry-edit-correct", True, None),
    ):
        return (
            "e2e-retry-tests-pass",
            "run_command",
            {
                "argv": ["python", "-m", "unittest", "-v"],
                "timeout_seconds": 30,
            },
        )
    if signature == (
        ("e2e-retry-read-calculator", True, None),
        ("e2e-retry-read-test", True, None),
        ("e2e-retry-edit-incorrect", True, None),
        ("e2e-retry-tests-fail", False, "command_failed"),
        ("e2e-retry-edit-correct", True, None),
        ("e2e-retry-tests-pass", True, None),
    ):
        return "Observed the failing test, corrected calculator.add, and verified the fix."
    raise ValueError("coding retry scenario received an unexpected tool-result sequence")


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
    feedback = _tool_feedback(request)
    identifiers: list[str] = []
    for tool_call_id, ok, _error_code in feedback:
        if not ok:
            raise ValueError("coding scenario tool execution did not succeed")
        identifiers.append(tool_call_id)
    return tuple(identifiers)


def _tool_feedback(request: dict[str, Any]) -> tuple[ToolFeedback, ...]:
    messages = request.get("messages")
    if not isinstance(messages, list):
        return ()
    feedback_items: list[ToolFeedback] = []
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
        if not isinstance(feedback, dict) or not isinstance(feedback.get("ok"), bool):
            raise TypeError("coding scenario received malformed tool feedback")
        ok = feedback["ok"]
        error_code: str | None = None
        if not ok:
            parsed_error = feedback.get("error")
            if parsed_error is not None and (
                not isinstance(parsed_error, dict) or not isinstance(parsed_error.get("code"), str)
            ):
                raise ValueError("coding scenario received malformed tool feedback")
            if isinstance(parsed_error, dict):
                error_code = parsed_error["code"]
        feedback_items.append((tool_call_id, ok, error_code))
    return tuple(feedback_items)


def _contains_marker(request: dict[str, Any], marker: str) -> bool:
    messages = request.get("messages")
    return isinstance(messages, list) and any(
        isinstance(message, dict)
        and isinstance(message.get("content"), str)
        and marker in message["content"]
        for message in messages
    )


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
