from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

if TYPE_CHECKING:
    from types import ModuleType


def _load_coding_scenario() -> ModuleType:
    scenario_path = Path(__file__).parents[2] / "services" / "fake-llm" / "coding_scenario.py"
    spec = importlib.util.spec_from_file_location("fake_llm_coding_scenario", scenario_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _request(*tool_call_ids: str) -> dict[str, Any]:
    return {
        "model": "coding-default",
        "messages": [
            {
                "role": "user",
                "content": "[fixture:calculator-bug-v1] Fix and verify the sample.",
            },
            *(
                {
                    "role": "tool",
                    "tool_call_id": tool_call_id,
                    "content": '{"ok":true,"result":{}}',
                }
                for tool_call_id in tool_call_ids
            ),
        ],
    }


def _retry_request(*tool_call_ids: str) -> dict[str, Any]:
    request = _request(*tool_call_ids)
    request["messages"][0]["content"] = "[fixture:calculator-retry-v1] Fix and verify."
    for message in request["messages"]:
        if message.get("tool_call_id") == "e2e-retry-tests-fail":
            message["content"] = (
                '{"error":{"code":"command_failed","message":"verification failed"},"ok":false}'
            )
    return request


def test_coding_scenario_matches_the_checked_in_fixture() -> None:
    scenario = _load_coding_scenario()
    fixture = (
        Path(__file__).parents[1] / "end_to_end" / "fixtures" / "calculator_bug" / "calculator.py"
    ).read_bytes()

    assert hashlib.sha256(fixture).hexdigest() == scenario.BROKEN_CALCULATOR_SHA256
    assert scenario.is_request(_request()) is True
    assert scenario.is_request({"messages": []}) is False
    containerfile = (
        Path(__file__).parents[2] / "services" / "fake-llm" / "Containerfile"
    ).read_text(encoding="utf-8")
    assert "COPY --chown=fake:fake coding_scenario.py /app/coding_scenario.py" in containerfile


def test_coding_scenario_emits_the_expected_bounded_tool_sequence() -> None:
    scenario = _load_coding_scenario()

    actions = [
        scenario.next_action(_request()),
        scenario.next_action(_request("e2e-read-calculator")),
        scenario.next_action(_request("e2e-read-calculator", "e2e-read-test")),
        scenario.next_action(
            _request(
                "e2e-read-calculator",
                "e2e-read-test",
                "e2e-edit-calculator",
            )
        ),
    ]

    assert [(action[0], action[1]) for action in actions] == [
        ("e2e-read-calculator", "read_file"),
        ("e2e-read-test", "read_file"),
        ("e2e-edit-calculator", "edit_file"),
        ("e2e-run-tests", "run_command"),
    ]
    edit_arguments = cast("tuple[str, str, dict[str, object]]", actions[2])[2]
    assert edit_arguments["expected_sha256"] == scenario.BROKEN_CALCULATOR_SHA256
    final = scenario.next_action(
        _request(
            "e2e-read-calculator",
            "e2e-read-test",
            "e2e-edit-calculator",
            "e2e-run-tests",
        )
    )
    assert isinstance(final, str)


def test_coding_scenario_streams_fragmented_canonical_arguments() -> None:
    scenario = _load_coding_scenario()
    chunks = scenario.tool_chunks(
        response_id="chatcmpl-test",
        model="coding-default",
        call_id="call-1",
        tool_name="read_file",
        arguments={"path": "calculator.py"},
    )

    first_function = chunks[0]["choices"][0]["delta"]["tool_calls"][0]["function"]
    second_function = chunks[1]["choices"][0]["delta"]["tool_calls"][0]["function"]
    assert first_function["arguments"] + second_function["arguments"] == (
        '{"path":"calculator.py"}'
    )
    assert chunks[-1]["choices"][0]["finish_reason"] == "tool_calls"


def test_coding_retry_scenario_requires_failure_before_correction() -> None:
    scenario = _load_coding_scenario()
    fixture = (
        Path(__file__).parents[1] / "end_to_end" / "fixtures" / "calculator_bug" / "calculator.py"
    ).read_text(encoding="utf-8")
    incorrect = fixture.replace("return left - right", "return left * right").encode()
    assert hashlib.sha256(incorrect).hexdigest() == scenario.INCORRECT_CALCULATOR_SHA256
    assert scenario.is_request(_retry_request()) is True

    sequence = (
        "e2e-retry-read-calculator",
        "e2e-retry-read-test",
        "e2e-retry-edit-incorrect",
        "e2e-retry-tests-fail",
        "e2e-retry-edit-correct",
        "e2e-retry-tests-pass",
    )
    actions = [scenario.next_action(_retry_request(*sequence[:index])) for index in range(6)]
    assert [(action[0], action[1]) for action in actions] == [
        ("e2e-retry-read-calculator", "read_file"),
        ("e2e-retry-read-test", "read_file"),
        ("e2e-retry-edit-incorrect", "edit_file"),
        ("e2e-retry-tests-fail", "run_command"),
        ("e2e-retry-edit-correct", "edit_file"),
        ("e2e-retry-tests-pass", "run_command"),
    ]
    corrective_arguments = cast("tuple[str, str, dict[str, object]]", actions[4])[2]
    assert corrective_arguments["expected_sha256"] == scenario.INCORRECT_CALCULATOR_SHA256
    assert isinstance(scenario.next_action(_retry_request(*sequence)), str)

    unexpected_success = _retry_request(*sequence[:4])
    unexpected_success["messages"][-1]["content"] = '{"ok":true,"result":{}}'
    with pytest.raises(ValueError, match="retry scenario"):
        scenario.next_action(unexpected_success)


def test_coding_scenario_fails_closed_on_bad_or_out_of_order_feedback() -> None:
    scenario = _load_coding_scenario()
    failed = _request("e2e-read-calculator")
    failed["messages"][-1]["content"] = '{"ok":false}'

    with pytest.raises(ValueError, match="did not succeed"):
        scenario.next_action(failed)
    with pytest.raises(ValueError, match="unexpected tool-result sequence"):
        scenario.next_action(_request("e2e-read-test"))
