import json
import socket
from pathlib import Path
from typing import Self
from urllib.request import Request

import pytest
from scripts import verify_local_stack as stack
from scripts.verify_local_stack import (
    StackVerificationError,
    build_checks,
    read_env_file,
    verify_checks,
)


class FakeResponse:
    def __init__(self, payload: bytes, status: int = 200) -> None:
        self.payload = payload
        self.status = status

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, size: int) -> bytes:
        return self.payload[:size]


class FakeSocket:
    def __init__(self, response: bytes = b"+PONG\r\n") -> None:
        self.response = response
        self.sent = b""

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def sendall(self, data: bytes) -> None:
        self.sent += data

    def recv(self, size: int) -> bytes:
        return self.response[:size]


def test_read_env_file_parses_comments_whitespace_and_quotes(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        """
        # local configuration
        FIRST=value
        SECOND = "quoted value"
        THIRD='other value'
        """
    )

    assert read_env_file(env_file) == {
        "FIRST": "value",
        "SECOND": "quoted value",
        "THIRD": "other value",
    }


def test_read_env_file_rejects_invalid_lines(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("not-an-assignment\n")

    with pytest.raises(StackVerificationError) as error:
        read_env_file(env_file)

    assert "KEY=VALUE" in error.value.failures["configuration"]


def test_read_env_file_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(StackVerificationError) as error:
        read_env_file(tmp_path / "missing")

    assert "does not exist" in error.value.failures["configuration"]


def test_read_env_file_rejects_empty_key(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(" =value\n")

    with pytest.raises(StackVerificationError, match="empty key"):
        read_env_file(env_file)


def test_local_http_request_restricts_origin_and_bounds_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[Request] = []

    def open_success(request: Request, *, timeout: float) -> FakeResponse:
        assert timeout == stack.HTTP_TIMEOUT_SECONDS
        captured.append(request)
        return FakeResponse(b'{"status":"ok"}')

    monkeypatch.setattr(stack, "urlopen", open_success)

    assert (
        stack._local_http_request(
            "http://127.0.0.1:4000/health",
            method="POST",
            headers={"X-Test": "value"},
            body=b"{}",
        )
        == b'{"status":"ok"}'
    )
    assert captured[0].get_method() == "POST"

    with pytest.raises(ValueError, match="only permits local HTTP"):
        stack._local_http_request("https://127.0.0.1/health")
    with pytest.raises(ValueError, match="only permits local HTTP"):
        stack._local_http_request("http://example.invalid/health")

    monkeypatch.setattr(
        stack,
        "urlopen",
        lambda *_args, **_kwargs: FakeResponse(b"x" * (stack.MAX_RESPONSE_BYTES + 1)),
    )
    with pytest.raises(ValueError, match="1 MiB"):
        stack._local_http_request("http://localhost/large")

    monkeypatch.setattr(
        stack,
        "urlopen",
        lambda *_args, **_kwargs: FakeResponse(b"unavailable", status=503),
    )
    with pytest.raises(ValueError, match="503"):
        stack._local_http_request("http://localhost/unavailable")


def test_json_object_rejects_non_object() -> None:
    with pytest.raises(TypeError, match="JSON object"):
        stack._json_object(b"[]")


def test_tcp_and_redis_protocol_checks(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = FakeSocket()
    monkeypatch.setattr(
        socket,
        "create_connection",
        lambda *_args, **_kwargs: connection,
    )

    stack._check_tcp("127.0.0.1", 5432)
    stack._check_redis()

    assert connection.sent == b"*1\r\n$4\r\nPING\r\n"

    monkeypatch.setattr(
        socket,
        "create_connection",
        lambda *_args, **_kwargs: FakeSocket(b"-ERR unavailable\r\n"),
    )
    with pytest.raises(ValueError, match="PONG"):
        stack._check_redis()


def test_text_and_grafana_checks_validate_response_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(stack, "_local_http_request", lambda _url: b"ready")
    stack._check_text_endpoint("http://localhost/ready", "ready")

    with pytest.raises(ValueError, match="did not contain"):
        stack._check_text_endpoint("http://localhost/ready", "missing")

    monkeypatch.setattr(
        stack,
        "_local_http_request",
        lambda _url: json.dumps({"database": "ok"}).encode(),
    )
    stack._check_grafana()

    monkeypatch.setattr(
        stack,
        "_local_http_request",
        lambda _url: json.dumps({"database": "failed"}).encode(),
    )
    with pytest.raises(ValueError, match="not ready"):
        stack._check_grafana()


def test_gateway_check_sends_secret_without_logging_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def complete(
        url: str,
        *,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
    ) -> bytes:
        captured.update(url=url, method=method, headers=headers, body=body)
        return json.dumps(
            {"choices": [{"message": {"content": "deterministic fake-primary"}}]}
        ).encode()

    monkeypatch.setattr(stack, "_local_http_request", complete)
    stack._check_gateway_route("super-secret", "coding-default", "fake-primary")

    assert captured["method"] == "POST"
    assert captured["headers"] == {
        "Authorization": "Bearer super-secret",
        "Content-Type": "application/json",
    }
    body = captured["body"]
    assert isinstance(body, bytes)
    assert json.loads(body)["model"] == "coding-default"


@pytest.mark.parametrize(
    ("response", "error_type", "message"),
    [
        ({}, ValueError, "completion choice"),
        ({"choices": [{}]}, TypeError, "assistant text"),
        (
            {"choices": [{"message": {"content": "wrong provider"}}]},
            ValueError,
            "did not reach",
        ),
    ],
)
def test_gateway_check_rejects_malformed_or_misrouted_responses(
    monkeypatch: pytest.MonkeyPatch,
    response: dict[str, object],
    error_type: type[Exception],
    message: str,
) -> None:
    monkeypatch.setattr(
        stack,
        "_local_http_request",
        lambda *_args, **_kwargs: json.dumps(response).encode(),
    )

    with pytest.raises(error_type, match=message):
        stack._check_gateway_route("local-key", "coding-default", "fake-primary")


def test_build_checks_covers_every_dependency_and_fake_route() -> None:
    assert set(build_checks("local-key")) == {
        "postgres",
        "redis",
        "minio",
        "litellm",
        "fake-llm-primary",
        "fake-llm-secondary",
        "prometheus",
        "grafana",
    }


def test_verify_checks_retries_only_pending_checks() -> None:
    attempts = {"ready": 0, "eventual": 0}
    events: list[str] = []

    def ready() -> None:
        attempts["ready"] += 1

    def eventual() -> None:
        attempts["eventual"] += 1
        if attempts["eventual"] == 1:
            raise RuntimeError("not ready")

    verify_checks(
        {"ready": ready, "eventual": eventual},
        timeout_seconds=2,
        retry_interval_seconds=0,
        clock=lambda: 0,
        sleeper=lambda _: None,
        emitter=events.append,
    )

    assert attempts == {"ready": 1, "eventual": 2}
    assert len(events) == 2


def test_verify_checks_returns_structured_bounded_failures() -> None:
    clock_values = iter((0.0, 2.0))

    def fail() -> None:
        raise RuntimeError("x" * 1_000)

    with pytest.raises(StackVerificationError) as error:
        verify_checks(
            {"unhealthy": fail},
            timeout_seconds=1,
            clock=lambda: next(clock_values),
            sleeper=lambda _: None,
            emitter=lambda _: None,
        )

    assert len(error.value.failures["unhealthy"]) == 500


def test_main_uses_env_file_and_emits_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("AGENT_PLATFORM_GATEWAY_API_KEY=local-key\n")
    captured: dict[str, object] = {}

    def verify(
        checks: dict[str, stack.Check],
        *,
        timeout_seconds: float,
    ) -> None:
        captured.update(checks=checks, timeout_seconds=timeout_seconds)

    monkeypatch.setattr(stack, "verify_checks", verify)

    assert stack.main(["--env-file", str(env_file), "--timeout-seconds", "2"]) == 0
    assert captured["timeout_seconds"] == 2
    event = json.loads(capsys.readouterr().out)
    assert event == {"event": "stack.verification.completed", "services": 8}


def test_main_requires_gateway_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("UNRELATED=value\n")
    monkeypatch.delenv("AGENT_PLATFORM_GATEWAY_API_KEY", raising=False)

    with pytest.raises(StackVerificationError, match="GATEWAY_API_KEY"):
        stack.main(["--env-file", str(env_file)])


@pytest.mark.parametrize(
    ("timeout_seconds", "retry_interval_seconds"),
    [(0, 1), (-1, 1), (1, -1)],
)
def test_verify_checks_validates_time_bounds(
    timeout_seconds: float,
    retry_interval_seconds: float,
) -> None:
    with pytest.raises(ValueError):
        verify_checks(
            {},
            timeout_seconds=timeout_seconds,
            retry_interval_seconds=retry_interval_seconds,
        )
