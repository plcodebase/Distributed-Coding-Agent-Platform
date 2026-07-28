"""Bounded runtime verification for the local Podman dependency stack."""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from http import HTTPStatus
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

MAX_RESPONSE_BYTES = 1_048_576
HTTP_TIMEOUT_SECONDS = 3.0
LOCAL_HTTP_HOSTS = frozenset({"127.0.0.1", "localhost"})

type Check = Callable[[], None]
type Clock = Callable[[], float]
type Sleeper = Callable[[float], None]
type Emitter = Callable[[str], None]


class StackVerificationError(RuntimeError):
    """Raised with bounded per-service failures when the stack is not ready."""

    def __init__(self, failures: Mapping[str, str]) -> None:
        self.failures = dict(failures)
        summary = "; ".join(f"{name}: {message}" for name, message in self.failures.items())
        super().__init__(f"local stack verification failed: {summary}")


def read_env_file(path: Path) -> dict[str, str]:
    """Read the simple KEY=VALUE subset used by the local environment file."""

    if not path.is_file():
        raise StackVerificationError({"configuration": f"{path} does not exist"})

    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise StackVerificationError(
                {"configuration": f"{path}:{line_number} must use KEY=VALUE syntax"}
            )
        key, value = line.split("=", maxsplit=1)
        key = key.strip()
        if not key:
            raise StackVerificationError(
                {"configuration": f"{path}:{line_number} has an empty key"}
            )
        values[key] = value.strip().strip("\"'")
    return values


def _emit(event: str, **fields: object) -> None:
    _write_line(json.dumps({"event": event, **fields}, separators=(",", ":"), sort_keys=True))


def _write_line(value: str) -> None:
    sys.stdout.write(value + "\n")
    sys.stdout.flush()


def _local_http_request(
    url: str,
    *,
    method: str = "GET",
    headers: Mapping[str, str] | None = None,
    body: bytes | None = None,
) -> bytes:
    parsed = urlsplit(url)
    if parsed.scheme != "http" or parsed.hostname not in LOCAL_HTTP_HOSTS:
        raise ValueError("stack verifier only permits local HTTP endpoints")

    request = Request(  # noqa: S310 - the URL is restricted to local HTTP above
        url,
        data=body,
        headers=dict(headers or {}),
        method=method,
    )
    with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:  # noqa: S310
        payload = cast("bytes", response.read(MAX_RESPONSE_BYTES + 1))
        if len(payload) > MAX_RESPONSE_BYTES:
            raise ValueError("response exceeded the 1 MiB verification limit")
        if response.status != HTTPStatus.OK:
            raise ValueError(f"unexpected HTTP status {response.status}")
        return payload


def _json_object(payload: bytes) -> dict[str, Any]:
    parsed: object = json.loads(payload)
    if not isinstance(parsed, dict):
        raise TypeError("expected a JSON object")
    return cast("dict[str, Any]", parsed)


def _check_tcp(host: str, port: int) -> None:
    with socket.create_connection((host, port), timeout=HTTP_TIMEOUT_SECONDS):
        return


def _check_redis() -> None:
    with socket.create_connection(("127.0.0.1", 6379), timeout=HTTP_TIMEOUT_SECONDS) as client:
        client.sendall(b"*1\r\n$4\r\nPING\r\n")
        response = client.recv(64)
    if response != b"+PONG\r\n":
        raise ValueError("Redis did not return PONG")


def _check_text_endpoint(url: str, expected_text: str) -> None:
    response = _local_http_request(url).decode(errors="replace")
    if expected_text.casefold() not in response.casefold():
        raise ValueError(f"response did not contain {expected_text!r}")


def _check_grafana() -> None:
    response = _json_object(_local_http_request("http://127.0.0.1:3000/api/health"))
    if response.get("database") != "ok":
        raise ValueError("Grafana database is not ready")


def _check_gateway_route(gateway_key: str, route: str, expected_provider: str) -> None:
    payload = json.dumps(
        {
            "model": route,
            "messages": [{"role": "user", "content": "sequence-1 smoke test"}],
            "max_tokens": 32,
            "stream": False,
        },
        separators=(",", ":"),
    ).encode()
    response = _json_object(
        _local_http_request(
            "http://127.0.0.1:4000/v1/chat/completions",
            method="POST",
            headers={
                "Authorization": f"Bearer {gateway_key}",
                "Content-Type": "application/json",
            },
            body=payload,
        )
    )

    choices = response.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        raise ValueError("gateway response did not contain a completion choice")
    message = choices[0].get("message")
    if not isinstance(message, dict) or not isinstance(message.get("content"), str):
        raise TypeError("gateway response did not contain assistant text")
    if expected_provider not in message["content"]:
        raise ValueError(f"gateway route did not reach {expected_provider}")


def build_checks(gateway_key: str) -> dict[str, Check]:
    """Build deterministic checks for every sequence-1 dependency."""

    return {
        "postgres": lambda: _check_tcp("127.0.0.1", 5432),
        "redis": _check_redis,
        "minio": lambda: _check_text_endpoint("http://127.0.0.1:9000/minio/health/live", ""),
        "litellm": lambda: _check_text_endpoint("http://127.0.0.1:4000/health/liveliness", ""),
        "fake-llm-primary": lambda: _check_gateway_route(
            gateway_key, "coding-default", "fake-primary"
        ),
        "fake-llm-secondary": lambda: _check_gateway_route(
            gateway_key, "coding-strong", "fake-secondary"
        ),
        "prometheus": lambda: _check_text_endpoint("http://127.0.0.1:9090/-/ready", "ready"),
        "grafana": _check_grafana,
    }


def verify_checks(
    checks: Mapping[str, Check],
    *,
    timeout_seconds: float,
    retry_interval_seconds: float = 1.0,
    clock: Clock = time.monotonic,
    sleeper: Sleeper = time.sleep,
    emitter: Emitter = _write_line,
) -> None:
    """Retry independent checks until all pass or the shared deadline expires."""

    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if retry_interval_seconds < 0:
        raise ValueError("retry_interval_seconds may not be negative")

    deadline = clock() + timeout_seconds
    pending = dict(checks)
    failures: dict[str, str] = {}
    while pending:
        for name, check in tuple(pending.items()):
            try:
                check()
            except Exception as error:
                failures[name] = str(error)[:500]
            else:
                pending.pop(name)
                failures.pop(name, None)
                emitter(
                    json.dumps(
                        {"event": "stack.check.passed", "service": name},
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                )

        if not pending:
            return
        if clock() >= deadline:
            raise StackVerificationError(
                {name: failures.get(name, "check did not complete") for name in pending}
            )
        sleeper(retry_interval_seconds)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    values = read_env_file(args.env_file)
    gateway_key = os.getenv("AGENT_PLATFORM_GATEWAY_API_KEY") or values.get(
        "AGENT_PLATFORM_GATEWAY_API_KEY"
    )
    if not gateway_key:
        raise StackVerificationError(
            {"configuration": "AGENT_PLATFORM_GATEWAY_API_KEY is required"}
        )

    checks = build_checks(gateway_key)
    verify_checks(checks, timeout_seconds=args.timeout_seconds)
    _emit("stack.verification.completed", services=len(checks))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
