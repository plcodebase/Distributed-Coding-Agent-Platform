from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from platform_telemetry import OperationsServer, OperationsServerSettings

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@pytest.fixture
async def server() -> AsyncIterator[tuple[OperationsServer, list[str]]]:
    drains: list[str] = []
    operations = OperationsServer(
        live=lambda: True,
        ready=lambda: True,
        metrics=lambda: b"metric_name 1\n",
        drain=lambda: drains.append("drain"),
        settings=OperationsServerSettings(host="127.0.0.1", port=0),
    )
    await operations.start()
    try:
        yield operations, drains
    finally:
        await operations.aclose()


@pytest.mark.asyncio
async def test_operations_server_health_metrics_and_unknown_route(
    server: tuple[OperationsServer, list[str]],
) -> None:
    operations, _ = server

    assert await _request(operations.port, "GET", "/live") == (200, b"ok\n")
    assert await _request(operations.port, "GET", "/ready") == (200, b"ok\n")
    assert await _request(operations.port, "GET", "/metrics") == (
        200,
        b"metric_name 1\n",
    )
    assert await _request(operations.port, "GET", "/missing") == (404, b"not found\n")


@pytest.mark.asyncio
async def test_operations_server_drain_is_idempotent_and_removes_readiness(
    server: tuple[OperationsServer, list[str]],
) -> None:
    operations, drains = server

    first, second = await asyncio.gather(
        _request(operations.port, "POST", "/drain"),
        _request(operations.port, "POST", "/drain"),
    )

    assert first == (202, b"draining\n")
    assert second == (202, b"draining\n")
    assert drains == ["drain"]
    assert await _request(operations.port, "GET", "/ready") == (503, b"unavailable\n")
    assert await _request(operations.port, "GET", "/live") == (200, b"ok\n")


@pytest.mark.asyncio
async def test_operations_server_rejects_bodies_and_malformed_requests(
    server: tuple[OperationsServer, list[str]],
) -> None:
    operations, drains = server

    status, _ = await _raw_request(
        operations.port,
        b"POST /drain HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1\r\n\r\nx",
    )
    assert status == 400
    status, _ = await _raw_request(operations.port, b"invalid\r\n\r\n")
    assert status == 400
    for payload in (
        b"GET /ready HTTP/1.1\r\nHost: one\r\nHost: two\r\n\r\n",
        b"GET /ready HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n",
        b"GET /ready HTTP/1.1\r\nContent-Length: 0\r\n\r\n",
        b"GET /ready HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n",
    ):
        status, _ = await _raw_request(operations.port, payload)
        assert status == 400
    assert drains == []


@pytest.mark.asyncio
async def test_operations_server_failed_drain_remains_ready_and_can_retry() -> None:
    calls = 0

    async def drain() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("opaque failure")

    operations = OperationsServer(
        live=lambda: True,
        ready=lambda: True,
        metrics=lambda: b"",
        drain=drain,
        settings=OperationsServerSettings(port=0),
    )
    await operations.start()
    try:
        assert await _request(operations.port, "POST", "/drain") == (
            500,
            b"drain failed\n",
        )
        assert await _request(operations.port, "GET", "/ready") == (200, b"ok\n")
        assert await _request(operations.port, "POST", "/drain") == (
            202,
            b"draining\n",
        )
        assert calls == 2
    finally:
        await operations.aclose()


@pytest.mark.asyncio
async def test_operations_server_validates_configuration_and_lifecycle() -> None:
    with pytest.raises(ValueError, match="IP address"):
        OperationsServerSettings(host="localhost")
    with pytest.raises(ValueError, match="port"):
        OperationsServerSettings(port=-1)
    with pytest.raises(TypeError, match="live callback"):
        OperationsServer(live=None, ready=lambda: True, metrics=lambda: b"")  # type: ignore[arg-type]

    operations = OperationsServer(
        live=lambda: True,
        ready=lambda: False,
        metrics=lambda: b"",
        settings=OperationsServerSettings(port=0),
    )
    with pytest.raises(RuntimeError, match="has not started"):
        _ = operations.port
    await operations.start()
    with pytest.raises(RuntimeError, match="already started"):
        await operations.start()
    assert await _request(operations.port, "GET", "/ready") == (503, b"unavailable\n")
    assert await _request(operations.port, "POST", "/drain") == (403, b"forbidden\n")
    await operations.aclose()
    await operations.aclose()


async def _request(port: int, method: str, path: str) -> tuple[int, bytes]:
    return await _raw_request(
        port,
        f"{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n".encode(),
    )


async def _raw_request(port: int, payload: bytes) -> tuple[int, bytes]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(payload)
    await writer.drain()
    response = await reader.read()
    writer.close()
    await writer.wait_closed()
    header, body = response.split(b"\r\n\r\n", 1)
    status = int(header.split(b" ", 2)[1])
    return status, body
