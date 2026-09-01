"""Real-TCP event replay, reconnect, and server-lifecycle integration coverage."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Self

import httpx
import pytest
import websockets
from _network_event_app import OTHER_TOKEN, RUN_ID, TOKEN
from websockets.exceptions import ConnectionClosedError, InvalidStatus

pytestmark = pytest.mark.integration
ROOT = Path(__file__).parents[2]


def _available_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


@dataclass(slots=True)
class _NetworkServer:
    process: asyncio.subprocess.Process
    port: int
    marker: Path

    @classmethod
    async def start(cls, root: Path, *, port: int, gap: bool = False) -> Self:
        marker = root / ("gap-marker.log" if gap else "event-marker.log")
        environment = {
            "AGENT_EVENT_TEST_MARKER": str(marker),
            "AGENT_EVENT_TEST_MODE": "gap" if gap else "normal",
            "LANG": "C.UTF-8",
            "PATH": os.defpath,
            "PYTHONPATH": str(ROOT),
        }
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "uvicorn",
            "tests.integration._network_event_app:create_app",
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
            "--no-access-log",
            cwd=ROOT,
            env=environment,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        server = cls(process=process, port=port, marker=marker)
        await server._wait_ready()
        return server

    @property
    def http_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def websocket_url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    async def stop(self) -> None:
        if self.process.returncode is not None:
            return
        self.process.terminate()
        try:
            await asyncio.wait_for(self.process.wait(), timeout=10)
        except TimeoutError:
            self.process.kill()
            await self.process.wait()

    async def _wait_ready(self) -> None:
        async with httpx.AsyncClient(timeout=0.25, trust_env=False) as client:
            for _ in range(100):
                if self.process.returncode is not None:
                    raise AssertionError(
                        f"event gateway exited during startup: {self.process.returncode}"
                    )
                try:
                    response = await client.get(f"{self.http_url}/health/live")
                except httpx.HTTPError:
                    await asyncio.sleep(0.05)
                    continue
                if response.status_code == 200:
                    return
                await asyncio.sleep(0.05)
        raise AssertionError("event gateway did not become live")


async def _wait_for_marker(path: Path, marker: str) -> None:
    for _ in range(100):
        if await asyncio.to_thread(_marker_contains, path, marker):
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"event gateway marker was not observed: {marker}")


def _marker_contains(path: Path, marker: str) -> bool:
    return path.exists() and marker in path.read_text(encoding="utf-8")


def _authorization(token: str = TOKEN) -> dict[str, str]:
    return {"authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_real_network_replay_disconnect_and_process_restart(tmp_path: Path) -> None:
    port = _available_port()
    first = await _NetworkServer.start(tmp_path, port=port)
    try:
        async with httpx.AsyncClient(base_url=first.http_url, trust_env=False) as client:
            page = await client.get(
                f"/v1/runs/{RUN_ID}/events?after=1&limit=2",
                headers=_authorization(),
            )
            assert page.status_code == 200
            assert [event["sequence"] for event in page.json()["events"]] == [2, 3]
            assert page.json()["next_after"] == 3

        async with websockets.connect(
            f"{first.websocket_url}/v1/runs/{RUN_ID}/stream",
            additional_headers=_authorization(),
            proxy=None,
        ) as websocket:
            first_event = json.loads(await websocket.recv())
            assert first_event["sequence"] == 1
        await _wait_for_marker(first.marker, "stream-closed")
    finally:
        await first.stop()
    await _wait_for_marker(first.marker, "application-closed")

    restarted = await _NetworkServer.start(tmp_path, port=port)
    try:
        async with websockets.connect(
            f"{restarted.websocket_url}/v1/runs/{RUN_ID}/stream?after=1",
            additional_headers=_authorization(),
            proxy=None,
        ) as websocket:
            assert json.loads(await websocket.recv())["sequence"] == 2
            assert json.loads(await websocket.recv())["sequence"] == 3
    finally:
        await restarted.stop()


@pytest.mark.asyncio
async def test_real_network_auth_body_limit_gap_and_shutdown(tmp_path: Path) -> None:
    server = await _NetworkServer.start(tmp_path, port=_available_port(), gap=True)
    try:
        async with httpx.AsyncClient(base_url=server.http_url, trust_env=False) as client:
            unauthenticated = await client.get(f"/v1/runs/{RUN_ID}/events")
            cross_tenant = await client.get(
                f"/v1/runs/{RUN_ID}/events",
                headers=_authorization(OTHER_TOKEN),
            )
            oversized = await client.post(
                f"/v1/runs/{RUN_ID}/events",
                headers={**_authorization(), "content-type": "application/json"},
                content=b"x" * 129,
            )
        assert unauthenticated.status_code == 401
        assert cross_tenant.status_code == 404
        assert oversized.status_code == 413
        assert oversized.json()["error"]["code"] == "request_body_limit"

        with pytest.raises(InvalidStatus) as rejected:
            async with websockets.connect(
                f"{server.websocket_url}/v1/runs/{RUN_ID}/stream",
                proxy=None,
            ):
                pass
        assert rejected.value.response.status_code == 403

        async with websockets.connect(
            f"{server.websocket_url}/v1/runs/{RUN_ID}/stream",
            additional_headers=_authorization(),
            proxy=None,
        ) as websocket:
            assert json.loads(await websocket.recv())["sequence"] == 1
            with pytest.raises(ConnectionClosedError) as gap:
                await websocket.recv()
            assert gap.value.rcvd is not None
            assert gap.value.rcvd.code == 1011
        await _wait_for_marker(server.marker, "stream-closed")
    finally:
        await server.stop()
    await _wait_for_marker(server.marker, "application-closed")
