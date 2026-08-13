"""Small bounded HTTP server for workload health, metrics, and local draining."""

from __future__ import annotations

import asyncio
import inspect
import ipaddress
from contextlib import suppress
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

type HealthCheck = Callable[[], bool]
type MetricsRenderer = Callable[[], bytes]
type DrainCallback = Callable[[], Awaitable[None] | None]

DEFAULT_MAX_REQUEST_BYTES = 8 * 1024
DEFAULT_REQUEST_TIMEOUT_SECONDS = 2.0
MAX_METRICS_BYTES = 8 * 1024 * 1024
MAX_PORT = 65_535
MIN_REQUEST_BYTES = 256
MAX_REQUEST_BYTES = 65_536
MIN_REQUEST_TIMEOUT_SECONDS = 0.1
MAX_REQUEST_TIMEOUT_SECONDS = 30
HTTP_REQUEST_PARTS = 3


@dataclass(frozen=True, slots=True)
class OperationsServerSettings:
    """Validated listener and request bounds for one operations server."""

    host: str = "127.0.0.1"
    port: int = 9090
    max_request_bytes: int = DEFAULT_MAX_REQUEST_BYTES
    request_timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        try:
            ipaddress.ip_address(self.host)
        except ValueError as error:
            raise ValueError("operations host must be an IP address") from error
        if type(self.port) is not int or not 0 <= self.port <= MAX_PORT:
            raise ValueError("operations port must be in [0, 65535]")
        if type(self.max_request_bytes) is not int or not (
            MIN_REQUEST_BYTES <= self.max_request_bytes <= MAX_REQUEST_BYTES
        ):
            raise ValueError("max_request_bytes must be in [256, 65536]")
        if not (
            MIN_REQUEST_TIMEOUT_SECONDS
            <= self.request_timeout_seconds
            <= MAX_REQUEST_TIMEOUT_SECONDS
        ):
            raise ValueError("request_timeout_seconds must be in [0.1, 30]")


class OperationsServer:
    """Serve four fixed endpoints without an application-framework dependency."""

    def __init__(
        self,
        *,
        live: HealthCheck,
        ready: HealthCheck,
        metrics: MetricsRenderer,
        drain: DrainCallback | None = None,
        settings: OperationsServerSettings | None = None,
    ) -> None:
        for name, callback in (("live", live), ("ready", ready), ("metrics", metrics)):
            if not callable(callback):
                raise TypeError(f"{name} callback must be callable")
        if drain is not None and not callable(drain):
            raise TypeError("drain callback must be callable")
        self._live = live
        self._ready = ready
        self._metrics = metrics
        self._drain = drain
        self._settings = settings or OperationsServerSettings()
        self._server: asyncio.Server | None = None
        self._connections: set[asyncio.StreamWriter] = set()
        self._drain_requested = False
        self._drain_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()

    @property
    def port(self) -> int:
        if self._server is None or not self._server.sockets:
            raise RuntimeError("operations server has not started")
        return int(self._server.sockets[0].getsockname()[1])

    async def start(self) -> None:
        if self._server is not None:
            raise RuntimeError("operations server is already started")
        self._server = await asyncio.start_server(
            self._handle,
            host=self._settings.host,
            port=self._settings.port,
            limit=self._settings.max_request_bytes,
            start_serving=True,
        )

    async def aclose(self) -> None:
        async with self._close_lock:
            server = self._server
            if server is None:
                return
            self._server = None
            server.close()
            await server.wait_closed()
            writers = tuple(self._connections)
            for writer in writers:
                writer.close()
            for writer in writers:
                with suppress(Exception):
                    await writer.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._connections.add(writer)
        try:
            try:
                request = await asyncio.wait_for(
                    reader.readuntil(b"\r\n\r\n"),
                    timeout=self._settings.request_timeout_seconds,
                )
            except (TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
                await self._respond(writer, HTTPStatus.BAD_REQUEST, b"invalid request\n")
                return
            if len(request) > self._settings.max_request_bytes:
                await self._respond(
                    writer, HTTPStatus.REQUEST_HEADER_FIELDS_TOO_LARGE, b"too large\n"
                )
                return
            method, path = self._parse_request(request)
            if method is None:
                await self._respond(writer, HTTPStatus.BAD_REQUEST, b"invalid request\n")
            elif method == "GET" and path == "/live":
                await self._health(writer, self._live())
            elif method == "GET" and path == "/ready":
                await self._health(writer, not self._drain_requested and self._ready())
            elif method == "GET" and path == "/metrics":
                payload = self._metrics()
                if not isinstance(payload, bytes) or len(payload) > MAX_METRICS_BYTES:
                    await self._respond(
                        writer, HTTPStatus.INTERNAL_SERVER_ERROR, b"metrics unavailable\n"
                    )
                else:
                    await self._respond(
                        writer,
                        HTTPStatus.OK,
                        payload,
                        content_type="text/plain; version=0.0.4; charset=utf-8",
                    )
            elif method == "POST" and path == "/drain":
                await self._request_drain(writer)
            else:
                await self._respond(writer, HTTPStatus.NOT_FOUND, b"not found\n")
        except Exception:
            with suppress(Exception):
                await self._respond(writer, HTTPStatus.INTERNAL_SERVER_ERROR, b"internal error\n")
        finally:
            self._connections.discard(writer)
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()

    @staticmethod
    def _parse_request(  # noqa: PLR0911 - each malformed framing branch fails closed
        request: bytes,
    ) -> tuple[str | None, str | None]:
        try:
            header = request.decode("ascii")
        except UnicodeDecodeError:
            return None, None
        lines = header.split("\r\n")
        parts = lines[0].split(" ")
        if len(parts) != HTTP_REQUEST_PARTS or parts[2] != "HTTP/1.1":
            return None, None
        headers: dict[str, str] = {}
        for line in lines[1:]:
            if not line:
                continue
            if line.startswith((" ", "\t")) or ":" not in line:
                return None, None
            name, value = line.split(":", 1)
            normalized_name = name.casefold()
            if (
                not name
                or any(character.isspace() for character in name)
                or normalized_name in headers
            ):
                return None, None
            headers[normalized_name] = value.strip()
        if "transfer-encoding" in headers:
            return None, None
        if headers.get("content-length", "0") != "0" or "host" not in headers:
            return None, None
        method, path, _ = parts
        if method not in {"GET", "POST"} or not path.startswith("/") or "?" in path:
            return None, None
        return method, path

    async def _request_drain(self, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        try:
            loopback = isinstance(peer, tuple) and ipaddress.ip_address(str(peer[0])).is_loopback
        except ValueError:
            loopback = False
        if not loopback or self._drain is None:
            await self._respond(writer, HTTPStatus.FORBIDDEN, b"forbidden\n")
            return
        async with self._drain_lock:
            if not self._drain_requested:
                try:
                    outcome = self._drain()
                    if inspect.isawaitable(outcome):
                        await outcome
                except Exception:
                    self._drain_requested = False
                    await self._respond(
                        writer,
                        HTTPStatus.INTERNAL_SERVER_ERROR,
                        b"drain failed\n",
                    )
                    return
                self._drain_requested = True
        await self._respond(writer, HTTPStatus.ACCEPTED, b"draining\n")

    async def _health(self, writer: asyncio.StreamWriter, healthy: bool) -> None:
        await self._respond(
            writer,
            HTTPStatus.OK if healthy else HTTPStatus.SERVICE_UNAVAILABLE,
            b"ok\n" if healthy else b"unavailable\n",
        )

    @staticmethod
    async def _respond(
        writer: asyncio.StreamWriter,
        status: HTTPStatus,
        body: bytes,
        *,
        content_type: str = "text/plain; charset=utf-8",
    ) -> None:
        reason = status.phrase.encode("ascii")
        headers = (
            b"HTTP/1.1 "
            + str(status.value).encode("ascii")
            + b" "
            + reason
            + b"\r\nContent-Type: "
            + content_type.encode("ascii")
            + b"\r\nContent-Length: "
            + str(len(body)).encode("ascii")
            + b"\r\nConnection: close\r\n\r\n"
        )
        writer.write(headers + body)
        await writer.drain()


__all__ = ["OperationsServer", "OperationsServerSettings"]
