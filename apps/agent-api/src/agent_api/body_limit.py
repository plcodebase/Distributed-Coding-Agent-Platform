"""A bounded ASGI request-body boundary for the control-plane API."""

from __future__ import annotations

import json
from collections import deque
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

MAX_HTTP_REQUEST_BODY_BYTES = 64 * 1024
MAX_CONFIGURED_HTTP_REQUEST_BODY_BYTES = 1024 * 1024


class RequestBodyLimitMiddleware:
    """Reject oversized HTTP bodies before application parsing or routing."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        max_body_bytes: int = MAX_HTTP_REQUEST_BODY_BYTES,
    ) -> None:
        if (
            type(max_body_bytes) is not int
            or not 1 <= max_body_bytes <= MAX_CONFIGURED_HTTP_REQUEST_BODY_BYTES
        ):
            raise ValueError(
                "max_body_bytes must be an integer in "
                f"[1, {MAX_CONFIGURED_HTTP_REQUEST_BODY_BYTES}]"
            )
        self._app = app
        self._max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        content_length = _content_length(scope)
        if content_length is None:
            await _send_error(
                send,
                status=400,
                code="invalid_content_length",
                message="the Content-Length header is invalid",
            )
            return
        has_declared_length, declared_length = content_length
        if declared_length > self._max_body_bytes:
            await _send_limit_error(send)
            return

        messages: deque[Message] = deque()
        received_bytes = 0
        while True:
            message = await receive()
            messages.append(message)
            if message["type"] == "http.disconnect":
                break
            if message["type"] != "http.request":
                await _send_error(
                    send,
                    status=400,
                    code="invalid_request_body",
                    message="the request body stream is invalid",
                )
                return
            received_bytes += len(message.get("body", b""))
            if received_bytes > self._max_body_bytes:
                await _send_limit_error(send)
                return
            if not message.get("more_body", False):
                break
        if has_declared_length and received_bytes != declared_length:
            await _send_error(
                send,
                status=400,
                code="invalid_content_length",
                message="the Content-Length header does not match the request body",
            )
            return

        async def replay() -> Message:
            if messages:
                return messages.popleft()
            return {"type": "http.request", "body": b"", "more_body": False}

        await self._app(scope, replay, send)


def _content_length(scope: Scope) -> tuple[bool, int] | None:
    values = [
        value for name, value in scope.get("headers", ()) if name.lower() == b"content-length"
    ]
    if not values:
        return False, 0
    if len(values) != 1:
        return None
    raw = values[0]
    if not raw or not raw.isdigit():
        return None
    return True, int(raw)


async def _send_limit_error(send: Send) -> None:
    await _send_error(
        send,
        status=413,
        code="request_body_limit",
        message="the request body exceeds the configured byte limit",
    )


async def _send_error(
    send: Send,
    *,
    status: int,
    code: str,
    message: str,
) -> None:
    content = json.dumps(
        {
            "error": {
                "code": code,
                "message": message,
                "retryable": False,
                "details": {},
            }
        },
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": (
                (b"content-type", b"application/json"),
                (b"content-length", str(len(content)).encode("ascii")),
            ),
        }
    )
    await send({"type": "http.response.body", "body": content})


__all__ = [
    "MAX_CONFIGURED_HTTP_REQUEST_BODY_BYTES",
    "MAX_HTTP_REQUEST_BODY_BYTES",
    "RequestBodyLimitMiddleware",
]
