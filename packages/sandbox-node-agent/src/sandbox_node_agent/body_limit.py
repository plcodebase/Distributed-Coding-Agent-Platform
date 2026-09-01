"""Bounded request-body middleware for the private node API."""

from __future__ import annotations

import json
from collections import deque
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send


class NodeRequestBodyLimitMiddleware:
    def __init__(self, app: ASGIApp, *, max_body_bytes: int) -> None:
        if type(max_body_bytes) is not int or not 1 <= max_body_bytes <= 150 * 1024 * 1024:
            raise ValueError("max_body_bytes must be an integer in [1, 157286400]")
        self._app = app
        self._limit = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        declared = _content_length(scope)
        if declared is None:
            await _send_error(send, 400, "invalid_content_length")
            return
        if declared > self._limit:
            await _send_error(send, 413, "request_body_limit")
            return
        messages: deque[Message] = deque()
        observed = 0
        while True:
            message = await receive()
            messages.append(message)
            if message["type"] == "http.disconnect":
                break
            if message["type"] != "http.request":
                await _send_error(send, 400, "invalid_request_body")
                return
            observed += len(message.get("body", b""))
            if observed > self._limit:
                await _send_error(send, 413, "request_body_limit")
                return
            if not message.get("more_body", False):
                break

        async def replay() -> Message:
            if messages:
                return messages.popleft()
            # StreamingResponse listens for a real client disconnect after the
            # request body has been consumed. Fabricating one here cancels the
            # response body under ASGI spec versions before 2.4.
            return await receive()

        await self._app(scope, replay, send)


def _content_length(scope: Scope) -> int | None:
    values = [
        value for name, value in scope.get("headers", ()) if name.lower() == b"content-length"
    ]
    if not values:
        return 0
    if len(values) != 1 or not values[0] or not values[0].isdigit():
        return None
    return int(values[0])


async def _send_error(send: Send, status: int, code: str) -> None:
    content = json.dumps(
        {
            "error": {
                "code": code,
                "message": "the node request body is invalid or exceeds its limit",
                "retryable": False,
            }
        },
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


__all__ = ["NodeRequestBodyLimitMiddleware"]
