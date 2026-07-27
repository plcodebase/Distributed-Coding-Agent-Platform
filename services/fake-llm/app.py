"""Tiny deterministic OpenAI-compatible upstream used only by integration tests."""

from __future__ import annotations

import json
import os
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode()


class Handler(BaseHTTPRequestHandler):
    server_version = "FakeLLM/1.0"

    def do_GET(self) -> None:
        if self.path == "/health":
            self._write_json(HTTPStatus.OK, {"status": "ok"})
            return
        self._write_json(HTTPStatus.NOT_FOUND, {"error": {"message": "not found"}})

    def do_POST(self) -> None:
        if self.path != "/v1/chat/completions":
            self._write_json(HTTPStatus.NOT_FOUND, {"error": {"message": "not found"}})
            return

        forced_status = int(os.getenv("FAKE_FAILURE_STATUS", "0"))
        if forced_status:
            self._write_json(
                forced_status,
                {
                    "error": {
                        "message": "scripted fake-provider failure",
                        "type": "fake_error",
                    }
                },
            )
            return

        length = int(self.headers.get("Content-Length", "0"))
        try:
            request = json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._write_json(
                HTTPStatus.BAD_REQUEST,
                {"error": {"message": "invalid JSON", "type": "invalid_request_error"}},
            )
            return

        provider = os.getenv("FAKE_PROVIDER", "fake")
        text = os.getenv("FAKE_RESPONSE_TEXT", f"deterministic response from {provider}")
        response_id = f"chatcmpl-{provider}"
        model = str(request.get("model", "fake-model"))
        if bool(request.get("stream")):
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            chunks = (
                {
                    "id": response_id,
                    "object": "chat.completion.chunk",
                    "model": model,
                    "choices": [{"index": 0, "delta": {"role": "assistant"}}],
                },
                {
                    "id": response_id,
                    "object": "chat.completion.chunk",
                    "model": model,
                    "choices": [{"index": 0, "delta": {"content": text}}],
                },
                {
                    "id": response_id,
                    "object": "chat.completion.chunk",
                    "model": model,
                    "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                    "usage": {
                        "prompt_tokens": 1,
                        "completion_tokens": 1,
                        "total_tokens": 2,
                    },
                },
            )
            for chunk in chunks:
                self.wfile.write(b"data: " + _json_bytes(chunk) + b"\n\n")
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            return

        self._write_json(
            HTTPStatus.OK,
            {
                "id": response_id,
                "object": "chat.completion",
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": text},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 1,
                    "completion_tokens": 1,
                    "total_tokens": 2,
                },
            },
        )

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        message = format % args
        sys.stdout.write(
            json.dumps(
                {
                    "event": "http_request",
                    "client": self.client_address[0],
                    "message": message,
                },
                separators=(",", ":"),
            )
            + "\n",
        )
        sys.stdout.flush()

    def _write_json(self, status: int, body: object) -> None:
        payload = _json_bytes(body)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()  # noqa: S104
