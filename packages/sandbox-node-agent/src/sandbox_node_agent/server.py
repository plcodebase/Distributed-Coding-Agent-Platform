"""Mutually authenticated TLS server lifecycle for the node-agent ASGI app."""

from __future__ import annotations

import ssl
from typing import TYPE_CHECKING

import uvicorn

if TYPE_CHECKING:
    from fastapi import FastAPI

    from sandbox_node_agent.tls import NodeAgentTlsSettings

_MAX_HOST_BYTES = 255
_MAX_PORT = 65_535


async def serve_node_agent(
    app: FastAPI,
    settings: NodeAgentTlsSettings,
    *,
    host: str = "127.0.0.1",
    port: int = 9443,
) -> None:
    """Serve only TLS 1.3 clients presenting a certificate signed by the private CA."""

    if not host or len(host.encode("utf-8")) > _MAX_HOST_BYTES or "\x00" in host:
        raise ValueError("node-agent host must be bounded canonical text")
    if type(port) is not int or not 1 <= port <= _MAX_PORT:
        raise ValueError("node-agent port must be an integer in [1, 65535]")
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        log_config=None,
        access_log=False,
        server_header=False,
        date_header=False,
        proxy_headers=False,
        timeout_keep_alive=5,
        limit_concurrency=1024,
        ssl_ca_certs=settings.ca_file,
        ssl_certfile=settings.certificate_file,
        ssl_keyfile=settings.private_key_file,
        ssl_cert_reqs=ssl.CERT_REQUIRED,
    )
    config.load()
    if config.ssl is None:
        raise RuntimeError("node-agent TLS context was not created")
    config.ssl.minimum_version = ssl.TLSVersion.TLSv1_3
    config.ssl.verify_mode = ssl.CERT_REQUIRED
    await uvicorn.Server(config).serve()


__all__ = ["serve_node_agent"]
