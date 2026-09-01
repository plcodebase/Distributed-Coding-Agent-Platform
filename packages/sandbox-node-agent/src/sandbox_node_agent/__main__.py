"""Production mTLS sandbox node-agent process entry point."""

from __future__ import annotations

import asyncio

from sandbox_node_agent.production import ProductionNodeSettings, create_production_node_app
from sandbox_node_agent.server import serve_node_agent


async def _serve() -> None:
    settings = ProductionNodeSettings()
    app = await create_production_node_app(node_settings=settings)
    await serve_node_agent(
        app,
        settings.tls_settings(),
        host=settings.listen_host,
        port=settings.listen_port,
    )


if __name__ == "__main__":
    asyncio.run(_serve())
