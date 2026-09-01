"""Command-line client for typed event rendering and local agent development."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

from agent_cli.renderer import iter_rendered_events, render_event
from agent_core.domain.errors import DomainOperationError
from agent_core.events import RunFailedEvent
from agent_core.local_session import InMemoryAgentSession
from agent_core.loop import (
    AgentLoop,
    AgentLoopConfig,
    TranscriptJournal,
    UtcClock,
    UuidIdGenerator,
)
from agent_core.settings import PlatformSettings
from agents_sdk_adapter import OpenAIAgentsGateway, create_openai_compatible_agents_gateway
from gateway_client import GatewayClient, GatewayClientConfig
from platform_telemetry import Redactor
from sandbox_runtime import (
    GitWorktreeManager,
    InMemoryCheckpointCoordinator,
    PodmanSandbox,
    PodmanSandboxConfig,
    WorkspaceToolset,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import TextIO

_DEFAULT_SANDBOX_IMAGE = "localhost/agent-platform-sandbox:sequence-10"
_SYSTEM_INSTRUCTIONS = (
    "You are a coding agent operating in an isolated Git worktree. Inspect before editing, "
    "validate changes, use only registered tools, and report only observed results."
)


def main(arguments: Sequence[str] | None = None) -> None:
    """Parse command-line arguments and exit with a stable process status."""

    parser = _parser()
    parsed = parser.parse_args(arguments)
    try:
        if parsed.command == "render":
            for line in iter_rendered_events(sys.stdin.buffer):
                _emit(sys.stdout, line)
            status = 0
        else:
            if parsed.allow_commands and not parsed.allow_edit:
                parser.error("--allow-commands requires --allow-edit")
            status = asyncio.run(_run_local(parsed))
    except DomainOperationError as error:
        _emit(
            sys.stderr,
            f"agent-platform: {_terminal_safe(error.code)}: {_terminal_safe(error.message)}",
        )
        status = 1
    except (OSError, TypeError, ValueError) as error:
        _emit(sys.stderr, f"agent-platform: {_terminal_safe(str(error))}")
        status = 2
    raise SystemExit(status)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "render",
        help="validate and render JSON-Line agent events from standard input",
    )
    local = commands.add_parser(
        "local",
        help="run one bounded in-memory session against an isolated Git worktree",
    )
    local.add_argument("--workspace", type=Path, required=True)
    local.add_argument("--task", action="append", required=True)
    local.add_argument("--route", default="coding-default")
    local.add_argument("--allow-edit", action="store_true")
    local.add_argument("--allow-commands", action="store_true")
    local.add_argument("--sandbox-image", default=_DEFAULT_SANDBOX_IMAGE)
    local.add_argument("--patch-output", type=Path)
    local.add_argument("--json", action="store_true", dest="json_events")
    return parser


async def _run_local(arguments: argparse.Namespace) -> int:  # noqa: PLR0912
    settings = PlatformSettings()
    source = arguments.workspace.resolve(strict=True)
    run_group_id = uuid.uuid4()
    workspace = await asyncio.to_thread(
        GitWorktreeManager().create,
        source,
        run_id=f"local-cli-{run_group_id}",
    )
    adapter: OpenAIAgentsGateway | None = None
    gateway: GatewayClient | None = None
    sandbox: PodmanSandbox | None = None
    try:
        adapter = create_openai_compatible_agents_gateway(settings)
        gateway = GatewayClient(
            adapter,
            config=GatewayClientConfig(route_names=(arguments.route,)),
            close=adapter.aclose,
        )
        active_gateway = gateway
        if arguments.allow_commands:
            sandbox = await PodmanSandbox.create(
                workspace,
                config=PodmanSandboxConfig(
                    image=arguments.sandbox_image,
                    environment="development",
                ),
            )
        tools = WorkspaceToolset(workspace, sandbox=sandbox).registry(
            include_edit=arguments.allow_edit,
            include_command=arguments.allow_commands,
        )
        clock = UtcClock()
        session_id = uuid.uuid4()

        def loop_factory(run_id: uuid.UUID, journal: TranscriptJournal) -> AgentLoop:
            checkpoints = InMemoryCheckpointCoordinator(
                run_id=run_id,
                session_id=session_id,
                workspace=workspace,
                clock=clock,
                cancel_active=sandbox.cancel_active if sandbox is not None else None,
            )
            return AgentLoop(
                gateway=active_gateway,
                tools=tools,
                clock=clock,
                id_generator=UuidIdGenerator(),
                config=AgentLoopConfig(
                    max_turns=settings.max_turns,
                    max_tool_calls=settings.max_tool_calls,
                    max_semantic_retries=settings.max_semantic_retries,
                    tool_timeout_seconds=settings.max_command_timeout_seconds,
                ),
                redactor=Redactor(settings.redaction_values()),
                checkpoints=checkpoints,
                transcript_journal=journal,
            )

        session = InMemoryAgentSession(
            loop_factory=loop_factory,
            route_name=arguments.route,
            system_message=_SYSTEM_INSTRUCTIONS,
            session_id=session_id,
        )
        failed = False
        for task in arguments.task:
            async for event in session.run(task):
                if arguments.json_events:
                    _emit(sys.stdout, event.model_dump_json())
                else:
                    _emit(sys.stdout, render_event(event))
                failed = failed or isinstance(event, RunFailedEvent)
            if failed:
                break
        if arguments.allow_edit:
            patch = await asyncio.to_thread(workspace.final_patch)
            if arguments.patch_output is not None:
                await asyncio.to_thread(_write_exclusive, arguments.patch_output, patch)
                _emit(
                    sys.stderr,
                    "patch "
                    f"bytes={len(patch)} sha256={hashlib.sha256(patch).hexdigest()} "
                    f"path={_terminal_safe(os.fspath(arguments.patch_output))}",
                )
            else:
                _emit(
                    sys.stderr,
                    f"patch bytes={len(patch)} sha256={hashlib.sha256(patch).hexdigest()}",
                )
        return 1 if failed else 0
    finally:
        try:
            if sandbox is not None:
                await sandbox.destroy()
        finally:
            try:
                await workspace.destroy()
            finally:
                if gateway is not None:
                    await gateway.aclose()
                elif adapter is not None:
                    await adapter.aclose()


def _write_exclusive(path: Path, content: bytes) -> None:
    destination = path.resolve(strict=False)
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        os.close(descriptor)


def _terminal_safe(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)[1:-1]


def _emit(stream: TextIO, value: str) -> None:
    stream.write(f"{value}\n")
    stream.flush()


__all__ = ["main"]
