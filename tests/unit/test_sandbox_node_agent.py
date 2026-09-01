from __future__ import annotations

import os
import ssl
import uuid
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, cast

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from agent_core.artifacts import StoredObject
from agent_core.domain.base import FrozenJsonObject
from agent_core.domain.errors import DomainOperationError
from agent_core.sandbox import (
    CommandCompleted,
    CommandEvent,
    CommandOutput,
    CommandSpec,
    WorkspaceSnapshot,
)
from agent_core.tools import (
    RegisteredTool,
    ToolArguments,
    ToolEffect,
    ToolExecutionCompleted,
    ToolExecutionContext,
    ToolOutputChannel,
    ToolRegistry,
)
from sandbox_node_agent import (
    CreateSandboxRequest,
    NodeAgentTlsSettings,
    NodeSandboxRegistry,
    NodeSandboxResources,
    RemoteSandbox,
    create_client_ssl_context,
    create_node_agent_app,
)
from sandbox_node_agent import tls as node_tls
from sandbox_node_agent.body_limit import NodeRequestBodyLimitMiddleware
from sandbox_node_agent.production import (
    _podman_runtime_directory,
    _replace_worktree_from_checkpoint,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator

    from starlette.types import ASGIApp, Message, Receive, Scope

    from agent_core.tools import ToolExecutionEvent


class _Sandbox:
    def __init__(self, *, fail_command: bool = False) -> None:
        self.files = {"README.md": b"initial"}
        self.fail_command = fail_command
        self.cancelled = False
        self.destroyed = False
        self.restored: WorkspaceSnapshot | None = None

    async def execute(self, command: CommandSpec) -> AsyncIterator[CommandEvent]:
        assert command.argv == ("python", "-V")
        yield CommandOutput(
            sequence=1,
            channel=ToolOutputChannel.STDOUT,
            chunk="Python 3.12\n",
        )
        if self.fail_command:
            raise DomainOperationError(
                code="sandbox_test_failure",
                message="the test sandbox failed",
                retryable=True,
            )
        yield CommandCompleted(exit_code=0)

    async def read_file(self, path: str) -> bytes:
        return self.files[path]

    async def write_file(self, path: str, content: bytes) -> None:
        self.files[path] = content

    async def create_snapshot(self) -> WorkspaceSnapshot:
        return WorkspaceSnapshot(id="snapshot-1", uri="node://snapshot-1", revision="revision-1")

    async def restore_snapshot(self, snapshot: WorkspaceSnapshot) -> None:
        self.restored = snapshot

    async def final_patch(self) -> StoredObject:
        return StoredObject(
            object_key="tenants/test/final.patch",
            sha256="b" * 64,
            size_bytes=5,
            content_type="text/x-diff",
        )

    async def current_patch(self, *, max_bytes: int) -> bytes:
        content = b"diff --git a/README.md b/README.md\n"
        if len(content) > max_bytes:
            raise DomainOperationError(
                code="context_git_diff_limit",
                message="the current Git diff exceeds its context limit",
            )
        return content

    async def cancel_active(self) -> None:
        self.cancelled = True

    async def destroy(self) -> None:
        self.destroyed = True


class _Authorizer:
    def __init__(self) -> None:
        self.calls = 0

    async def authorize(self, request: CreateSandboxRequest) -> None:
        self.calls += 1
        if request.run_lease_generation != 1:
            raise DomainOperationError(
                code="run_lease_lost",
                message="the run lease is no longer active",
            )


class _Factory:
    def __init__(self, sandbox: _Sandbox, tools: ToolRegistry | None = None) -> None:
        self.sandbox = sandbox
        self.tools = tools or ToolRegistry()
        self.calls = 0

    async def create(self, request: CreateSandboxRequest) -> NodeSandboxResources:
        del request
        self.calls += 1
        return NodeSandboxResources(
            sandbox=self.sandbox,
            tools=self.tools,
            final_patch_exporter=self.sandbox,
            current_patch_exporter=self.sandbox,
        )


class _NoPatchFactory(_Factory):
    async def create(self, request: CreateSandboxRequest) -> NodeSandboxResources:
        del request
        self.calls += 1
        return NodeSandboxResources(sandbox=self.sandbox, tools=self.tools)


class _IncompleteSandbox(_Sandbox):
    async def execute(self, command: CommandSpec) -> AsyncIterator[CommandEvent]:
        del command
        yield CommandOutput(
            sequence=1,
            channel=ToolOutputChannel.STDOUT,
            chunk="incomplete",
        )


class _EchoArguments(ToolArguments):
    text: str


async def _echo_tool(
    arguments: _EchoArguments,
    context: ToolExecutionContext,
) -> AsyncGenerator[ToolExecutionEvent, None]:
    del context
    yield ToolExecutionCompleted(result=FrozenJsonObject({"echo": arguments.text}))


def _tools() -> ToolRegistry:
    return ToolRegistry(
        (
            RegisteredTool(
                name="echo",
                description="Echo validated text.",
                arguments_type=_EchoArguments,
                handler=_echo_tool,
                effect=ToolEffect.READ_ONLY,
            ),
        )
    )


def _request() -> CreateSandboxRequest:
    return CreateSandboxRequest(
        tenant_id=uuid.UUID("10000000-0000-0000-0000-000000000001"),
        session_id=uuid.UUID("10000000-0000-0000-0000-000000000002"),
        run_id=uuid.UUID("10000000-0000-0000-0000-000000000003"),
        workspace_id=uuid.UUID("10000000-0000-0000-0000-000000000004"),
        worker_id="worker-1",
        run_lease_token=uuid.UUID("10000000-0000-0000-0000-000000000005"),
        run_lease_generation=1,
        source_object_key=(
            "tenants/10000000-0000-0000-0000-000000000001/workspaces/"
            "10000000-0000-0000-0000-000000000004/source.tar.gz"
        ),
        source_sha256="a" * 64,
        source_size_bytes=123,
    )


@pytest.mark.asyncio
async def test_remote_sandbox_round_trip_is_capability_scoped_and_idempotent() -> None:
    sandbox = _Sandbox()
    authorizer = _Authorizer()
    factory = _Factory(sandbox, _tools())
    registry = NodeSandboxRegistry(authorizer=authorizer, factory=factory)
    app = create_node_agent_app(registry, close_registry=False)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="https://node.invalid",
    ) as client:
        settings = NodeAgentTlsSettings(
            environment="test",
            base_url="https://node.invalid",
            ca_file="/nonexistent/ca.pem",
            certificate_file="/nonexistent/client.pem",
            private_key_file="/nonexistent/client.key",
        )
        remote = await RemoteSandbox.create(_request(), settings, client=client)
        events = [
            event
            async for event in remote.execute(
                CommandSpec(
                    argv=("python", "-V"),
                    timeout_seconds=10,
                    max_output_bytes=1024,
                )
            )
        ]
        assert events[-1] == CommandCompleted(exit_code=0)
        assert await remote.read_file("README.md") == b"initial"
        await remote.write_file("README.md", b"changed")
        assert await remote.read_file("README.md") == b"changed"
        snapshot = await remote.create_snapshot()
        await remote.restore_snapshot(snapshot)
        final_patch = await remote.final_patch()
        current_patch = await remote.current_patch()
        await remote.cancel_active()

        tools = remote.tool_registry()
        assert [definition.name for definition in tools.definitions] == ["echo"]
        with pytest.raises(DomainOperationError) as malformed:
            tools.prepare("echo", FrozenJsonObject({}))
        assert malformed.value.code == "malformed_tool_arguments"
        prepared = tools.prepare("echo", FrozenJsonObject({"text": "hello"}))
        tool_events = [
            event
            async for event in prepared.stream(
                ToolExecutionContext(
                    run_id=_request().run_id,
                    tool_call_id="echo-1",
                    max_output_bytes=1024,
                    max_result_bytes=1024,
                )
            )
        ]
        assert tool_events == [ToolExecutionCompleted(result=FrozenJsonObject({"echo": "hello"}))]

        first = await client.post("/v1/sandboxes", json=_request().model_dump(mode="json"))
        second = await client.post("/v1/sandboxes", json=_request().model_dump(mode="json"))
        assert first.json() == second.json()
        assert factory.calls == 1
        assert authorizer.calls == 3

        hidden = await client.post(
            f"/v1/sandboxes/{first.json()['sandbox_id']}/cancel",
            headers={"authorization": "Bearer " + "0" * 64},
            json={},
        )
        assert hidden.status_code == 404
        assert hidden.json()["error"]["code"] == "sandbox_not_found"

        await remote.destroy()
        await remote.destroy()

    assert sandbox.cancelled is True
    assert final_patch.object_key == "tenants/test/final.patch"
    assert current_patch == b"diff --git a/README.md b/README.md\n"
    assert sandbox.restored == snapshot
    assert sandbox.destroyed is True
    await registry.aclose()


@pytest.mark.asyncio
async def test_remote_command_surfaces_sanitized_stream_failure() -> None:
    sandbox = _Sandbox(fail_command=True)
    registry = NodeSandboxRegistry(authorizer=_Authorizer(), factory=_Factory(sandbox))
    app = create_node_agent_app(registry, close_registry=False)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="https://node.invalid",
    ) as client:
        remote = await RemoteSandbox.create(
            _request(),
            NodeAgentTlsSettings(
                environment="test",
                base_url="https://node.invalid",
                ca_file="/none/ca",
                certificate_file="/none/cert",
                private_key_file="/none/key",
            ),
            client=client,
        )
        with pytest.raises(DomainOperationError) as failure:
            async for _event in remote.execute(
                CommandSpec(
                    argv=("python", "-V"),
                    timeout_seconds=10,
                    max_output_bytes=1024,
                )
            ):
                pass
        assert failure.value.code == "sandbox_test_failure"
        assert failure.value.retryable is True
        await remote.destroy()


@pytest.mark.asyncio
async def test_node_readiness_fails_closed_when_a_live_dependency_is_unavailable() -> None:
    registry = NodeSandboxRegistry(authorizer=_Authorizer(), factory=_Factory(_Sandbox()))
    calls = 0

    async def dependencies_ready() -> bool:
        nonlocal calls
        calls += 1
        return calls == 1

    app = create_node_agent_app(
        registry,
        close_registry=False,
        dependencies_ready=dependencies_ready,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="https://node.invalid",
    ) as client:
        first = await client.get("/health/ready")
        second = await client.get("/health/ready")

    assert first.status_code == 200
    assert first.json() == {"status": "ready"}
    assert second.status_code == 503
    assert second.json() == {"status": "unavailable"}
    await registry.aclose()


@pytest.mark.asyncio
async def test_node_readiness_hides_dependency_probe_failures() -> None:
    registry = NodeSandboxRegistry(authorizer=_Authorizer(), factory=_Factory(_Sandbox()))

    async def dependencies_ready() -> bool:
        raise RuntimeError("sensitive dependency failure")

    app = create_node_agent_app(
        registry,
        close_registry=False,
        dependencies_ready=dependencies_ready,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="https://node.invalid",
    ) as client:
        response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "unavailable"}
    assert "sensitive" not in response.text
    await registry.aclose()


def test_node_agent_request_limits_and_tls_settings_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(ValidationError):
        NodeAgentTlsSettings(
            base_url="http://node.invalid",
            ca_file="relative-ca",
            certificate_file="/cert",
            private_key_file="/key",
        )

    ca_file = tmp_path / "ca.pem"
    certificate = tmp_path / "certificate.pem"
    private_key = tmp_path / "private.key"
    for path in (ca_file, certificate, private_key):
        path.write_text("not-a-certificate", encoding="utf-8")
    private_key.chmod(0o644)
    settings = NodeAgentTlsSettings(
        ca_file=str(ca_file),
        certificate_file=str(certificate),
        private_key_file=str(private_key),
    )
    with pytest.raises(ValueError, match="group/world"):
        create_client_ssl_context(settings)

    registry = NodeSandboxRegistry(authorizer=_Authorizer(), factory=_Factory(_Sandbox()))
    client = TestClient(
        create_node_agent_app(
            registry,
            close_registry=False,
            max_request_body_bytes=128,
        ),
        raise_server_exceptions=False,
    )
    oversized = client.post(
        "/v1/sandboxes",
        content=b"x" * 129,
        headers={"content-type": "application/json"},
    )
    assert oversized.status_code == 413
    assert oversized.json()["error"]["code"] == "request_body_limit"


def test_podman_runtime_directory_is_derived_from_the_configured_socket() -> None:
    assert _podman_runtime_directory(Path("/run/user/1000/podman/podman.sock")) == Path(
        "/run/user/1000"
    )
    assert _podman_runtime_directory(Path("/private/tmp/podman-forward.sock")) == Path(
        "/private/tmp"
    )


def test_checkpoint_overlay_preserves_git_metadata_and_replaces_content(tmp_path: Path) -> None:
    checkpoint = tmp_path / "checkpoint"
    worktree = tmp_path / "worktree"
    checkpoint.mkdir()
    worktree.mkdir()
    (checkpoint / "nested").mkdir()
    (checkpoint / "nested" / "new.txt").write_text("restored", encoding="utf-8")
    (worktree / ".git").write_text("gitdir: /private/git", encoding="utf-8")
    (worktree / "old.txt").write_text("obsolete", encoding="utf-8")

    _replace_worktree_from_checkpoint(checkpoint, worktree)

    assert (worktree / ".git").read_text(encoding="utf-8") == "gitdir: /private/git"
    assert not (worktree / "old.txt").exists()
    assert (worktree / "nested" / "new.txt").read_text(encoding="utf-8") == "restored"

    (checkpoint / ".GIT").write_text("forbidden", encoding="utf-8")
    with pytest.raises(DomainOperationError) as protected:
        _replace_worktree_from_checkpoint(checkpoint, worktree)
    assert protected.value.code == "checkpoint_materialization_failed"


@pytest.mark.asyncio
async def test_node_body_limit_preserves_real_disconnect_channel() -> None:
    incoming: deque[Message] = deque(
        (
            {"type": "http.request", "body": b"{}", "more_body": False},
            {"type": "http.disconnect"},
        )
    )
    observed: list[Message] = []

    async def receive() -> Message:
        return incoming.popleft()

    async def send(_message: Message) -> None:
        return None

    async def downstream(_scope: Scope, replay: object, _send: object) -> None:
        typed_replay = cast("Receive", replay)
        observed.append(await typed_replay())
        observed.append(await typed_replay())

    middleware = NodeRequestBodyLimitMiddleware(
        cast("ASGIApp", downstream),
        max_body_bytes=1024,
    )
    await middleware(
        cast("Scope", {"type": "http", "headers": [(b"content-length", b"2")]}),
        receive,
        send,
    )

    assert [message["type"] for message in observed] == ["http.request", "http.disconnect"]


def test_node_agent_tls_file_policy_is_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing = NodeAgentTlsSettings(
        ca_file=str(tmp_path / "missing-ca"),
        certificate_file=str(tmp_path / "missing-cert"),
        private_key_file=str(tmp_path / "missing-key"),
    )
    with pytest.raises(ValueError, match="must exist"):
        node_tls._require_tls_files(missing)

    directory = tmp_path / "directory"
    directory.mkdir()
    not_regular = missing.model_copy(
        update={
            "ca_file": str(directory),
            "certificate_file": str(directory),
            "private_key_file": str(directory),
        }
    )
    with pytest.raises(ValueError, match="regular files"):
        node_tls._require_tls_files(not_regular)

    for name in ("ca.pem", "cert.pem", "key.pem"):
        (tmp_path / name).write_text("fixture", encoding="utf-8")
    key = tmp_path / "key.pem"
    key.chmod(0o640)
    settings = NodeAgentTlsSettings(
        ca_file=str(tmp_path / "ca.pem"),
        certificate_file=str(tmp_path / "cert.pem"),
        private_key_file=str(key),
    )
    monkeypatch.setattr(os, "getegid", lambda: key.stat().st_gid + 1)
    with pytest.raises(ValueError, match="process group"):
        node_tls._require_tls_files(settings)

    key.chmod(0o600)
    monkeypatch.setattr(os, "geteuid", lambda: key.stat().st_uid + 1)
    with pytest.raises(ValueError, match="process user"):
        node_tls._require_tls_files(settings)

    test_settings = settings.model_copy(update={"environment": "test"})
    node_tls._require_tls_files(test_settings)


def test_node_agent_tls_contexts_require_tls13_and_mutual_auth(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Context:
        minimum_version: ssl.TLSVersion | None = None
        verify_mode: ssl.VerifyMode | None = None
        check_hostname = False

        def __init__(self) -> None:
            self.loaded_chain: tuple[str, str] | None = None
            self.loaded_ca: str | None = None

        def load_cert_chain(self, certificate: str, private_key: str) -> None:
            self.loaded_chain = (certificate, private_key)

        def load_verify_locations(self, *, cafile: str) -> None:
            self.loaded_ca = cafile

    contexts: list[_Context] = []

    def create_context(*_args: object, **_kwargs: object) -> _Context:
        context = _Context()
        contexts.append(context)
        return context

    monkeypatch.setattr(node_tls, "_require_tls_files", lambda _settings: None)
    monkeypatch.setattr(ssl, "create_default_context", create_context)
    settings = NodeAgentTlsSettings(
        environment="test",
        ca_file=str(tmp_path / "ca.pem"),
        certificate_file=str(tmp_path / "cert.pem"),
        private_key_file=str(tmp_path / "key.pem"),
    )

    client = cast("_Context", node_tls.create_client_ssl_context(settings))
    server = cast("_Context", node_tls.create_server_ssl_context(settings))

    assert client is contexts[0]
    assert server is contexts[1]
    assert all(context.minimum_version is ssl.TLSVersion.TLSv1_3 for context in contexts)
    assert all(context.verify_mode is ssl.CERT_REQUIRED for context in contexts)
    assert contexts[0].check_hostname is True
    assert contexts[0].loaded_chain == (settings.certificate_file, settings.private_key_file)
    assert contexts[1].loaded_ca == settings.ca_file
    assert contexts[1].loaded_chain == (settings.certificate_file, settings.private_key_file)


@pytest.mark.asyncio
async def test_node_routes_fail_closed_for_invalid_capabilities_and_content() -> None:
    sandbox = _Sandbox()
    registry = NodeSandboxRegistry(
        authorizer=_Authorizer(),
        factory=_NoPatchFactory(sandbox),
    )
    app = create_node_agent_app(registry, close_registry=False)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="https://node.invalid",
    ) as client:
        created = await client.post("/v1/sandboxes", json=_request().model_dump(mode="json"))
        sandbox_id = created.json()["sandbox_id"]
        token = created.json()["capability_token"]
        headers = {"authorization": f"Bearer {token}"}

        for authorization in (None, "Basic value", "Bearer short", "Bearer " + "G" * 64):
            request_headers = {} if authorization is None else {"authorization": authorization}
            hidden = await client.post(
                f"/v1/sandboxes/{sandbox_id}/cancel",
                headers=request_headers,
                json={},
            )
            assert hidden.status_code == 404

        invalid_path = await client.post(
            "/v1/sandboxes/not-a-uuid/cancel",
            headers=headers,
            json={},
        )
        assert invalid_path.status_code == 422
        assert invalid_path.json()["error"]["code"] == "invalid_node_request"

        too_small = await client.post(
            f"/v1/sandboxes/{sandbox_id}/files/read",
            headers=headers,
            json={"path": "README.md", "max_bytes": 1},
        )
        assert too_small.status_code == 409
        assert too_small.json()["error"]["code"] == "sandbox_read_limit"

        invalid_base64 = await client.post(
            f"/v1/sandboxes/{sandbox_id}/files/write",
            headers=headers,
            json={
                "path": "README.md",
                "content_base64": "!!!!",
                "size_bytes": 0,
                "sha256": "0" * 64,
            },
        )
        assert invalid_base64.status_code == 409
        assert invalid_base64.json()["error"]["code"] == "sandbox_write_invalid"

        mismatch = await client.post(
            f"/v1/sandboxes/{sandbox_id}/files/write",
            headers=headers,
            json={
                "path": "README.md",
                "content_base64": "YQ==",
                "size_bytes": 2,
                "sha256": "0" * 64,
            },
        )
        assert mismatch.status_code == 409
        assert mismatch.json()["error"]["code"] == "sandbox_write_checksum_mismatch"

        no_patch = await client.post(
            f"/v1/sandboxes/{sandbox_id}/final-patch",
            headers=headers,
            json={},
        )
        assert no_patch.status_code == 409
        assert no_patch.json()["error"]["code"] == "final_patch_unavailable"

        no_context_patch = await client.post(
            f"/v1/sandboxes/{sandbox_id}/context/current-patch",
            headers=headers,
            json={"max_bytes": 1024},
        )
        assert no_context_patch.status_code == 409
        assert no_context_patch.json()["error"]["code"] == "context_git_diff_unavailable"


@pytest.mark.asyncio
async def test_remote_rejects_incomplete_and_malformed_node_protocols() -> None:
    registry = NodeSandboxRegistry(
        authorizer=_Authorizer(),
        factory=_Factory(_IncompleteSandbox()),
    )
    app = create_node_agent_app(registry, close_registry=False)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="https://node.invalid",
    ) as client:
        remote = await RemoteSandbox.create(
            _request(),
            NodeAgentTlsSettings(
                environment="test",
                base_url="https://node.invalid",
                ca_file="/none/ca",
                certificate_file="/none/cert",
                private_key_file="/none/key",
            ),
            client=client,
        )
        with pytest.raises(DomainOperationError) as incomplete:
            async for _ in remote.execute(
                CommandSpec(argv=("python", "-V"), timeout_seconds=10, max_output_bytes=1024)
            ):
                pass
        assert incomplete.value.code == "sandbox_protocol_error"
        await remote.destroy()

    async def invalid_response(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"not-json")

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(invalid_response),
        base_url="https://node.invalid",
    )
    remote = RemoteSandbox(
        client=client,
        owns_client=False,
        sandbox_id=uuid.uuid4(),
        capability_token="a" * 64,
        tools=(),
    )
    with pytest.raises(DomainOperationError) as invalid:
        await remote.cancel_active()
    assert invalid.value.code == "sandbox_node_unavailable"
    assert invalid.value.retryable is True
    await client.aclose()
