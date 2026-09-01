from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import os
import shutil
import socket
import uuid
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from sqlalchemy import select

from agent_api import ApiServices, Principal, StaticTokenAuthenticator, create_app
from agent_core.artifacts import DurableSnapshotReference
from agent_core.context import (
    ContextBudgetRegistry,
    ContextCompressionRequest,
    ContextCompressionResult,
    ContextPipeline,
    ContextRouteBudget,
)
from agent_core.domain.errors import DomainOperationError
from agent_core.domain.status import RunStatus, ToolCallStatus
from agent_core.fakes import ScriptedGatewayTurn, ScriptedModelGateway
from agent_core.gateway import GatewayToolCall
from agent_core.loop import AgentLoopConfig, UtcClock
from agent_worker import (
    AgentLoopRunExecutor,
    DurableRunContextBuilder,
    PersistentRunContextSource,
    WorkerConfig,
    WorkerService,
)
from agent_worker.production import ObjectCheckpointRestorer, RemoteAgentLoopFactory
from artifact_store import (
    S3ObjectStoreSettings,
    SnapshotValidationWorker,
    SnapshotValidator,
    WorkspaceArchiver,
    create_s3_object_store,
)
from event_store import PostgresEventStore
from gateway_client import GatewayClient
from platform_persistence import (
    Database,
    DatabaseSettings,
    PostgresApprovalRepository,
    PostgresContextRepository,
    PostgresExecutionRepository,
    PostgresMemoryRepository,
    PostgresRunQueue,
    PostgresRunRepository,
    PostgresSessionRepository,
    PostgresTaskRepository,
    PostgresWorkspaceLeaseStore,
    PostgresWorkspaceRepository,
)
from platform_persistence.distributed import PostgresRecoveryStore
from platform_persistence.models import CheckpointRecord, ToolCallRecord
from platform_telemetry import PlatformTelemetry, Redactor, TelemetrySettings
from sandbox_node_agent import NodeAgentTlsSettings, create_client_ssl_context, serve_node_agent
from sandbox_node_agent.production import ProductionNodeSettings, create_production_node_app

if TYPE_CHECKING:
    from agent_core.distributed import (
        RunExecutionResult,
        RunLease,
        RunRecoveryState,
        WorkspaceWriterLease,
    )
    from agent_core.loop import AgentLoop

pytestmark = [
    pytest.mark.end_to_end,
    pytest.mark.skipif(
        os.getenv("AGENT_PLATFORM_RUN_DISTRIBUTED_E2E") != "1",
        reason="set AGENT_PLATFORM_RUN_DISTRIBUTED_E2E=1",
    ),
]

_FIXTURE = Path(__file__).parent / "fixtures" / "calculator_bug"
_TOKEN = "distributed-e2e-token"  # noqa: S105 - inert local test credential
_OTHER_TOKEN = "distributed-e2e-other-token"  # noqa: S105 - inert local test credential
_TENANT_ID = uuid.UUID("40000000-0000-0000-0000-000000000001")
_OTHER_TENANT_ID = uuid.UUID("40000000-0000-0000-0000-000000000002")


class _Ready:
    async def ready(self) -> bool:
        return True


class _UnexpectedCompressor:
    async def compress(
        self,
        request: ContextCompressionRequest,
    ) -> ContextCompressionResult:
        del request
        raise AssertionError("the bounded E2E context must not require compression")


class _RecordingExecutor:
    def __init__(
        self,
        executor: AgentLoopRunExecutor,
        errors: list[tuple[str, str]],
    ) -> None:
        self._executor = executor
        self._errors = errors

    async def execute(
        self,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        recovery: RunRecoveryState,
    ) -> RunExecutionResult:
        try:
            return await self._executor.execute(lease, writer_lease, recovery)
        except DomainOperationError as error:
            self._errors.append(("execute", error.code))
            raise

    async def cancel(self, lease: RunLease) -> None:
        await self._executor.cancel(lease)


def _private_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65_537, key_size=2048)


def _write_key(path: Path, key: rsa.RSAPrivateKey) -> None:
    path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    path.chmod(0o600)


def _create_tls_material(root: Path) -> tuple[NodeAgentTlsSettings, NodeAgentTlsSettings]:
    now = datetime.now(UTC)
    ca_key = _private_key()
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "agent-platform-e2e-ca")])
    ca = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=False,
                content_commitment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )
    ca_path = root / "ca.crt"
    ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))

    def issue(name: str, usage: x509.ObjectIdentifier, *, server: bool) -> tuple[Path, Path]:
        key = _private_key()
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
        builder = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(ca.subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.ExtendedKeyUsage([usage]), critical=True)
        )
        if server:
            builder = builder.add_extension(
                x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
                critical=False,
            )
        certificate = builder.sign(ca_key, hashes.SHA256())
        certificate_path = root / f"{name}.crt"
        key_path = root / f"{name}.key"
        certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
        _write_key(key_path, key)
        return certificate_path, key_path

    server_certificate, server_key = issue(
        "node-server",
        ExtendedKeyUsageOID.SERVER_AUTH,
        server=True,
    )
    client_certificate, client_key = issue(
        "worker-client",
        ExtendedKeyUsageOID.CLIENT_AUTH,
        server=False,
    )
    server = NodeAgentTlsSettings(
        environment="test",
        ca_file=str(ca_path),
        certificate_file=str(server_certificate),
        private_key_file=str(server_key),
    )
    client = NodeAgentTlsSettings(
        environment="test",
        ca_file=str(ca_path),
        certificate_file=str(client_certificate),
        private_key_file=str(client_key),
    )
    return server, client


def _available_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


async def _wait_for_node(
    server: asyncio.Task[None],
    settings: NodeAgentTlsSettings,
) -> None:
    context = create_client_ssl_context(settings)
    async with httpx.AsyncClient(
        base_url=settings.base_url,
        verify=context,
        timeout=1,
        trust_env=False,
    ) as client:
        for _ in range(100):
            if server.done():
                await server
            try:
                response = await client.get("/health/ready")
            except httpx.HTTPError:
                await asyncio.sleep(0.05)
                continue
            if response.status_code == 200:
                return
            await asyncio.sleep(0.05)
    raise AssertionError("mTLS sandbox node did not become ready")


async def _wait_for_worker(worker: WorkerService) -> None:
    for _ in range(1200):
        if worker.active_count == 0:
            return
        await asyncio.sleep(0.025)
    raise AssertionError("distributed worker did not finish its attempt")


async def _run_attempt(
    worker_id: str,
    *,
    queue: PostgresRunQueue,
    workspace_leases: PostgresWorkspaceLeaseStore,
    recovery: PostgresRecoveryStore,
    executor: AgentLoopRunExecutor,
    errors: list[tuple[str, str]],
) -> None:
    worker = WorkerService(
        config=WorkerConfig(
            worker_id=worker_id,
            lease_seconds=60,
            heartbeat_seconds=5,
            max_attempt_seconds=120,
        ),
        queue=queue,
        workspace_leases=workspace_leases,
        recovery=recovery,
        restorer=ObjectCheckpointRestorer(),
        executor=_RecordingExecutor(executor, errors),
        clock=UtcClock(),
    )
    claimed = False
    for _ in range(200):
        if await worker.run_once():
            claimed = True
            break
        await asyncio.sleep(0.025)
    assert claimed is True
    await _wait_for_worker(worker)
    await worker.aclose()


async def _approval_id(client: httpx.AsyncClient, run_id: uuid.UUID, index: int) -> uuid.UUID:
    response = await client.get(
        f"/v1/runs/{run_id}/events?limit=100",
        headers={"authorization": f"Bearer {_TOKEN}"},
    )
    assert response.status_code == 200
    approvals = [
        event["payload"]["approval_id"]
        for event in response.json()["events"]
        if event["event_type"] == "tool.approval_required"
    ]
    return uuid.UUID(approvals[index])


@pytest.mark.asyncio
async def test_api_s3_mtls_node_approval_recovery_and_patch_happy_path(  # noqa: PLR0915
    tmp_path: Path,
) -> None:
    database_settings = DatabaseSettings()
    s3_settings = S3ObjectStoreSettings(environment="test")
    database = Database(database_settings)
    objects = create_s3_object_store(s3_settings)
    sessions = PostgresSessionRepository(database.sessions)
    runs = PostgresRunRepository(database.sessions)
    approvals = PostgresApprovalRepository(database.sessions)
    events = PostgresEventStore(database.sessions)
    execution = PostgresExecutionRepository(database.sessions)
    context = PostgresContextRepository(database.sessions)
    memories = PostgresMemoryRepository(database.sessions)
    queue = PostgresRunQueue(database.sessions)
    workspace_leases = PostgresWorkspaceLeaseStore(database.sessions)
    recovery = PostgresRecoveryStore(database.sessions)
    workspaces = PostgresWorkspaceRepository(database.sessions)
    tasks = PostgresTaskRepository(database.sessions)
    telemetry = PlatformTelemetry(
        TelemetrySettings(service_name="distributed-e2e", environment="test")
    )
    scripted = ScriptedModelGateway(
        (
            ScriptedGatewayTurn.tool_calls(
                GatewayToolCall(
                    id="distributed-edit",
                    name="edit_file",
                    arguments={
                        "path": "calculator.py",
                        "expected_sha256": hashlib.sha256(
                            (_FIXTURE / "calculator.py").read_bytes()
                        ).hexdigest(),
                        "old_text": "return left - right",
                        "new_text": "return left + right",
                    },
                )
            ),
            ScriptedGatewayTurn.tool_calls(
                GatewayToolCall(
                    id="distributed-tests",
                    name="run_command",
                    arguments={
                        "argv": ["python", "-m", "unittest", "-v"],
                        "cwd": ".",
                        "timeout_seconds": 30,
                    },
                )
            ),
            ScriptedGatewayTurn.text("Fixed the calculator and verified its tests."),
            ScriptedGatewayTurn.tool_calls(
                GatewayToolCall(
                    id="distributed-edit",
                    name="edit_file",
                    arguments={
                        "path": "calculator.py",
                        "expected_sha256": hashlib.sha256(
                            (_FIXTURE / "calculator.py").read_bytes()
                        ).hexdigest(),
                        "old_text": "return left - right",
                        "new_text": "return sum((left, right))",
                    },
                )
            ),
            ScriptedGatewayTurn.text("Applied the replacement branch fix."),
        )
    )
    gateway = GatewayClient(scripted)
    server_task: asyncio.Task[None] | None = None
    factory: RemoteAgentLoopFactory | None = None
    attempt_errors: list[tuple[str, str]] = []
    try:
        assert await database.ready()
        assert await objects.ready()
        services = ApiServices(
            authenticator=StaticTokenAuthenticator(
                {
                    _TOKEN: Principal(tenant_id=_TENANT_ID, subject="distributed-user"),
                    _OTHER_TOKEN: Principal(
                        tenant_id=_OTHER_TENANT_ID,
                        subject="other-user",
                    ),
                }
            ),
            sessions=sessions,
            runs=runs,
            approvals=approvals,
            events=events,
            readiness=_Ready(),
            tasks=tasks,
            workspaces=workspaces,
            object_store=objects,
        )
        app = create_app(services)
        api = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://api.invalid",
        )
        async with api:
            authorization = {"authorization": f"Bearer {_TOKEN}"}
            workspace_response = await api.post(
                "/v1/workspaces",
                headers=authorization,
                json={"display_name": "distributed production journey"},
            )
            assert workspace_response.status_code == 201
            workspace_id = uuid.UUID(workspace_response.json()["id"])
            upload_response = await api.post(
                f"/v1/workspaces/{workspace_id}/snapshots",
                headers=authorization,
            )
            assert upload_response.status_code == 201
            upload_body = upload_response.json()
            snapshot_id = uuid.UUID(upload_body["snapshot"]["id"])
            archive = tmp_path / "source.tar.gz"
            source_sha256, source_size = WorkspaceArchiver().create(_FIXTURE, archive)
            async with httpx.AsyncClient(trust_env=False) as upload_client:
                uploaded = await upload_client.put(
                    upload_body["upload"]["url"],
                    headers=upload_body["upload"]["headers"],
                    content=archive.read_bytes(),
                )
            assert uploaded.status_code in {200, 204}
            finalized = await api.post(
                f"/v1/workspaces/{workspace_id}/snapshots/{snapshot_id}/finalize",
                headers=authorization,
                json={"sha256": source_sha256, "compressed_bytes": source_size},
            )
            assert finalized.status_code == 202
            validator = SnapshotValidationWorker(
                workspaces,
                SnapshotValidator(objects),
                worker_id="distributed-validator",
            )
            assert await validator.run_once() is True
            snapshot = await workspaces.get_snapshot(_TENANT_ID, workspace_id, snapshot_id)
            assert snapshot is not None and snapshot.status.value == "ready"

            session_response = await api.post(
                "/v1/sessions",
                headers=authorization,
                json={"workspace_id": str(workspace_id)},
            )
            assert session_response.status_code == 201
            session_id = uuid.UUID(session_response.json()["id"])
            run_response = await api.post(
                f"/v1/sessions/{session_id}/runs",
                headers={**authorization, "idempotency-key": "distributed-production-e2e"},
                json={
                    "task": "Fix calculator.add and run the repository tests.",
                    "priority": 100,
                    "referenced_files": [{"path": "README.md"}],
                },
            )
            assert run_response.status_code == 200
            run_id = uuid.UUID(run_response.json()["run"]["id"])

            tls_root = tmp_path / "tls"
            tls_root.mkdir(mode=0o700)
            server_tls, client_tls = _create_tls_material(tls_root)
            port = _available_port()
            client_tls = client_tls.model_copy(update={"base_url": f"https://127.0.0.1:{port}"})
            podman_socket = os.environ["AGENT_PLATFORM_PODMAN_SOCKET"].removeprefix("unix://")
            workspace_parent = tmp_path / "node-workspaces"
            podman_home = tmp_path / "podman-home"
            workspace_parent.mkdir(mode=0o700)
            podman_home.mkdir(mode=0o700)
            podman = shutil.which("podman")
            git = shutil.which("git")
            ripgrep = shutil.which("rg")
            assert podman is not None and git is not None and ripgrep is not None
            node_app = await create_production_node_app(
                node_settings=ProductionNodeSettings(
                    environment="test",
                    sandbox_image=os.getenv(
                        "AGENT_PLATFORM_SANDBOX_IMAGE",
                        "localhost/agent-platform-sandbox:sequence-10",
                    ),
                    workspace_parent=str(workspace_parent),
                    podman_executable=podman,
                    podman_socket=podman_socket,
                    podman_home=str(podman_home),
                    git_executable=git,
                    ripgrep_executable=ripgrep,
                    tls_ca_file=server_tls.ca_file,
                    tls_certificate_file=server_tls.certificate_file,
                    tls_private_key_file=server_tls.private_key_file,
                ),
                database_settings=database_settings,
                object_store_settings=s3_settings,
            )
            server_task = asyncio.create_task(
                serve_node_agent(node_app, server_tls, host="127.0.0.1", port=port)
            )
            await _wait_for_node(server_task, client_tls)

            factory = RemoteAgentLoopFactory(
                gateway=gateway,
                workspaces=workspaces,
                node_tls=client_tls,
                loop_config=AgentLoopConfig(),
                telemetry=telemetry,
                redactor=Redactor(()),
                tasks=tasks,
                execution=execution,
            )

            async def finalize_attempt(
                lease: RunLease,
                loop: AgentLoop,
                result: RunExecutionResult,
            ) -> None:
                assert factory is not None
                try:
                    await factory.finalize(lease, loop, result)
                except DomainOperationError as error:
                    attempt_errors.append(("finalize", error.code))
                    raise

            async def cleanup_attempt(lease: RunLease, loop: AgentLoop) -> None:
                assert factory is not None
                try:
                    await factory.cleanup(lease, loop)
                except DomainOperationError as error:
                    attempt_errors.append(("cleanup", error.code))
                    raise

            executor = AgentLoopRunExecutor(
                loop_factory=factory.create,
                events=events,
                tool_calls=execution,
                approvals=execution,
                context_builder=DurableRunContextBuilder(
                    pipeline=ContextPipeline(
                        budgets=ContextBudgetRegistry(
                            (
                                ContextRouteBudget(
                                    route_name="coding-default",
                                    max_context_tokens=1_000_000,
                                    reserved_output_tokens=4096,
                                ),
                            )
                        ),
                        compressor=_UnexpectedCompressor(),
                    ),
                    source=PersistentRunContextSource(
                        history=context,
                        memories=memories,
                        system_instructions="Work safely in the isolated workspace.",
                        workspace_context=factory,
                    ),
                    compactions=context,
                    proactive_compactions=context,
                    clock=UtcClock(),
                ),
                attempt_finalizer=finalize_attempt,
                loop_cleanup=cleanup_attempt,
                telemetry=telemetry,
            )

            await _run_attempt(
                "distributed-worker-a",
                queue=queue,
                workspace_leases=workspace_leases,
                recovery=recovery,
                executor=executor,
                errors=attempt_errors,
            )
            waiting = await runs.get(_TENANT_ID, run_id)
            assert waiting is not None and waiting.status is RunStatus.WAITING_APPROVAL
            edit_approval = await _approval_id(api, run_id, 0)
            approved = await api.post(
                f"/v1/runs/{run_id}/approvals/{edit_approval}",
                headers=authorization,
                json={"approved": True},
            )
            assert approved.status_code == 200
            resumed = await runs.get(_TENANT_ID, run_id)
            assert resumed is not None and resumed.status is RunStatus.QUEUED

            await _run_attempt(
                "distributed-worker-b",
                queue=queue,
                workspace_leases=workspace_leases,
                recovery=recovery,
                executor=executor,
                errors=attempt_errors,
            )
            waiting = await runs.get(_TENANT_ID, run_id)
            assert waiting is not None and waiting.status is RunStatus.WAITING_APPROVAL
            command_approval = await _approval_id(api, run_id, 1)
            approved = await api.post(
                f"/v1/runs/{run_id}/approvals/{command_approval}",
                headers=authorization,
                json={"approved": True},
            )
            assert approved.status_code == 200
            resumed = await runs.get(_TENANT_ID, run_id)
            assert resumed is not None and resumed.status is RunStatus.QUEUED

            await _run_attempt(
                "distributed-worker-c",
                queue=queue,
                workspace_leases=workspace_leases,
                recovery=recovery,
                executor=executor,
                errors=attempt_errors,
            )
            completed = await runs.get(_TENANT_ID, run_id)
            replay = await api.get(
                f"/v1/runs/{run_id}/events?limit=100",
                headers=authorization,
            )
            assert replay.status_code == 200
            replayed_events = replay.json()["events"]
            failures = [
                item["payload"] for item in replayed_events if item["event_type"] == "run.failed"
            ]
            event_types = [item["event_type"] for item in replayed_events]
            assert completed is not None and completed.status is RunStatus.COMPLETED, (
                failures,
                attempt_errors,
                scripted.remaining_turns,
                event_types,
            )
            assert scripted.remaining_turns == 2
            all_model_context = "\n".join(
                message.content for request in scripted.requests for message in request.messages
            )
            assert "Follow the calculator fixture instructions." in all_model_context
            assert "Referenced file `README.md`" in all_model_context
            assert "This fixture is used to verify distributed coding-agent execution." in (
                all_model_context
            )
            assert "return left + right" in all_model_context

            assert [item["sequence"] for item in replayed_events] == list(
                range(1, len(replayed_events) + 1)
            )
            assert event_types.count("tool.approval_required") == 2
            started_tool_calls = [
                item["payload"]["tool_call_id"]
                for item in replayed_events
                if item["event_type"] == "tool.started"
            ]
            tool_event_summary = [
                (
                    item["event_type"],
                    item["payload"].get("tool_call_id"),
                    (item["payload"].get("error") or {}).get("code"),
                    (item["payload"].get("error") or {}).get("details"),
                )
                for item in replayed_events
                if item["event_type"].startswith("tool.")
            ]
            async with database.sessions() as diagnostic_query:
                diagnostic_tool_rows = tuple(
                    (
                        await diagnostic_query.scalars(
                            select(ToolCallRecord)
                            .where(
                                ToolCallRecord.tenant_id == _TENANT_ID,
                                ToolCallRecord.run_id == run_id,
                            )
                            .order_by(ToolCallRecord.tool_call_id)
                        )
                    ).all()
                )
            tool_state_summary = [
                (
                    row.tool_call_id,
                    row.status,
                    row.started_at is not None,
                    (row.error or {}).get("code"),
                )
                for row in diagnostic_tool_rows
            ]
            assert started_tool_calls == ["distributed-edit", "distributed-tests"], (
                started_tool_calls,
                tool_event_summary,
                tool_state_summary,
            )
            assert event_types.count("checkpoint.created") == 2
            assert event_types[-1] == "run.completed"
            output = "".join(
                item["payload"].get("chunk", "")
                for item in replayed_events
                if item["event_type"] in {"tool.stdout", "tool.stderr"}
            )
            assert "OK" in output

            artifacts_response = await api.get(
                f"/v1/workspaces/{workspace_id}/artifacts?run_id={run_id}",
                headers=authorization,
            )
            assert artifacts_response.status_code == 200
            artifacts = artifacts_response.json()["artifacts"]
            assert len(artifacts) == 1
            final_artifact = artifacts[0]
            download_response = await api.get(
                f"/v1/artifacts/{final_artifact['id']}/download",
                headers=authorization,
            )
            assert download_response.status_code == 200
            assert (
                await api.get(
                    f"/v1/artifacts/{final_artifact['id']}/download",
                    headers={"authorization": f"Bearer {_OTHER_TOKEN}"},
                )
            ).status_code == 404
            async with httpx.AsyncClient(trust_env=False) as download_client:
                patch_response = await download_client.get(
                    download_response.json()["download"]["url"]
                )
            assert patch_response.status_code == 200
            patch = patch_response.content
            assert b"return left + right" in patch
            assert hashlib.sha256(patch).hexdigest() == final_artifact["object"]["sha256"]

            async with database.sessions() as query:
                tool_rows = tuple(
                    (
                        await query.scalars(
                            select(ToolCallRecord)
                            .where(
                                ToolCallRecord.tenant_id == _TENANT_ID,
                                ToolCallRecord.run_id == run_id,
                            )
                            .order_by(ToolCallRecord.tool_call_id)
                        )
                    ).all()
                )
                checkpoint_rows = tuple(
                    (
                        await query.scalars(
                            select(CheckpointRecord)
                            .where(
                                CheckpointRecord.tenant_id == _TENANT_ID,
                                CheckpointRecord.run_id == run_id,
                            )
                            .order_by(CheckpointRecord.created_at)
                        )
                    ).all()
                )
            assert [row.tool_call_id for row in tool_rows] == [
                "distributed-edit",
                "distributed-tests",
            ]
            assert all(row.status == ToolCallStatus.COMPLETED.value for row in tool_rows)
            command_result = tool_rows[1].result
            assert command_result is not None and command_result["exit_code"] == 0
            assert len(checkpoint_rows) == 2
            for row in checkpoint_rows:
                completed_snapshot_uri = row.completed_snapshot_uri
                assert completed_snapshot_uri is not None
                reference = DurableSnapshotReference.from_uri(completed_snapshot_uri)
                assert await objects.head(reference.object_key) is not None

            rewind_response = await api.post(
                f"/v1/runs/{run_id}/rewind",
                headers=authorization,
                json={"checkpoint_id": str(checkpoint_rows[0].id)},
            )
            assert rewind_response.status_code == 200
            rewound = rewind_response.json()
            assert rewound["status"] == RunStatus.QUEUED.value
            assert rewound["attempt"] == 2
            assert rewound["execution_epoch"] == 2

            await _run_attempt(
                "distributed-worker-d",
                queue=queue,
                workspace_leases=workspace_leases,
                recovery=recovery,
                executor=executor,
                errors=attempt_errors,
            )
            replacement_waiting = await runs.get(_TENANT_ID, run_id)
            assert replacement_waiting is not None
            assert replacement_waiting.status is RunStatus.WAITING_APPROVAL
            assert replacement_waiting.execution_epoch == 2
            replacement_approval = await _approval_id(api, run_id, 2)
            replacement_approved = await api.post(
                f"/v1/runs/{run_id}/approvals/{replacement_approval}",
                headers=authorization,
                json={"approved": True},
            )
            assert replacement_approved.status_code == 200

            await _run_attempt(
                "distributed-worker-e",
                queue=queue,
                workspace_leases=workspace_leases,
                recovery=recovery,
                executor=executor,
                errors=attempt_errors,
            )
            replacement_completed = await runs.get(_TENANT_ID, run_id)
            assert replacement_completed is not None
            assert replacement_completed.status is RunStatus.COMPLETED
            assert replacement_completed.execution_epoch == 2
            assert scripted.remaining_turns == 0

            replacement_replay = await api.get(
                f"/v1/runs/{run_id}/events?limit=100",
                headers=authorization,
            )
            assert replacement_replay.status_code == 200
            replacement_events = replacement_replay.json()["events"]
            assert [item["sequence"] for item in replacement_events] == list(
                range(1, len(replacement_events) + 1)
            )
            assert sum(item["event_type"] == "run.completed" for item in replacement_events) == 2
            assert (
                sum(item["event_type"] == "tool.approval_required" for item in replacement_events)
                == 3
            )
            assert {item["execution_epoch"] for item in replacement_events} == {1, 2}

            replacement_artifacts_response = await api.get(
                f"/v1/workspaces/{workspace_id}/artifacts?run_id={run_id}",
                headers=authorization,
            )
            assert replacement_artifacts_response.status_code == 200
            replacement_artifacts = replacement_artifacts_response.json()["artifacts"]
            assert len(replacement_artifacts) == 1
            replacement_artifact = replacement_artifacts[0]
            assert replacement_artifact["id"] != final_artifact["id"]
            assert replacement_artifact["execution_epoch"] == 2
            assert (
                "/branches/2/artifacts/final.patch" in replacement_artifact["object"]["object_key"]
            )
            replacement_download = await api.get(
                f"/v1/artifacts/{replacement_artifact['id']}/download",
                headers=authorization,
            )
            assert replacement_download.status_code == 200
            async with httpx.AsyncClient(trust_env=False) as download_client:
                replacement_patch_response = await download_client.get(
                    replacement_download.json()["download"]["url"]
                )
            assert replacement_patch_response.status_code == 200
            replacement_patch = replacement_patch_response.content
            assert b"return sum((left, right))" in replacement_patch
            assert b"return left + right" not in replacement_patch
            assert (
                hashlib.sha256(replacement_patch).hexdigest()
                == replacement_artifact["object"]["sha256"]
            )

            abandoned_download = await api.get(
                f"/v1/artifacts/{final_artifact['id']}/download",
                headers=authorization,
            )
            assert abandoned_download.status_code == 200

            async with database.sessions() as branch_query:
                branch_tool_rows = tuple(
                    (
                        await branch_query.scalars(
                            select(ToolCallRecord)
                            .where(
                                ToolCallRecord.tenant_id == _TENANT_ID,
                                ToolCallRecord.run_id == run_id,
                            )
                            .order_by(
                                ToolCallRecord.execution_epoch,
                                ToolCallRecord.tool_call_id,
                            )
                        )
                    ).all()
                )
                branch_checkpoint_rows = tuple(
                    (
                        await branch_query.scalars(
                            select(CheckpointRecord)
                            .where(
                                CheckpointRecord.tenant_id == _TENANT_ID,
                                CheckpointRecord.run_id == run_id,
                            )
                            .order_by(
                                CheckpointRecord.execution_epoch,
                                CheckpointRecord.created_at,
                            )
                        )
                    ).all()
                )
            assert [(row.execution_epoch, row.tool_call_id) for row in branch_tool_rows] == [
                (1, "distributed-edit"),
                (1, "distributed-tests"),
                (2, "distributed-edit"),
            ]
            assert all(row.status == ToolCallStatus.COMPLETED.value for row in branch_tool_rows)
            assert [row.execution_epoch for row in branch_checkpoint_rows] == [1, 1, 2]
    finally:
        if factory is not None:
            with suppress(Exception):
                await factory.aclose()
        if server_task is not None:
            server_task.cancel()
            with suppress(asyncio.CancelledError):
                await server_task
        await gateway.aclose()
        await objects.aclose()
        await database.aclose()
        telemetry.shutdown()
