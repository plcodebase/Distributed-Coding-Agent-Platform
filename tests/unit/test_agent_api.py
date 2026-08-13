from __future__ import annotations

import asyncio
import json
import threading
import uuid
from collections import deque
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

import pytest
from fastapi.testclient import TestClient
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from starlette.websockets import WebSocketDisconnect

from agent_api import (
    ApiServices,
    EventGatewayServices,
    Principal,
    RequestBodyLimitMiddleware,
    StaticTokenAuthenticator,
    create_app,
    create_event_gateway_app,
)
from agent_api.event_factory import (
    EventGatewaySettings,
    create_production_event_gateway_app,
)
from agent_api.factory import AgentApiSettings, _credentials
from agent_core.control import (
    ApprovalDecision,
    ApprovalStatus,
    ContextCompactionStatus,
    MemoryKind,
    PersistedApproval,
    PersistedContextCompaction,
    PersistedMemory,
    PersistedTaskState,
    RunCreationResult,
    TaskPlanUpdate,
    memory_content_hash,
)
from agent_core.domain.base import FrozenJsonObject
from agent_core.domain.errors import DomainOperationError
from agent_core.domain.models import Run, Session
from agent_core.domain.status import ApprovalMode, RunStatus, SessionStatus
from agent_core.domain.transitions import transition_run
from agent_core.event_store import EventPage, StoredEvent
from platform_telemetry import PlatformTelemetry, TelemetrySettings

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from starlette.types import ASGIApp, Message, Scope


TENANT_A = uuid.UUID("00000000-0000-0000-0000-000000000001")
TENANT_B = uuid.UUID("00000000-0000-0000-0000-000000000002")
WORKSPACE_ID = uuid.UUID("00000000-0000-0000-0000-000000000010")
CHECKPOINT_ID = uuid.UUID("00000000-0000-0000-0000-000000000020")
TOKEN_A = "token-a"  # noqa: S105 - inert test credential


class SessionRepositoryFake:
    def __init__(self) -> None:
        self.values: dict[tuple[uuid.UUID, uuid.UUID], Session] = {}

    async def create(self, session: Session) -> Session:
        self.values[(session.tenant_id, session.id)] = session
        return session

    async def get(self, tenant_id: uuid.UUID, session_id: uuid.UUID) -> Session | None:
        return self.values.get((tenant_id, session_id))


class BrokenSessionRepository(SessionRepositoryFake):
    async def create(self, session: Session) -> Session:
        del session
        raise RuntimeError("database-secret-must-not-escape")


class RunRepositoryFake:
    def __init__(self) -> None:
        self.values: dict[tuple[uuid.UUID, uuid.UUID], Run] = {}
        self.idempotency: dict[tuple[uuid.UUID, uuid.UUID, str], tuple[str, Run]] = {}
        self.cancel_calls = 0

    async def create_idempotent(
        self,
        tenant_id: uuid.UUID,
        run: Run,
        *,
        idempotency_key: str,
        creation_hash: str,
    ) -> RunCreationResult:
        key = (tenant_id, run.session_id, idempotency_key)
        existing = self.idempotency.get(key)
        if existing is not None:
            stored_hash, stored_run = existing
            if stored_hash != creation_hash:
                raise DomainOperationError(
                    code="run_idempotency_conflict",
                    message="the key belongs to another payload",
                )
            return RunCreationResult(run=stored_run, created=False)
        self.values[(tenant_id, run.id)] = run
        self.idempotency[key] = (creation_hash, run)
        return RunCreationResult(run=run, created=True)

    async def get(self, tenant_id: uuid.UUID, run_id: uuid.UUID) -> Run | None:
        return self.values.get((tenant_id, run_id))

    async def request_cancel(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        *,
        occurred_at: datetime,
    ) -> Run | None:
        self.cancel_calls += 1
        run = self.values.get((tenant_id, run_id))
        if run is None:
            return None
        cancelled = transition_run(run, RunStatus.CANCELLED, occurred_at=occurred_at)
        self.values[(tenant_id, run_id)] = cancelled
        return cancelled

    async def rewind(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        checkpoint_id: uuid.UUID,
    ) -> Run | None:
        if checkpoint_id != CHECKPOINT_ID:
            return None
        run = self.values.get((tenant_id, run_id))
        if run is None:
            return None
        rewound = run.model_copy(update={"last_checkpoint_id": checkpoint_id})
        self.values[(tenant_id, run_id)] = rewound
        return rewound


class BrokenRunRepository(RunRepositoryFake):
    async def get(self, tenant_id: uuid.UUID, run_id: uuid.UUID) -> Run | None:
        del tenant_id, run_id
        raise DomainOperationError(
            code="persistence_unavailable",
            message="repository details must not alter the authentication result",
            retryable=True,
        )


class OverloadedRunRepository(RunRepositoryFake):
    async def create_idempotent(
        self,
        tenant_id: uuid.UUID,
        run: Run,
        *,
        idempotency_key: str,
        creation_hash: str,
    ) -> RunCreationResult:
        del tenant_id, run, idempotency_key, creation_hash
        raise DomainOperationError(
            code="queue_overloaded",
            message="the global run queue has reached its admission threshold",
            retryable=True,
            details={"retry_after_seconds": 2.25, "scope": "global_queue"},
        )


class ApprovalRepositoryFake:
    def __init__(self) -> None:
        self.values: dict[tuple[uuid.UUID, uuid.UUID, uuid.UUID], PersistedApproval] = {}

    async def decide(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        approval_id: uuid.UUID,
        decision: ApprovalDecision,
    ) -> PersistedApproval | None:
        current = self.values.get((tenant_id, run_id, approval_id))
        if current is None:
            return None
        decided = current.model_copy(
            update={
                "status": (
                    ApprovalStatus.APPROVED if decision.approved else ApprovalStatus.REJECTED
                ),
                "decided_by": decision.decided_by,
                "decided_at": decision.decided_at,
            }
        )
        self.values[(tenant_id, run_id, approval_id)] = decided
        return decided


class EventStoreFake:
    def __init__(self) -> None:
        self.values: dict[tuple[uuid.UUID, uuid.UUID], tuple[StoredEvent, ...]] = {}

    async def read_page(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        *,
        after: int = 0,
        limit: int = 100,
    ) -> EventPage:
        selected = tuple(
            event for event in self.values.get((tenant_id, run_id), ()) if event.sequence > after
        )
        retained = selected[:limit]
        return EventPage(
            events=retained,
            next_after=retained[-1].sequence if retained else after,
            has_more=len(selected) > limit,
        )

    async def stream(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        *,
        after: int = 0,
        page_size: int = 100,
    ) -> AsyncIterator[StoredEvent]:
        del page_size
        for event in self.values.get((tenant_id, run_id), ()):
            if event.sequence > after:
                yield event


class BlockingEventStore(EventStoreFake):
    def __init__(self) -> None:
        super().__init__()
        self.closed = threading.Event()

    async def stream(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        *,
        after: int = 0,
        page_size: int = 100,
    ) -> AsyncIterator[StoredEvent]:
        del page_size
        try:
            for event in self.values.get((tenant_id, run_id), ()):
                if event.sequence > after:
                    yield event
            await asyncio.Event().wait()
        finally:
            self.closed.set()


class ReadinessFake:
    def __init__(self, ready: bool = True) -> None:
        self.value = ready

    async def ready(self) -> bool:
        return self.value


class ContextRepositoryFake:
    def __init__(self) -> None:
        self.values: dict[tuple[uuid.UUID, uuid.UUID, str], PersistedContextCompaction] = {}

    async def request_compaction(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        *,
        compaction_id: uuid.UUID,
        idempotency_key: str,
        route_name: str,
        requested_at: datetime,
    ) -> PersistedContextCompaction | None:
        key = (tenant_id, session_id, idempotency_key)
        existing = self.values.get(key)
        if existing is not None:
            if existing.route_name != route_name:
                raise DomainOperationError(
                    code="context_compaction_idempotency_conflict",
                    message="the key belongs to another request",
                )
            return existing
        pending = next(
            (
                value
                for (stored_tenant, stored_session, _), value in self.values.items()
                if stored_tenant == tenant_id
                and stored_session == session_id
                and value.status is ContextCompactionStatus.PENDING
            ),
            None,
        )
        if pending is not None:
            raise DomainOperationError(
                code="context_compaction_in_progress",
                message="the session already has a pending context compaction",
                retryable=True,
                details={"compaction_id": str(pending.id)},
            )
        value = PersistedContextCompaction(
            id=compaction_id,
            session_id=session_id,
            status=ContextCompactionStatus.PENDING,
            idempotency_key=idempotency_key,
            source_message_sequence=0,
            route_name=route_name,
            requested_at=requested_at,
        )
        self.values[key] = value
        return value

    async def latest_completed(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
    ) -> PersistedContextCompaction | None:
        matches = [
            value
            for (stored_tenant, stored_session, _), value in self.values.items()
            if stored_tenant == tenant_id
            and stored_session == session_id
            and value.status is ContextCompactionStatus.COMPLETED
        ]
        return matches[-1] if matches else None

    async def get(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        compaction_id: uuid.UUID,
    ) -> PersistedContextCompaction | None:
        return next(
            (
                value
                for (stored_tenant, stored_session, _), value in self.values.items()
                if stored_tenant == tenant_id
                and stored_session == session_id
                and value.id == compaction_id
            ),
            None,
        )


class TaskRepositoryFake:
    def __init__(self) -> None:
        self.values: dict[tuple[uuid.UUID, uuid.UUID], PersistedTaskState] = {}

    async def get(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
    ) -> PersistedTaskState | None:
        return self.values.get((tenant_id, run_id))

    async def update(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        update: TaskPlanUpdate,
        *,
        plan_id: uuid.UUID,
        created_at: datetime,
    ) -> PersistedTaskState | None:
        current = self.values.get((tenant_id, run_id))
        version = current.version if current is not None else 0
        if version != update.expected_version:
            raise DomainOperationError(
                code="task_plan_version_conflict",
                message="the task plan changed since it was read",
            )
        state = PersistedTaskState(
            id=plan_id,
            run_id=run_id,
            version=version + 1,
            tasks=update.tasks,
            created_at=created_at,
        )
        self.values[(tenant_id, run_id)] = state
        return state


class MemoryRepositoryFake:
    def __init__(self, sessions: SessionRepositoryFake) -> None:
        self.sessions = sessions
        self.values: dict[tuple[uuid.UUID, uuid.UUID], tuple[PersistedMemory, ...]] = {}

    async def set_session_enabled(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        *,
        enabled: bool,
        updated_at: datetime,
    ) -> bool:
        session = await self.sessions.get(tenant_id, session_id)
        if session is None:
            return False
        self.sessions.values[(tenant_id, session_id)] = session.model_copy(
            update={"memory_enabled": enabled, "updated_at": updated_at}
        )
        return True

    async def list_active(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        *,
        limit: int = 100,
    ) -> tuple[PersistedMemory, ...]:
        session = await self.sessions.get(tenant_id, session_id)
        if session is None or not session.memory_enabled:
            return ()
        return self.values.get((tenant_id, session_id), ())[:limit]

    async def archive(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        memory_id: uuid.UUID,
        *,
        archived_at: datetime,
    ) -> PersistedMemory | None:
        key = (tenant_id, session_id)
        values = self.values.get(key, ())
        for index, memory in enumerate(values):
            if memory.id == memory_id and memory.archived_at is None:
                archived = memory.model_copy(update={"archived_at": archived_at})
                self.values[key] = (*values[:index], archived, *values[index + 1 :])
                return archived
        return None


def services() -> tuple[ApiServices, SessionRepositoryFake, RunRepositoryFake, EventStoreFake]:
    sessions = SessionRepositoryFake()
    runs = RunRepositoryFake()
    events = EventStoreFake()
    return (
        ApiServices(
            authenticator=StaticTokenAuthenticator(
                {
                    "token-a": Principal(tenant_id=TENANT_A, subject="user-a"),
                    "token-b": Principal(tenant_id=TENANT_B, subject="user-b"),
                }
            ),
            sessions=sessions,
            runs=runs,
            approvals=ApprovalRepositoryFake(),
            events=events,
            readiness=ReadinessFake(),
            context=ContextRepositoryFake(),
            tasks=TaskRepositoryFake(),
            memories=MemoryRepositoryFake(sessions),
        ),
        sessions,
        runs,
        events,
    )


def authorization(token: str | None = None) -> dict[str, str]:
    return {"Authorization": f"Bearer {token or TOKEN_A}"}


def test_health_and_authentication_boundary() -> None:
    api_services, _, _, _ = services()
    client = TestClient(create_app(api_services))

    assert client.get("/health/live").json() == {"status": "ok"}
    assert client.get("/health/ready").json() == {"status": "ok"}
    unauthenticated = client.post(
        "/v1/sessions",
        json={"workspace_id": str(WORKSPACE_ID)},
    )
    assert unauthenticated.status_code == 401
    assert unauthenticated.headers["www-authenticate"] == "Bearer"
    assert unauthenticated.json()["error"]["code"] == "authentication_required"


def test_event_gateway_exposes_only_health_metrics_and_event_routes() -> None:
    api_services, sessions, runs, events = services()
    now = datetime.now(UTC)
    session = _session(TENANT_A, now)
    run = _run(session, now)
    sessions.values[(TENANT_A, session.id)] = session
    runs.values[(TENANT_A, run.id)] = run
    events.values[(TENANT_A, run.id)] = (_event(run.id, 1, now),)
    event_services = EventGatewayServices(
        authenticator=api_services.authenticator,
        runs=runs,
        events=events,
        readiness=api_services.readiness,
    )
    application = create_event_gateway_app(event_services)
    client = TestClient(application)

    response = client.get(f"/v1/runs/{run.id}/events", headers=authorization())
    assert response.status_code == 200
    assert len(response.json()["events"]) == 1
    assert client.get("/health/ready").status_code == 200
    assert client.post("/v1/sessions", headers=authorization(), json={}).status_code == 404
    paths = {
        path
        for route in application.routes
        if isinstance(path := getattr(route, "path", None), str)
    }
    assert "/v1/sessions" not in paths
    assert "/v1/runs/{run_id}/cancel" not in paths


def test_event_gateway_health_auth_validation_metrics_and_close() -> None:
    api_services, _, runs, events = services()
    close_calls = 0

    async def close() -> None:
        nonlocal close_calls
        close_calls += 1

    event_services = EventGatewayServices(
        authenticator=api_services.authenticator,
        runs=runs,
        events=events,
        readiness=ReadinessFake(False),
    )
    telemetry = PlatformTelemetry(TelemetrySettings(service_name="event-gateway"))
    token = "event-metrics-token"  # noqa: S105 - inert test credential
    application = create_event_gateway_app(
        event_services,
        close=close,
        telemetry=telemetry,
        metrics_token=token,
    )
    with TestClient(application) as client:
        assert client.get("/health/live").json() == {"status": "ok"}
        assert client.get("/health/ready").status_code == 503
        assert client.get("/metrics").status_code == 401
        assert client.get("/metrics", headers=authorization("wrong-token")).status_code == 401
        metrics = client.get("/metrics", headers=authorization(token))
        assert metrics.status_code == 200
        assert "agent_platform" in metrics.text
        run_id = uuid.uuid4()
        assert client.get(f"/v1/runs/{run_id}/events").status_code == 401
        assert (
            client.get(
                f"/v1/runs/{run_id}/events?limit=0",
                headers=authorization(),
            ).status_code
            == 422
        )
        missing = client.get(f"/v1/runs/{run_id}/events", headers=authorization())
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "resource_not_found"
    assert close_calls == 1
    telemetry.shutdown()

    no_metrics = TestClient(create_event_gateway_app(event_services))
    assert no_metrics.get("/metrics").status_code == 404
    with pytest.raises(ValueError, match="body_bytes"):
        create_event_gateway_app(event_services, max_request_body_bytes=0)
    with pytest.raises(ValueError, match="metrics_token"):
        create_event_gateway_app(
            event_services,
            metrics_token="short",  # noqa: S106 - deliberately invalid test value
        )


def test_event_gateway_websocket_auth_not_found_replay_and_reconnect_metric() -> None:
    api_services, sessions, runs, events = services()
    now = datetime.now(UTC)
    session = _session(TENANT_A, now)
    run = _run(session, now)
    sessions.values[(TENANT_A, session.id)] = session
    runs.values[(TENANT_A, run.id)] = run
    events.values[(TENANT_A, run.id)] = (_event(run.id, 1, now), _event(run.id, 2, now))
    event_services = EventGatewayServices(
        authenticator=api_services.authenticator,
        runs=runs,
        events=events,
        readiness=api_services.readiness,
    )
    telemetry = PlatformTelemetry(TelemetrySettings(service_name="event-gateway-websocket"))
    client = TestClient(create_event_gateway_app(event_services, telemetry=telemetry))

    with (
        pytest.raises(WebSocketDisconnect) as unauthenticated,
        client.websocket_connect(f"/v1/runs/{run.id}/stream"),
    ):
        pass
    assert unauthenticated.value.code == 4401
    with (
        pytest.raises(WebSocketDisconnect) as missing,
        client.websocket_connect(
            f"/v1/runs/{uuid.uuid4()}/stream",
            headers=authorization(),
        ),
    ):
        pass
    assert missing.value.code == 4404
    with client.websocket_connect(
        f"/v1/runs/{run.id}/stream?after=1",
        headers=authorization(),
    ) as websocket:
        assert websocket.receive_json()["sequence"] == 2
    rendered = telemetry.metrics.render().decode("utf-8")
    assert "agent_platform_event_reconnects_total 1.0" in rendered
    telemetry.shutdown()


def test_production_event_gateway_factory_builds_event_only_graph() -> None:
    credentials = json.dumps(
        {
            TOKEN_A: {
                "tenant_id": str(TENANT_A),
                "subject": "event-reader",
            }
        }
    )
    application = create_production_event_gateway_app(
        event_settings=EventGatewaySettings(
            event_credentials_json=credentials,
            telemetry_environment="test",
            metrics_token="event-metrics-token",  # noqa: S106 - inert test credential
        )
    )

    with TestClient(application) as client:
        assert client.get("/health/live").status_code == 200
        paths = {
            path
            for route in application.routes
            if isinstance(path := getattr(route, "path", None), str)
        }
        assert "/v1/runs/{run_id}/events" in paths
        assert "/v1/sessions" not in paths


def test_authentication_and_validation_failures_are_header_safe_and_opaque() -> None:
    principal = Principal(tenant_id=TENANT_A, subject="user-a")
    with pytest.raises(ValueError):
        StaticTokenAuthenticator({"bad token": principal})
    with pytest.raises(ValueError):
        StaticTokenAuthenticator({"tökén": principal})
    with pytest.raises(TypeError):
        StaticTokenAuthenticator({"token": object()})  # type: ignore[dict-item]
    with pytest.raises(TypeError):
        StaticTokenAuthenticator([("token", principal)])  # type: ignore[arg-type]

    api_services, _, _, _ = services()
    client = TestClient(create_app(api_services))
    known_value = "invalid-input-must-not-be-reflected"
    invalid_body = client.post(
        "/v1/sessions",
        headers=authorization(),
        json={
            "workspace_id": str(WORKSPACE_ID),
            "unexpected": known_value,
        },
    )
    invalid_header = client.post(
        f"/v1/sessions/{uuid.uuid4()}/runs",
        headers={**authorization(), "Idempotency-Key": f"bad {known_value}"},
        json={},
    )

    for response in (invalid_body, invalid_header):
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "invalid_request"
        assert known_value not in response.text


def test_static_credential_json_is_secret_safe_and_rejects_duplicate_keys() -> None:
    settings = AgentApiSettings(
        api_credentials_json=(
            '{"token-a":{"tenant_id":"00000000-0000-0000-0000-000000000001","subject":"user-a"}}'
        )
    )
    assert _credentials(settings)["token-a"].tenant_id == TENANT_A
    assert "token-a" not in repr(settings)

    duplicate_token = AgentApiSettings(
        api_credentials_json=(
            '{"token-a":{"tenant_id":"00000000-0000-0000-0000-000000000001",'
            '"subject":"user-a"},"token-a":{"tenant_id":'
            '"00000000-0000-0000-0000-000000000002","subject":"user-b"}}'
        )
    )
    duplicate_principal_key = AgentApiSettings(
        api_credentials_json=(
            '{"token-a":{"tenant_id":"00000000-0000-0000-0000-000000000001",'
            '"subject":"user-a","subject":"user-b"}}'
        )
    )
    with pytest.raises(ValueError, match="duplicate"):
        _credentials(duplicate_token)
    with pytest.raises(ValueError, match="duplicate"):
        _credentials(duplicate_principal_key)


def test_session_and_run_creation_are_tenant_scoped_and_idempotent() -> None:
    api_services, sessions, _, _ = services()
    client = TestClient(create_app(api_services))
    created_session = client.post(
        "/v1/sessions",
        headers=authorization(),
        json={"workspace_id": str(WORKSPACE_ID)},
    )
    assert created_session.status_code == 201
    session_id = created_session.json()["id"]

    assert (
        client.get(f"/v1/sessions/{session_id}", headers=authorization("token-b")).status_code
        == 404
    )
    headers = {**authorization(), "Idempotency-Key": "create-run-1"}
    first = client.post(
        f"/v1/sessions/{session_id}/runs",
        headers=headers,
        json={"priority": 7, "priority_class": "background"},
    )
    second = client.post(
        f"/v1/sessions/{session_id}/runs",
        headers=headers,
        json={"priority": 7, "priority_class": "background"},
    )
    conflict = client.post(
        f"/v1/sessions/{session_id}/runs",
        headers=headers,
        json={"priority": 8},
    )

    assert first.status_code == 200
    assert first.json()["created"] is True
    assert first.json()["run"]["priority_class"] == "background"
    assert second.json()["created"] is False
    assert second.json()["run"]["id"] == first.json()["run"]["id"]
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "run_idempotency_conflict"

    stored_session = sessions.values[(TENANT_A, uuid.UUID(session_id))]
    sessions.values[(TENANT_A, stored_session.id)] = stored_session.model_copy(
        update={"status": SessionStatus.COMPLETED}
    )
    inactive = client.post(
        f"/v1/sessions/{session_id}/runs",
        headers={**authorization(), "Idempotency-Key": "inactive-session"},
        json={},
    )
    assert inactive.status_code == 409
    assert inactive.json()["error"]["code"] == "session_state_conflict"


def test_compact_task_status_and_memory_operations_are_durable_and_tenant_scoped() -> None:
    api_services, _, _, _ = services()
    client = TestClient(create_app(api_services))
    session = client.post(
        "/v1/sessions",
        headers=authorization(),
        json={"workspace_id": str(WORKSPACE_ID), "memory_enabled": True},
    ).json()
    session_id = uuid.UUID(session["id"])
    compact_headers = {**authorization(), "Idempotency-Key": "compact-1"}
    first_compaction = client.post(
        f"/v1/sessions/{session_id}/compact",
        headers=compact_headers,
    )
    replay = client.post(
        f"/v1/sessions/{session_id}/compact",
        headers=compact_headers,
    )
    denied = client.post(
        f"/v1/sessions/{session_id}/compact",
        headers={**authorization("token-b"), "Idempotency-Key": "compact-1"},
    )
    assert first_compaction.status_code == 202
    assert first_compaction.json()["status"] == "pending"
    assert first_compaction.json()["route_name"] == "summarization"
    assert replay.json()["id"] == first_compaction.json()["id"]
    assert denied.status_code == 404
    concurrent = client.post(
        f"/v1/sessions/{session_id}/compact",
        headers={**authorization(), "Idempotency-Key": "compact-2"},
    )
    assert concurrent.status_code == 409
    assert concurrent.json()["error"]["code"] == "context_compaction_in_progress"
    compaction_status = client.get(
        f"/v1/sessions/{session_id}/compactions/{first_compaction.json()['id']}",
        headers=authorization(),
    )
    hidden_compaction = client.get(
        f"/v1/sessions/{session_id}/compactions/{first_compaction.json()['id']}",
        headers=authorization("token-b"),
    )
    assert compaction_status.json()["id"] == first_compaction.json()["id"]
    assert hidden_compaction.status_code == 404

    run = client.post(
        f"/v1/sessions/{session_id}/runs",
        headers={**authorization(), "Idempotency-Key": "task-run"},
        json={},
    ).json()["run"]
    run_id = uuid.UUID(run["id"])
    plan = client.put(
        f"/v1/runs/{run_id}/task-plan",
        headers=authorization(),
        json={
            "expected_version": 0,
            "tasks": [
                {
                    "id": "task-1",
                    "title": "Finish context persistence",
                    "status": "in_progress",
                }
            ],
        },
    )
    stale = client.put(
        f"/v1/runs/{run_id}/task-plan",
        headers=authorization(),
        json={"expected_version": 0, "tasks": []},
    )
    status = client.get(f"/v1/runs/{run_id}/status", headers=authorization())
    assert plan.status_code == 200
    assert plan.json()["version"] == 1
    assert stale.status_code == 409
    assert status.json()["task_plan"]["tasks"][0]["id"] == "task-1"

    memories = cast("MemoryRepositoryFake", api_services.memories)
    memory = PersistedMemory(
        id=uuid.uuid4(),
        tenant_id=TENANT_A,
        session_id=session_id,
        source_run_id=run_id,
        kind=MemoryKind.DECISION,
        content="Use the bounded context pipeline.",
        content_hash=memory_content_hash("Use the bounded context pipeline."),
        extracted_at=datetime.now(UTC),
    )
    memories.values[(TENANT_A, session_id)] = (memory,)
    listed = client.get(f"/v1/sessions/{session_id}/memories", headers=authorization())
    denied_archive = client.delete(
        f"/v1/sessions/{session_id}/memories/{memory.id}",
        headers=authorization("token-b"),
    )
    archived = client.delete(
        f"/v1/sessions/{session_id}/memories/{memory.id}",
        headers=authorization(),
    )
    archived_replay = client.delete(
        f"/v1/sessions/{session_id}/memories/{memory.id}",
        headers=authorization(),
    )
    disabled = client.patch(
        f"/v1/sessions/{session_id}/memory",
        headers=authorization(),
        json={"enabled": False},
    )
    hidden = client.get(f"/v1/sessions/{session_id}/memories", headers=authorization())
    assert listed.json()["memories"][0]["source_run_id"] == str(run_id)
    assert denied_archive.status_code == 404
    assert archived.status_code == 200
    assert archived.json()["archived_at"] is not None
    assert archived_replay.status_code == 404
    assert disabled.json()["memory_enabled"] is False
    assert hidden.json()["memories"] == []


def test_run_overload_response_is_structured_and_retryable() -> None:
    api_services, sessions, _, _ = services()
    overloaded = ApiServices(
        authenticator=api_services.authenticator,
        sessions=sessions,
        runs=OverloadedRunRepository(),
        approvals=api_services.approvals,
        events=api_services.events,
        readiness=api_services.readiness,
    )
    client = TestClient(create_app(overloaded))
    session = client.post(
        "/v1/sessions",
        headers=authorization(),
        json={"workspace_id": str(WORKSPACE_ID)},
    ).json()

    response = client.post(
        f"/v1/sessions/{session['id']}/runs",
        headers={**authorization(), "Idempotency-Key": "overload-1"},
        json={},
    )
    assert response.status_code == 429
    assert response.headers["Retry-After"] == "3"
    assert response.json()["error"]["code"] == "queue_overloaded"
    assert response.json()["error"]["details"]["scope"] == "global_queue"


def test_request_body_limit_rejects_declared_oversize_and_invalid_configuration() -> None:
    api_services, _, _, _ = services()
    client = TestClient(create_app(api_services, max_request_body_bytes=4))
    response = client.post(
        "/v1/sessions",
        headers={**authorization(), "content-type": "application/json"},
        content=b"12345",
    )

    assert response.status_code == 413
    assert response.json()["error"]["code"] == "request_body_limit"
    for value in (0, True, 1024 * 1024 + 1):
        with pytest.raises(ValueError):
            create_app(api_services, max_request_body_bytes=value)


@pytest.mark.asyncio
async def test_request_body_limit_bounds_chunked_and_rejects_duplicate_lengths() -> None:
    called = False

    async def downstream(
        scope: Scope,
        receive: object,
        send: object,
    ) -> None:
        del scope, receive, send
        nonlocal called
        called = True

    async def invoke(
        headers: list[tuple[bytes, bytes]],
        incoming: list[Message],
    ) -> list[Message]:
        queued = deque(incoming)
        sent: list[Message] = []

        async def receive() -> Message:
            return queued.popleft()

        async def send(message: Message) -> None:
            sent.append(message)

        middleware = RequestBodyLimitMiddleware(
            cast("ASGIApp", downstream),
            max_body_bytes=4,
        )
        scope = cast("Scope", {"type": "http", "headers": headers})
        await middleware(scope, receive, send)
        return sent

    oversized = await invoke(
        [],
        [
            {"type": "http.request", "body": b"123", "more_body": True},
            {"type": "http.request", "body": b"45", "more_body": False},
        ],
    )
    assert oversized[0]["status"] == 413
    assert called is False

    invalid = await invoke(
        [(b"content-length", b"1"), (b"content-length", b"1")],
        [{"type": "http.request", "body": b"1", "more_body": False}],
    )
    assert invalid[0]["status"] == 400
    assert called is False

    mismatched = await invoke(
        [(b"content-length", b"1")],
        [{"type": "http.request", "body": b"12", "more_body": False}],
    )
    assert mismatched[0]["status"] == 400
    assert called is False


def test_cancel_rewind_approval_and_event_replay_do_not_cross_tenants() -> None:
    api_services, _, runs, events = services()
    approvals = api_services.approvals
    assert isinstance(approvals, ApprovalRepositoryFake)
    client = TestClient(create_app(api_services))
    session = client.post(
        "/v1/sessions",
        headers=authorization(),
        json={"workspace_id": str(WORKSPACE_ID)},
    ).json()
    run = client.post(
        f"/v1/sessions/{session['id']}/runs",
        headers={**authorization(), "Idempotency-Key": "run-controls"},
        json={},
    ).json()["run"]
    run_id = uuid.UUID(run["id"])
    approval_id = uuid.uuid4()
    now = datetime.now(UTC)
    approvals.values[(TENANT_A, run_id, approval_id)] = PersistedApproval(
        id=approval_id,
        run_id=run_id,
        status=ApprovalStatus.PENDING,
        reason="sensitive command",
        arguments=FrozenJsonObject({}),
        requested_at=now,
    )
    events.values[(TENANT_A, run_id)] = (
        _event(run_id, 1, now),
        _event(run_id, 2, now),
    )

    denied = client.get(
        f"/v1/runs/{run_id}/events",
        headers=authorization("token-b"),
    )
    replay = client.get(
        f"/v1/runs/{run_id}/events?after=1",
        headers=authorization(),
    )
    approval = client.post(
        f"/v1/runs/{run_id}/approvals/{approval_id}",
        headers=authorization(),
        json={"approved": True},
    )
    rewind = client.post(
        f"/v1/runs/{run_id}/rewind",
        headers=authorization(),
        json={"checkpoint_id": str(CHECKPOINT_ID)},
    )
    cancel = client.post(f"/v1/runs/{run_id}/cancel", headers=authorization())

    assert denied.status_code == 404
    assert [event["sequence"] for event in replay.json()["events"]] == [2]
    assert approval.json()["status"] == "approved"
    assert rewind.json()["last_checkpoint_id"] == str(CHECKPOINT_ID)
    assert cancel.json()["status"] == "cancelled"
    assert runs.cancel_calls == 1


def test_websocket_replays_after_cursor_without_cancelling_run() -> None:
    api_services, sessions, runs, _ = services()
    events = BlockingEventStore()
    api_services = ApiServices(
        authenticator=api_services.authenticator,
        sessions=sessions,
        runs=runs,
        approvals=api_services.approvals,
        events=events,
        readiness=api_services.readiness,
    )
    now = datetime.now(UTC)
    session = _session(TENANT_A, now)
    sessions.values[(TENANT_A, session.id)] = session
    run = _run(session, now)
    runs.values[(TENANT_A, run.id)] = run
    events.values[(TENANT_A, run.id)] = (
        _event(run.id, 1, now),
        _event(run.id, 2, now),
    )
    client = TestClient(create_app(api_services))

    with client.websocket_connect(
        f"/v1/runs/{run.id}/stream?after=1",
        headers=authorization(),
    ) as websocket:
        assert websocket.receive_json()["sequence"] == 2

    assert events.closed.wait(timeout=1)
    assert runs.cancel_calls == 0


def test_websocket_authentication_tenant_isolation_and_repository_failures_are_distinct() -> None:
    api_services, sessions, runs, _ = services()
    now = datetime.now(UTC)
    session = _session(TENANT_A, now)
    sessions.values[(TENANT_A, session.id)] = session
    run = _run(session, now)
    runs.values[(TENANT_A, run.id)] = run
    client = TestClient(create_app(api_services))

    with (
        pytest.raises(WebSocketDisconnect) as unauthenticated,
        client.websocket_connect(f"/v1/runs/{run.id}/stream"),
    ):
        pass
    assert unauthenticated.value.code == 4401

    with (
        pytest.raises(WebSocketDisconnect) as cross_tenant,
        client.websocket_connect(
            f"/v1/runs/{run.id}/stream",
            headers=authorization("token-b"),
        ),
    ):
        pass
    assert cross_tenant.value.code == 4404

    broken_services = ApiServices(
        authenticator=api_services.authenticator,
        sessions=api_services.sessions,
        runs=BrokenRunRepository(),
        approvals=api_services.approvals,
        events=api_services.events,
        readiness=api_services.readiness,
    )
    broken_client = TestClient(create_app(broken_services))
    with (
        pytest.raises(WebSocketDisconnect) as unavailable,
        broken_client.websocket_connect(
            f"/v1/runs/{run.id}/stream",
            headers=authorization(),
        ),
    ):
        pass
    assert unavailable.value.code == 1011


def test_readiness_failure_is_503() -> None:
    api_services, _, _, _ = services()
    api_services = ApiServices(
        authenticator=api_services.authenticator,
        sessions=api_services.sessions,
        runs=api_services.runs,
        approvals=api_services.approvals,
        events=api_services.events,
        readiness=ReadinessFake(False),
    )
    response = TestClient(create_app(api_services)).get("/health/ready")
    assert response.status_code == 503
    assert response.json() == {"status": "unavailable"}


def test_unexpected_repository_failure_is_opaque() -> None:
    api_services, _, _, _ = services()
    api_services = ApiServices(
        authenticator=api_services.authenticator,
        sessions=BrokenSessionRepository(),
        runs=api_services.runs,
        approvals=api_services.approvals,
        events=api_services.events,
        readiness=api_services.readiness,
    )
    response = TestClient(create_app(api_services), raise_server_exceptions=False).post(
        "/v1/sessions",
        headers=authorization(),
        json={"workspace_id": str(WORKSPACE_ID)},
    )
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "internal_error"
    assert "database-secret-must-not-escape" not in response.text


def test_metrics_endpoint_is_bounded_and_optionally_authenticated() -> None:
    api_services, _, _, _ = services()
    telemetry = PlatformTelemetry(TelemetrySettings(service_name="agent-api"))
    token = "metrics-token-value"  # noqa: S105 - inert test credential
    client = TestClient(create_app(api_services, telemetry=telemetry, metrics_token=token))

    assert client.get("/metrics").status_code == 401
    assert client.get("/metrics", headers=authorization("wrong-token")).status_code == 401
    assert client.get("/health/live").status_code == 200
    invalid = client.post(
        "/v1/sessions",
        headers=authorization(),
        json={},
    )
    assert invalid.status_code == 422
    response = client.get("/metrics", headers=authorization(token))

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "agent_platform_api_requests_total" in response.text
    assert 'route="/health/live"' in response.text
    assert 'method="GET"' in response.text
    assert 'status="200"' in response.text
    assert 'category="validation",component="agent-api"' in response.text
    assert token not in response.text
    telemetry.shutdown()


def test_api_trace_uses_route_and_authenticated_tenant_correlation() -> None:
    api_services, _, _, _ = services()
    exporter = InMemorySpanExporter()
    telemetry = PlatformTelemetry(
        TelemetrySettings(service_name="agent-api"),
        span_exporter=exporter,
    )
    client = TestClient(create_app(api_services, telemetry=telemetry))

    response = client.post(
        "/v1/sessions",
        headers={
            **authorization(),
            "traceparent": "00-11111111111111111111111111111111-2222222222222222-01",
        },
        json={"workspace_id": str(WORKSPACE_ID)},
    )

    assert response.status_code == 201
    request_span = next(
        span for span in exporter.get_finished_spans() if span.name == "api.request"
    )
    assert request_span.context.trace_id == int("1" * 32, 16)
    attributes = request_span.attributes
    assert attributes is not None
    assert attributes["http.route"] == "/v1/sessions"
    assert attributes["http.response.status_code"] == 201
    assert attributes["agent.tenant.id"] == str(TENANT_A)
    assert str(WORKSPACE_ID) not in repr(attributes)
    telemetry.shutdown()


def test_run_creation_persists_current_w3c_context_for_worker_handoff() -> None:
    api_services, _, _, _ = services()
    exporter = InMemorySpanExporter()
    telemetry = PlatformTelemetry(
        TelemetrySettings(service_name="agent-api"),
        span_exporter=exporter,
    )
    client = TestClient(create_app(api_services, telemetry=telemetry))
    session_response = client.post(
        "/v1/sessions",
        headers=authorization(),
        json={"workspace_id": str(WORKSPACE_ID)},
    )
    session_id = session_response.json()["id"]
    exporter.clear()

    response = client.post(
        f"/v1/sessions/{session_id}/runs",
        headers={
            **authorization(),
            "Idempotency-Key": "trace-handoff",
            "traceparent": "00-33333333333333333333333333333333-4444444444444444-01",
        },
        json={},
    )

    assert response.status_code == 200
    run = response.json()["run"]
    traceparent = run["traceparent"]
    assert traceparent.startswith("00-" + "3" * 32 + "-")
    request_span = next(
        span for span in exporter.get_finished_spans() if span.name == "api.request"
    )
    assert traceparent.split("-")[2] == f"{request_span.context.span_id:016x}"
    assert run["tracestate"] is None
    replay = client.post(
        f"/v1/sessions/{session_id}/runs",
        headers={**authorization(), "Idempotency-Key": "trace-handoff"},
        json={},
    )
    assert replay.status_code == 200
    metrics = telemetry.metrics.render().decode("utf-8")
    assert "agent_platform_runs_accepted_total 1.0" in metrics
    telemetry.shutdown()


def _session(tenant_id: uuid.UUID, now: datetime) -> Session:
    return Session(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        workspace_id=WORKSPACE_ID,
        status=SessionStatus.ACTIVE,
        approval_mode=ApprovalMode.REQUIRE_SENSITIVE,
        model_route="coding-default",
        created_at=now,
        updated_at=now,
    )


def _run(session: Session, now: datetime) -> Run:
    return Run(
        id=uuid.uuid4(),
        session_id=session.id,
        workspace_id=session.workspace_id,
        status=RunStatus.QUEUED,
        priority=0,
        attempt=1,
        created_at=now,
    )


def _event(run_id: uuid.UUID, sequence: int, now: datetime) -> StoredEvent:
    return StoredEvent(
        run_id=run_id,
        sequence=sequence,
        event_type="context.build_started",
        payload=FrozenJsonObject(
            {
                "message_count": sequence,
                "checkpoint_id": None,
            }
        ),
        created_at=now,
    )
