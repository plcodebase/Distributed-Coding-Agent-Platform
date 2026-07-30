from __future__ import annotations

import asyncio
import threading
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from agent_api import ApiServices, Principal, StaticTokenAuthenticator, create_app
from agent_api.factory import AgentApiSettings, _credentials
from agent_core.control import (
    ApprovalDecision,
    ApprovalStatus,
    PersistedApproval,
    RunCreationResult,
)
from agent_core.domain.base import FrozenJsonObject
from agent_core.domain.errors import DomainOperationError
from agent_core.domain.models import Run, Session
from agent_core.domain.status import ApprovalMode, RunStatus, SessionStatus
from agent_core.domain.transitions import transition_run
from agent_core.event_store import EventPage, StoredEvent

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


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
    api_services, _, _, _ = services()
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
        json={"priority": 7},
    )
    second = client.post(
        f"/v1/sessions/{session_id}/runs",
        headers=headers,
        json={"priority": 7},
    )
    conflict = client.post(
        f"/v1/sessions/{session_id}/runs",
        headers=headers,
        json={"priority": 8},
    )

    assert first.status_code == 200
    assert first.json()["created"] is True
    assert second.json()["created"] is False
    assert second.json()["run"]["id"] == first.json()["run"]["id"]
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "run_idempotency_conflict"


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
