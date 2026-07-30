from __future__ import annotations

import ast
import asyncio
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self, cast

import pytest
from pydantic import ValidationError
from sqlalchemy import CheckConstraint
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.schema import CreateTable

from agent_core.domain.errors import DomainOperationError
from agent_core.event_store import EventDraft, EventPage, StoredEvent
from platform_persistence import Base, Database, DatabaseSettings
from platform_persistence.models import GatewayRequestRecord, ToolCallRecord

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

EXPECTED_TABLES = {
    "agent_events",
    "approvals",
    "checkpoints",
    "gateway_requests",
    "gateway_rate_limits",
    "gateway_circuits",
    "messages",
    "model_calls",
    "runs",
    "sessions",
    "task_plans",
    "tool_calls",
}
TENANT_OWNED_TABLES = EXPECTED_TABLES - {"gateway_circuits"}
ROOT = Path(__file__).resolve().parents[2]


def test_metadata_contains_every_phase_five_entity_and_compiles_for_postgresql() -> None:
    assert set(Base.metadata.tables) == EXPECTED_TABLES
    engine = create_async_engine("postgresql+asyncpg://")
    try:
        for table in Base.metadata.sorted_tables:
            ddl = str(CreateTable(table).compile(dialect=engine.sync_engine.dialect))
            assert f"CREATE TABLE {table.name}" in ddl
    finally:
        engine.sync_engine.dispose()


def test_tenant_owned_tables_have_tenant_ids_and_required_uniqueness() -> None:
    for name in TENANT_OWNED_TABLES:
        assert "tenant_id" in Base.metadata.tables[name].c

    event_constraints = {
        tuple(column.name for column in constraint.columns)
        for constraint in Base.metadata.tables["agent_events"].constraints
        if hasattr(constraint, "columns")
    }
    assert ("run_id", "sequence") in event_constraints

    run_constraints = {
        tuple(column.name for column in constraint.columns)
        for constraint in Base.metadata.tables["runs"].constraints
        if hasattr(constraint, "columns")
    }
    assert ("tenant_id", "session_id", "idempotency_key") in run_constraints

    circuit = Base.metadata.tables["gateway_circuits"]
    assert "tenant_id" not in circuit.c
    assert "probe_started_at" in circuit.c


def test_nullable_json_objects_bind_python_none_as_sql_null() -> None:
    assert ToolCallRecord.result.type.none_as_null is True
    assert GatewayRequestRecord.error.type.none_as_null is True


def test_initial_migration_check_constraints_match_declarative_metadata() -> None:
    migration = ast.parse(
        (
            ROOT
            / "packages"
            / "persistence"
            / "migrations"
            / "versions"
            / "0001_durable_control_plane.py"
        ).read_text(encoding="utf-8")
    )
    migrated: dict[str, dict[str, str]] = {}
    for node in ast.walk(migration):
        if not isinstance(node, ast.Call) or not _is_call(node, "op", "create_table"):
            continue
        table_name = ast.literal_eval(node.args[0])
        checks: dict[str, str] = {}
        for argument in node.args[1:]:
            if not isinstance(argument, ast.Call) or not _is_call(
                argument,
                "sa",
                "CheckConstraint",
            ):
                continue
            name_keyword = next(keyword for keyword in argument.keywords if keyword.arg == "name")
            checks[ast.literal_eval(name_keyword.value)] = _normalized_sql(
                ast.literal_eval(argument.args[0])
            )
        migrated[table_name] = checks

    declared = {
        table.name: {
            str(constraint.name): _normalized_sql(str(constraint.sqltext))
            for constraint in table.constraints
            if isinstance(constraint, CheckConstraint)
        }
        for table in Base.metadata.sorted_tables
    }
    assert migrated == declared


def test_database_settings_are_closed_bounded_and_secret_safe() -> None:
    settings = DatabaseSettings(
        database_url="postgresql+asyncpg://agent:known-secret@localhost/agent"
    )
    assert "known-secret" not in repr(settings)

    with pytest.raises(ValidationError):
        DatabaseSettings(database_url="sqlite+aiosqlite:///local.db")
    with pytest.raises(ValidationError):
        DatabaseSettings(database_pool_size=0)
    with pytest.raises(ValidationError):
        DatabaseSettings.model_validate({"unexpected": True})


class _BlockingEngine:
    def __init__(self) -> None:
        self.dispose_started = asyncio.Event()
        self.allow_dispose = asyncio.Event()
        self.dispose_calls = 0

    async def dispose(self) -> None:
        self.dispose_calls += 1
        self.dispose_started.set()
        await self.allow_dispose.wait()


@pytest.mark.asyncio
async def test_database_cleanup_is_serialized_idempotent_and_cancellation_safe() -> None:
    engine = _BlockingEngine()
    database = Database(
        DatabaseSettings(),
        engine=cast("AsyncEngine", engine),
    )
    cancelled = asyncio.create_task(database.aclose())
    await engine.dispose_started.wait()
    concurrent = asyncio.create_task(database.aclose())

    cancelled.cancel()
    await asyncio.sleep(0)
    assert not cancelled.done()
    assert not concurrent.done()
    engine.allow_dispose.set()

    with pytest.raises(asyncio.CancelledError):
        await cancelled
    await concurrent
    await database.aclose()
    assert engine.dispose_calls == 1
    assert await database.ready() is False


class _FakeSession:
    def __init__(self, *, fail_execute: bool = False) -> None:
        self.fail_execute = fail_execute
        self.execute_calls = 0
        self.begin_calls = 0

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def begin(self) -> Self:
        self.begin_calls += 1
        return self

    async def execute(self, _statement: object) -> None:
        self.execute_calls += 1
        if self.fail_execute:
            raise RuntimeError("database detail must remain opaque")


class _RetryableEngine:
    def __init__(self) -> None:
        self.fail = True
        self.dispose_calls = 0

    async def dispose(self) -> None:
        self.dispose_calls += 1
        if self.fail:
            raise RuntimeError("engine detail must remain opaque")


@pytest.mark.asyncio
async def test_database_readiness_transactions_and_retryable_cleanup() -> None:
    engine = _RetryableEngine()
    database = Database(
        DatabaseSettings(),
        engine=cast("AsyncEngine", engine),
    )
    healthy = _FakeSession()
    cast("Any", database).sessions = lambda: healthy

    assert await database.ready() is True
    async with database.transaction() as transaction:
        assert cast("Any", transaction) is healthy
    assert healthy.execute_calls == 1
    assert healthy.begin_calls == 1

    unavailable = _FakeSession(fail_execute=True)
    cast("Any", database).sessions = lambda: unavailable
    assert await database.ready() is False

    with pytest.raises(DomainOperationError) as cleanup:
        await database.aclose()
    assert cleanup.value.code == "database_cleanup_failed"
    assert await database.__aenter__() is database

    engine.fail = False
    await database.__aexit__(None, None, None)
    assert engine.dispose_calls == 2
    assert await database.ready() is False
    with pytest.raises(RuntimeError, match="closed"):
        await database.__aenter__()
    with pytest.raises(RuntimeError, match="closed"):
        async with database.transaction():
            pass


def test_event_drafts_validate_discriminated_payloads_and_payload_ceiling() -> None:
    draft = EventDraft(
        event_type="context.build_started",
        payload={
            "message_count": 3,
            "checkpoint_id": None,
        },
    )
    assert draft.event_type == "context.build_started"

    with pytest.raises(ValidationError):
        EventDraft(
            event_type="context.build_started",
            payload={"message_count": -1, "checkpoint_id": None},
        )
    with pytest.raises(ValidationError):
        EventDraft(
            event_type="model.text_delta",
            payload={"model_call_id": "call", "delta": "x" * (1024 * 1024 + 1)},
        )


def test_event_page_requires_strict_order_and_cursor_alignment() -> None:
    run_id = uuid.uuid4()
    now = datetime.now(UTC)
    first = _event(run_id, 1, now)
    second = _event(run_id, 2, now)
    page = EventPage(events=(first, second), next_after=2, has_more=False)
    assert page.next_after == 2

    with pytest.raises(ValidationError):
        EventPage(events=(second, first), next_after=1, has_more=False)
    with pytest.raises(ValidationError):
        EventPage(events=(first,), next_after=0, has_more=False)
    with pytest.raises(ValidationError):
        EventPage(
            events=(first, _event(uuid.uuid4(), 2, now)),
            next_after=2,
            has_more=False,
        )
    with pytest.raises(ValidationError):
        EventPage(events=(), next_after=0, has_more=True)


def _event(run_id: uuid.UUID, sequence: int, now: datetime) -> StoredEvent:
    return StoredEvent(
        run_id=run_id,
        sequence=sequence,
        event_type="context.build_started",
        payload={"message_count": sequence, "checkpoint_id": None},
        created_at=now,
    )


def _is_call(node: ast.Call, namespace: str, name: str) -> bool:
    return (
        isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == namespace
        and node.func.attr == name
    )


def _normalized_sql(value: str) -> str:
    return " ".join(value.split())
