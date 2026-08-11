from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import httpx
import pytest
from pydantic import ValidationError
from scripts.load_test import (
    DeterministicLoadDriver,
    LiveLoadSettings,
    LivePlatformDriver,
    LoadProfile,
    LoadRunner,
    LoadSample,
    LoadScenario,
    _bounded_response_body,
    _bounded_stream_total,
    _sample_from_run_events,
    _validate_event_sequence,
    load_profiles,
    write_report,
)

from agent_core.event_store import StoredEvent
from agent_core.events import EventType

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

ROOT = Path(__file__).parents[2]


def profile(**updates: object) -> LoadProfile:
    values: dict[str, object] = {
        "name": "unit-load",
        "scenario": LoadScenario.API_SUBMISSION,
        "concurrency": 4,
        "operations": 20,
        "operation_timeout_seconds": 1,
    }
    values.update(updates)
    return LoadProfile.model_validate(values)


def test_versioned_profiles_cover_every_required_load_surface() -> None:
    profiles = load_profiles(ROOT / "benchmarks" / "gateway_load" / "profiles.yaml")

    assert {item.scenario for item in profiles} == set(LoadScenario)
    assert {10, 50, 100} <= {item.concurrency for item in profiles}
    assert all(item.preconditions for item in profiles)
    assert max(item.operations for item in profiles) <= 2_000


def test_profiles_are_closed_bounded_and_unique(tmp_path: Path) -> None:
    with pytest.raises(ValidationError):
        profile(concurrency=0)
    with pytest.raises(ValidationError):
        profile(operations=100_001)
    with pytest.raises(ValidationError):
        profile(unknown=True)

    duplicate = tmp_path / "duplicate.yaml"
    duplicate.write_text(
        "- name: same\n  scenario: api_submission\n  concurrency: 1\n"
        "  operations: 1\n  operation_timeout_seconds: 1\n"
        "- name: same\n  scenario: api_submission\n  concurrency: 1\n"
        "  operations: 1\n  operation_timeout_seconds: 1\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="uniquely named"):
        load_profiles(duplicate)

    oversized = tmp_path / "oversized.yaml"
    oversized.write_bytes(b"x" * (1024 * 1024 + 1))
    with pytest.raises(ValueError, match="1 MiB"):
        load_profiles(oversized)


@pytest.mark.asyncio
async def test_deterministic_report_is_explicitly_synthetic_and_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AGENT_PLATFORM_BENCHMARK_GIT_REVISION", "a" * 40)
    monkeypatch.setenv("AGENT_PLATFORM_BENCHMARK_GIT_DIRTY", "dirty")
    result = await LoadRunner().run(
        profile(
            scenario=LoadScenario.GATEWAY_RATE_LIMIT,
            concurrency=10,
            operations=100,
        ),
        DeterministicLoadDriver(),
    )

    assert result.synthetic is True
    assert result.result_claim == "simulation_only"
    assert result.aggregate.attempted == 100
    assert result.aggregate.succeeded == 90
    assert result.aggregate.errors == {"rate_limit": 10}
    assert result.aggregate.error_rate == 0.1
    assert result.aggregate.throughput_per_second > 0
    assert result.aggregate.latency_seconds.count == 100
    assert result.aggregate.latency_seconds.minimum <= result.aggregate.latency_seconds.p50
    assert result.aggregate.latency_seconds.p50 <= result.aggregate.latency_seconds.p95
    assert result.aggregate.latency_seconds.p95 <= result.aggregate.latency_seconds.p99
    assert result.aggregate.fallbacks.maximum == 0
    assert result.environment.system
    assert result.environment.machine
    assert result.resources.wall_seconds > 0
    assert result.resources.scope == "load_generator_process"
    assert result.resources.process_cpu_seconds >= 0
    assert result.source.revision == "a" * 40
    assert result.source.dirty is True
    assert any("Synthetic" in line for line in result.methodology)


class ConcurrencyDriver:
    mode: Literal["simulation"] = "simulation"
    campaign_id = "concurrency-test"

    def __init__(self) -> None:
        self.active = 0
        self.maximum = 0

    async def execute(self, profile: LoadProfile, operation: int) -> LoadSample:
        del profile
        self.active += 1
        self.maximum = max(self.maximum, self.active)
        await asyncio.sleep(0.01)
        self.active -= 1
        return LoadSample(
            operation=operation,
            success=True,
            total_latency_seconds=0.01,
        )


@pytest.mark.asyncio
async def test_runner_uses_fixed_worker_ceiling_instead_of_task_per_operation() -> None:
    driver = ConcurrencyDriver()

    report = await LoadRunner().run(
        profile(concurrency=3, operations=12),
        driver,
    )

    assert driver.maximum == 3
    assert report.aggregate.attempted == 12
    assert report.aggregate.succeeded == 12


class FailureDriver:
    mode: Literal["live"] = "live"
    campaign_id = "failure-test"

    async def execute(self, profile: LoadProfile, operation: int) -> LoadSample:
        del profile
        if operation == 0:
            await asyncio.Event().wait()
        raise RuntimeError("driver-secret-must-not-escape")


@pytest.mark.asyncio
async def test_timeout_and_driver_errors_are_bounded_and_opaque() -> None:
    report = await LoadRunner().run(
        profile(concurrency=2, operations=2, operation_timeout_seconds=0.01),
        FailureDriver(),
    )

    assert report.synthetic is False
    assert report.aggregate.succeeded == 0
    assert report.aggregate.errors == {"driver_error": 1, "timeout": 1}
    assert "driver-secret-must-not-escape" not in report.model_dump_json()


@pytest.mark.asyncio
async def test_mismatched_driver_operation_fails_the_harness() -> None:
    class MismatchedDriver:
        mode: Literal["simulation"] = "simulation"
        campaign_id = "mismatch-test"

        async def execute(self, profile: LoadProfile, operation: int) -> LoadSample:
            del profile
            return LoadSample(
                operation=operation + 1,
                success=True,
                total_latency_seconds=0,
            )

    with pytest.raises(ValueError, match="mismatched"):
        await LoadRunner().run(profile(operations=1), MismatchedDriver())


@pytest.mark.asyncio
async def test_report_write_is_atomic_json_and_live_secrets_are_repr_safe(
    tmp_path: Path,
) -> None:
    report = await LoadRunner().run(
        profile(operations=2),
        DeterministicLoadDriver(),
    )
    output = tmp_path / "report.json"

    write_report(report, output)

    parsed = json.loads(output.read_text(encoding="utf-8"))
    assert parsed["report_version"] == "agent-load-v1"
    assert parsed["campaign_id"] == "simulation"
    assert parsed["profile"]["operations"] == 2
    assert await asyncio.to_thread(lambda: list(tmp_path.glob(".report.json.*"))) == []
    with pytest.raises(ValueError, match="JSON file"):
        write_report(report, tmp_path / "report.txt")

    settings = LiveLoadSettings(
        api_base_url="http://127.0.0.1:8000",
        api_token="load-secret-value",  # noqa: S106 - verifies secret-safe repr
        session_id="00000000-0000-0000-0000-000000000001",
    )
    assert "load-secret-value" not in repr(settings)
    driver = LivePlatformDriver(settings)
    assert "load-secret-value" not in repr(driver)
    await driver.aclose()


def test_live_settings_and_samples_reject_unsafe_values() -> None:
    for invalid_url in (
        "ftp://invalid.example",
        "https://user:password@example.test",
        "https://example.test/base",
        "https://example.test?token=secret",
        "https://example.test#fragment",
        "https://example.test:99999",
    ):
        with pytest.raises(ValidationError):
            LiveLoadSettings(
                api_base_url=invalid_url,
                api_token="secret",  # noqa: S106 - invalid URL test fixture
                session_id="00000000-0000-0000-0000-000000000001",
            )
    with pytest.raises(ValidationError):
        LoadSample(
            operation=1,
            success=True,
            total_latency_seconds=float("nan"),
        )


@pytest.mark.asyncio
async def test_campaign_keys_are_unique_and_injected_clients_are_not_owned() -> None:
    settings = LiveLoadSettings(
        api_base_url="https://example.test/",
        api_token="secret",  # noqa: S106 - test fixture
        session_id="00000000-0000-0000-0000-000000000001",
    )
    client = httpx.AsyncClient(base_url="https://example.test")
    first = LivePlatformDriver(settings, http_client=client)
    second = LivePlatformDriver(settings, http_client=client)

    assert first.campaign_id != second.campaign_id
    assert first._idempotency_key(profile(operations=1), 0) != second._idempotency_key(
        profile(operations=1), 0
    )
    await first.aclose()
    assert not client.is_closed
    await client.aclose()

    class InvalidCampaignDriver(ConcurrencyDriver):
        campaign_id = "INVALID CAMPAIGN"

    with pytest.raises(ValidationError):
        await LoadRunner().run(profile(operations=1), InvalidCampaignDriver())


@pytest.mark.asyncio
async def test_response_and_event_stream_byte_budgets_are_cumulative() -> None:
    class ChunkStream(httpx.AsyncByteStream):
        def __init__(self) -> None:
            self.yielded = 0

        async def __aiter__(self) -> AsyncIterator[bytes]:
            for chunk in (b"a" * 600_000, b"b" * 600_000, b"unreachable"):
                self.yielded += 1
                yield chunk

    stream = ChunkStream()
    response = httpx.Response(200, stream=stream)
    with pytest.raises(ValueError, match="run-creation response exceeded"):
        await _bounded_response_body(response)
    assert stream.yielded == 2
    await response.aclose()

    assert _bounded_stream_total(0, "🙂") == 4
    with pytest.raises(ValueError, match="event stream exceeded"):
        _bounded_stream_total(16 * 1024 * 1024, "🙂")


def test_standalone_stream_sequence_must_start_at_one_and_remain_contiguous() -> None:
    run_id = uuid.uuid4()
    event = StoredEvent(
        run_id=run_id,
        sequence=2,
        event_type=EventType.RUN_STARTED,
        payload={"attempt": 1, "worker_id": "worker-1"},
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="sequence gap"):
        _validate_event_sequence(0, event)
    _validate_event_sequence(1, event)


@pytest.mark.asyncio
async def test_standalone_terminal_failure_is_not_reported_as_stream_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = uuid.uuid4()
    failed = StoredEvent(
        run_id=run_id,
        sequence=1,
        event_type=EventType.RUN_FAILED,
        payload={
            "error": {
                "code": "sandbox_command_failed",
                "message": "the command failed",
                "retryable": False,
                "details": {},
            }
        },
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    class WebSocket:
        async def recv(self) -> str:
            return failed.model_dump_json()

    class Connection:
        async def __aenter__(self) -> WebSocket:
            return WebSocket()

        async def __aexit__(self, *errors: object) -> None:
            del errors

    monkeypatch.setattr("scripts.load_test.connect", lambda *args, **kwargs: Connection())
    driver = LivePlatformDriver(
        LiveLoadSettings(
            api_base_url="https://example.test",
            api_token="secret",  # noqa: S106 - test fixture
            session_id="00000000-0000-0000-0000-000000000001",
            stream_run_id=str(run_id),
        )
    )
    try:
        sample = await driver.execute(
            profile(
                scenario=LoadScenario.EVENT_THROUGHPUT,
                operations=1,
                max_events_per_connection=1,
            ),
            0,
        )
    finally:
        await driver.aclose()

    assert sample.success is False
    assert sample.task_success is False
    assert sample.error_category == "sandbox_failure"


def test_durable_run_events_produce_task_latency_and_no_fabricated_usage() -> None:
    run_id = uuid.uuid4()
    created_at = datetime(2026, 1, 1, tzinfo=UTC)

    def event(
        sequence: int,
        event_type: EventType,
        payload: dict[str, object],
        seconds: int,
    ) -> StoredEvent:
        return StoredEvent(
            run_id=run_id,
            sequence=sequence,
            event_type=event_type,
            payload=payload,
            created_at=created_at + timedelta(seconds=seconds),
        )

    events = (
        event(1, EventType.RUN_STARTED, {"attempt": 1, "worker_id": "worker-1"}, 2),
        event(
            2,
            EventType.MODEL_REQUEST_STARTED,
            {
                "model_call_id": "model-1",
                "request_id": "request-1",
                "route_name": "coding-default",
            },
            3,
        ),
        event(
            3,
            EventType.MODEL_TEXT_DELTA,
            {"model_call_id": "model-1", "delta": "bounded"},
            4,
        ),
        event(
            4,
            EventType.TOOL_STARTED,
            {"tool_call_id": "tool-1", "tool_name": "read_file"},
            5,
        ),
        event(5, EventType.RUN_COMPLETED, {"final_text": "done"}, 6),
    )

    sample = _sample_from_run_events(
        operation=7,
        status_code=202,
        run_created_at=created_at,
        elapsed_seconds=99,
        events=events,
    )

    assert sample.success is True
    assert sample.task_success is True
    assert sample.queue_wait_seconds == 2
    assert sample.first_token_seconds == 1
    assert sample.total_latency_seconds == 6
    assert sample.iterations == 1
    assert sample.tool_calls == 1
    assert sample.input_tokens is None
    assert sample.output_tokens is None
    assert sample.cost_usd is None
    assert sample.fallbacks is None


def test_failed_or_incomplete_run_events_are_counted_as_load_errors() -> None:
    run_id = uuid.uuid4()
    created_at = datetime(2026, 1, 1, tzinfo=UTC)
    failed = StoredEvent(
        run_id=run_id,
        sequence=1,
        event_type=EventType.RUN_FAILED,
        payload={
            "error": {
                "code": "sandbox_command_failed",
                "message": "the command failed",
                "retryable": False,
                "details": {},
            }
        },
        created_at=created_at + timedelta(seconds=2),
    )

    failed_sample = _sample_from_run_events(
        operation=0,
        status_code=202,
        run_created_at=created_at,
        elapsed_seconds=2,
        events=(failed,),
    )
    incomplete_sample = _sample_from_run_events(
        operation=1,
        status_code=202,
        run_created_at=created_at,
        elapsed_seconds=3,
        events=(),
    )

    assert failed_sample.success is False
    assert failed_sample.error_category == "sandbox_failure"
    assert incomplete_sample.success is False
    assert incomplete_sample.error_category == "event_limit"
    with pytest.raises(ValidationError):
        LoadSample(
            operation=1,
            success=True,
            total_latency_seconds=0,
            error_category="unbounded category value",
        )


@pytest.mark.asyncio
async def test_source_state_rejects_ambiguous_dirty_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AGENT_PLATFORM_BENCHMARK_GIT_DIRTY", "perhaps")
    with pytest.raises(ValueError, match="clean, dirty, or unknown"):
        await LoadRunner().run(profile(operations=1), DeterministicLoadDriver())
