from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

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
    _sample_from_run_events,
    load_profiles,
    write_report,
)

from agent_core.event_store import StoredEvent
from agent_core.events import EventType

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
    assert result.resources.process_cpu_seconds >= 0
    assert result.source.revision == "a" * 40
    assert result.source.dirty is True
    assert any("Synthetic" in line for line in result.methodology)


class ConcurrencyDriver:
    mode: Literal["simulation"] = "simulation"

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
    settings = LiveLoadSettings(
        api_base_url="ftp://invalid.example",
        api_token="secret",  # noqa: S106 - invalid URL test fixture
        session_id="00000000-0000-0000-0000-000000000001",
    )
    with pytest.raises(ValueError, match="http or https"):
        LivePlatformDriver(settings)
    with pytest.raises(ValidationError):
        LoadSample(
            operation=1,
            success=True,
            total_latency_seconds=float("nan"),
        )


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
