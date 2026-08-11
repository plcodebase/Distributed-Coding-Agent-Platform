"""Bounded deterministic and opt-in live load harness for the agent platform."""

from __future__ import annotations

import argparse
import asyncio
import math
import os
import platform
import resource
import sys
import tempfile
import time
import uuid
from collections import Counter
from collections.abc import Mapping
from contextlib import suppress
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, Protocol, Self
from urllib.parse import urlsplit

import httpx
import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    StringConstraints,
    TypeAdapter,
    field_validator,
    model_validator,
)
from websockets.asyncio.client import connect

from agent_api.schemas import RunCreationResponse
from agent_core.event_store import StoredEvent
from agent_core.events import EventType

if TYPE_CHECKING:
    from collections.abc import Sequence

MAX_PROFILE_BYTES = 1024 * 1024
MAX_REPORT_BYTES = 16 * 1024 * 1024
MAX_OPERATIONS = 100_000
MAX_CONCURRENCY = 1_000
MAX_API_RESPONSE_BYTES = 1024 * 1024
MAX_EVENT_STREAM_BYTES = 16 * 1024 * 1024
HTTP_CLIENT_ERROR = 400
HTTP_RATE_LIMITED = 429
HTTP_SERVER_ERROR = 500
type BoundedName = Annotated[
    str,
    StringConstraints(min_length=1, max_length=100, pattern=r"^[a-z][a-z0-9_-]*$"),
]
type PositiveCount = Annotated[int, Field(ge=1, le=MAX_OPERATIONS)]
type CampaignId = Annotated[
    str,
    StringConstraints(min_length=1, max_length=64, pattern=r"^[a-z0-9][a-z0-9-]*$"),
]
_CAMPAIGN_ADAPTER: TypeAdapter[str] = TypeAdapter(CampaignId)


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class LoadScenario(StrEnum):
    API_SUBMISSION = "api_submission"
    WEBSOCKET_CONNECTIONS = "websocket_connections"
    EVENT_THROUGHPUT = "event_throughput"
    WORKER_SATURATION = "worker_saturation"
    GATEWAY_RATE_LIMIT = "gateway_rate_limit"
    PROVIDER_FALLBACK = "provider_fallback"
    POSTGRES_CONTENTION = "postgres_contention"
    REDIS_CONTENTION = "redis_contention"
    SANDBOX_SATURATION = "sandbox_saturation"


class LoadProfile(_Model):
    name: BoundedName
    scenario: LoadScenario
    concurrency: int = Field(ge=1, le=MAX_CONCURRENCY)
    operations: int = Field(ge=1, le=MAX_OPERATIONS)
    operation_timeout_seconds: float = Field(gt=0, le=600)
    max_events_per_connection: int = Field(default=100, ge=1, le=100_000)
    preconditions: tuple[Annotated[str, StringConstraints(min_length=1, max_length=500)], ...] = ()


_PROFILE_ADAPTER = TypeAdapter(list[LoadProfile])


class LoadSample(_Model):
    operation: int = Field(ge=0, le=MAX_OPERATIONS)
    success: bool
    task_success: bool = False
    tests_passed: bool | None = None
    status_code: int | None = Field(default=None, ge=100, le=599)
    total_latency_seconds: float = Field(ge=0, le=86_400)
    queue_wait_seconds: float | None = Field(default=None, ge=0, le=86_400)
    first_token_seconds: float | None = Field(default=None, ge=0, le=86_400)
    iterations: int = Field(default=0, ge=0, le=1_000_000)
    tool_calls: int = Field(default=0, ge=0, le=1_000_000)
    input_tokens: int | None = Field(default=None, ge=0, le=1_000_000_000)
    output_tokens: int | None = Field(default=None, ge=0, le=1_000_000_000)
    cost_usd: float | None = Field(default=None, ge=0, le=1_000_000)
    retries: int = Field(default=0, ge=0, le=1_000_000)
    fallbacks: int | None = Field(default=None, ge=0, le=1_000_000)
    permission_denials: int = Field(default=0, ge=0, le=1_000_000)
    events_received: int = Field(default=0, ge=0, le=1_000_000)
    error_category: BoundedName | None = None

    @model_validator(mode="after")
    def validate_outcome(self) -> Self:
        if self.success == (self.error_category is not None):
            raise ValueError("successful samples have no error and failed samples require one")
        if self.task_success and not self.success:
            raise ValueError("task success requires operation success")
        if self.tests_passed is True and not self.task_success:
            raise ValueError("passing tests require task success")
        return self


class MetricSummary(_Model):
    count: int = Field(ge=0)
    minimum: float = Field(ge=0)
    p50: float = Field(ge=0)
    p95: float = Field(ge=0)
    p99: float = Field(ge=0)
    maximum: float = Field(ge=0)
    mean: float = Field(ge=0)

    @model_validator(mode="after")
    def validate_distribution(self) -> Self:
        values = (self.minimum, self.p50, self.p95, self.p99, self.maximum)
        if self.count == 0 and any(value != 0 for value in (*values, self.mean)):
            raise ValueError("an unobserved metric summary must contain only zero placeholders")
        if self.count > 0 and (
            tuple(sorted(values)) != values or not self.minimum <= self.mean <= self.maximum
        ):
            raise ValueError("metric summary values must be ordered and contain the mean")
        return self


class LoadAggregate(_Model):
    attempted: int = Field(ge=1)
    succeeded: int = Field(ge=0)
    task_succeeded: int = Field(ge=0)
    tests_passed: int = Field(ge=0)
    tests_observed: int = Field(ge=0)
    errors: dict[BoundedName, PositiveCount]
    error_rate: float = Field(ge=0, le=1)
    throughput_per_second: float = Field(ge=0)
    latency_seconds: MetricSummary
    queue_wait_seconds: MetricSummary
    first_token_seconds: MetricSummary
    iterations: MetricSummary
    tool_calls: MetricSummary
    input_tokens: MetricSummary
    output_tokens: MetricSummary
    cost_usd: MetricSummary
    retries: MetricSummary
    fallbacks: MetricSummary
    permission_denials: MetricSummary
    events_received: MetricSummary

    @model_validator(mode="after")
    def validate_counts(self) -> Self:
        if not 0 <= self.task_succeeded <= self.succeeded <= self.attempted:
            raise ValueError("load success counts exceed attempted operations")
        if not 0 <= self.tests_passed <= self.tests_observed <= self.attempted:
            raise ValueError("load test counts exceed attempted operations")
        if sum(self.errors.values()) != self.attempted - self.succeeded:
            raise ValueError("error counts must equal failed operations")
        if self.latency_seconds.count != self.attempted:
            raise ValueError("total latency requires one observation per operation")
        return self


class BenchmarkEnvironment(_Model):
    system: str
    release: str
    machine: str
    python: str
    logical_cpus: int | None = Field(default=None, ge=1)
    physical_memory_bytes: int | None = Field(default=None, ge=1)


class ResourceUtilization(_Model):
    scope: Literal["load_generator_process"] = "load_generator_process"
    wall_seconds: float = Field(gt=0)
    process_cpu_seconds: float = Field(ge=0)
    process_cpu_percent: float = Field(ge=0)
    process_max_rss_bytes: int | None = Field(default=None, ge=1)


class SourceState(_Model):
    revision: Annotated[
        str,
        StringConstraints(pattern=r"^(?:[0-9a-f]{7,64}|unavailable)$"),
    ]
    dirty: bool | None


class LoadReport(_Model):
    report_version: Literal["agent-load-v1"] = "agent-load-v1"
    synthetic: bool
    result_claim: Literal["measurement", "simulation_only"]
    campaign_id: CampaignId
    profile: LoadProfile
    started_at: datetime
    completed_at: datetime
    methodology: tuple[str, ...]
    environment: BenchmarkEnvironment
    source: SourceState
    resources: ResourceUtilization
    aggregate: LoadAggregate

    @model_validator(mode="after")
    def validate_report(self) -> Self:
        if self.completed_at < self.started_at:
            raise ValueError("load report completion may not precede its start")
        expected_claim = "simulation_only" if self.synthetic else "measurement"
        if self.result_claim != expected_claim:
            raise ValueError("load report claim must match its synthetic state")
        return self


class LoadDriver(Protocol):
    @property
    def mode(self) -> Literal["live", "simulation"]: ...

    @property
    def campaign_id(self) -> str: ...

    async def execute(self, profile: LoadProfile, operation: int) -> LoadSample: ...


class DeterministicLoadDriver:
    """Synthetic driver for repeatable CI validation; never a performance claim."""

    mode: Literal["simulation"] = "simulation"
    campaign_id = "simulation"

    async def execute(self, profile: LoadProfile, operation: int) -> LoadSample:
        await asyncio.sleep(0)
        latency = 0.005 + (operation % 17) * 0.001
        rate_limited = profile.scenario is LoadScenario.GATEWAY_RATE_LIMIT and operation % 10 == 0
        fallback = profile.scenario is LoadScenario.PROVIDER_FALLBACK and operation % 4 == 0
        return LoadSample(
            operation=operation,
            success=not rate_limited,
            task_success=not rate_limited,
            tests_passed=True if not rate_limited else None,
            status_code=429 if rate_limited else 202,
            total_latency_seconds=latency,
            queue_wait_seconds=(operation % 11) * 0.002,
            first_token_seconds=latency / 2,
            iterations=1 + operation % 3,
            tool_calls=operation % 5,
            input_tokens=100 + operation % 23,
            output_tokens=20 + operation % 7,
            cost_usd=(120 + operation % 30) / 1_000_000,
            retries=1 if rate_limited else 0,
            fallbacks=1 if fallback else 0,
            permission_denials=1 if operation % 97 == 0 else 0,
            events_received=1 + operation % profile.max_events_per_connection,
            error_category="rate_limit" if rate_limited else None,
        )


class LiveLoadSettings(_Model):
    api_base_url: Annotated[str, StringConstraints(min_length=8, max_length=2_048)]
    api_token: SecretStr
    session_id: Annotated[
        str,
        StringConstraints(
            pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        ),
    ]
    stream_run_id: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    )
    verify_tls: bool = True

    @field_validator("api_base_url")
    @classmethod
    def validate_api_base_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("api_base_url must be an absolute HTTP(S) origin")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("api_base_url must not contain credentials")
        if parsed.query or parsed.fragment or parsed.path not in {"", "/"}:
            raise ValueError("api_base_url must not contain a path, query, or fragment")
        try:
            _ = parsed.port
        except ValueError as error:
            raise ValueError("api_base_url contains an invalid port") from error
        return value.rstrip("/")


class LivePlatformDriver:
    """Opt-in client for a user-provisioned test deployment."""

    mode: Literal["live"] = "live"

    def __init__(
        self,
        settings: LiveLoadSettings,
        *,
        http_client: httpx.AsyncClient | None = None,
        campaign_id: str | None = None,
    ) -> None:
        self._settings = settings
        self.campaign_id: str = _CAMPAIGN_ADAPTER.validate_python(
            campaign_id if campaign_id is not None else uuid.uuid4().hex
        )
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(
            base_url=settings.api_base_url,
            verify=settings.verify_tls,
            follow_redirects=False,
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc_type, exc, traceback
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def execute(self, profile: LoadProfile, operation: int) -> LoadSample:
        if profile.scenario in {
            LoadScenario.WEBSOCKET_CONNECTIONS,
            LoadScenario.EVENT_THROUGHPUT,
        }:
            return await self._stream(profile, operation)
        return await self._submit(profile, operation)

    async def _submit(self, profile: LoadProfile, operation: int) -> LoadSample:
        started = time.monotonic()
        async with self._client.stream(
            "POST",
            f"/v1/sessions/{self._settings.session_id}/runs",
            headers={
                "Authorization": f"Bearer {self._settings.api_token.get_secret_value()}",
                "Idempotency-Key": self._idempotency_key(profile, operation),
            },
            json={
                "priority": -10 if profile.scenario is LoadScenario.WORKER_SATURATION else 0,
                "priority_class": (
                    "background"
                    if profile.scenario is LoadScenario.WORKER_SATURATION
                    else "interactive"
                ),
            },
        ) as response:
            status_code = response.status_code
            if status_code >= HTTP_CLIENT_ERROR:
                return LoadSample(
                    operation=operation,
                    success=False,
                    status_code=status_code,
                    total_latency_seconds=time.monotonic() - started,
                    retries=1 if status_code == HTTP_RATE_LIMITED else 0,
                    error_category=_http_error_category(status_code),
                )
            payload = await _bounded_response_body(response)
        created = RunCreationResponse.model_validate_json(payload)
        return await self._observe_submitted_run(
            profile,
            operation=operation,
            run_id=str(created.run.id),
            run_created_at=created.run.created_at,
            status_code=status_code,
            started=started,
        )

    def _idempotency_key(self, profile: LoadProfile, operation: int) -> str:
        return f"load-{self.campaign_id}-{profile.name}-{operation}"

    async def _observe_submitted_run(
        self,
        profile: LoadProfile,
        *,
        operation: int,
        run_id: str,
        run_created_at: datetime,
        status_code: int,
        started: float,
    ) -> LoadSample:
        events: list[StoredEvent] = []
        received_bytes = 0
        url = f"{_websocket_url(self._settings.api_base_url, run_id)}?after=0"
        async with connect(
            url,
            additional_headers={
                "Authorization": f"Bearer {self._settings.api_token.get_secret_value()}"
            },
            open_timeout=profile.operation_timeout_seconds,
            close_timeout=2,
            max_size=MAX_API_RESPONSE_BYTES,
            max_queue=16,
        ) as websocket:
            while len(events) < profile.max_events_per_connection:
                message = await websocket.recv()
                received_bytes = _bounded_stream_total(received_bytes, message)
                event = _stored_event(message, expected_run_id=run_id)
                _validate_event_sequence(events[-1].sequence if events else 0, event)
                events.append(event)
                if event.event_type in {EventType.RUN_COMPLETED, EventType.RUN_FAILED}:
                    break
        return _sample_from_run_events(
            operation=operation,
            status_code=status_code,
            run_created_at=run_created_at,
            elapsed_seconds=time.monotonic() - started,
            events=tuple(events),
        )

    async def _stream(self, profile: LoadProfile, operation: int) -> LoadSample:
        run_id = self._settings.stream_run_id
        if run_id is None:
            raise ValueError("stream_run_id is required for WebSocket load profiles")
        started = time.monotonic()
        events = 0
        received_bytes = 0
        previous_sequence = 0
        task_success = False
        terminal_failure: StoredEvent | None = None
        url = _websocket_url(self._settings.api_base_url, run_id)
        async with connect(
            url,
            additional_headers={
                "Authorization": f"Bearer {self._settings.api_token.get_secret_value()}"
            },
            open_timeout=profile.operation_timeout_seconds,
            close_timeout=2,
            max_size=1024 * 1024,
            max_queue=16,
        ) as websocket:
            while events < profile.max_events_per_connection:
                message = await websocket.recv()
                received_bytes = _bounded_stream_total(received_bytes, message)
                event = _stored_event(message, expected_run_id=run_id)
                _validate_event_sequence(previous_sequence, event)
                previous_sequence = event.sequence
                events += 1
                if event.event_type in {EventType.RUN_COMPLETED, EventType.RUN_FAILED}:
                    task_success = event.event_type is EventType.RUN_COMPLETED
                    if not task_success:
                        terminal_failure = event
                    break
        success = terminal_failure is None
        return LoadSample(
            operation=operation,
            success=success,
            task_success=task_success,
            total_latency_seconds=time.monotonic() - started,
            events_received=events,
            error_category=(
                _terminal_error_category(terminal_failure) if terminal_failure is not None else None
            ),
        )


class LoadRunner:
    """Run a fixed number of operations with a fixed worker-task ceiling."""

    async def run(self, profile: LoadProfile, driver: LoadDriver) -> LoadReport:
        campaign_id = _CAMPAIGN_ADAPTER.validate_python(driver.campaign_id)
        started_at = datetime.now(UTC)
        started_monotonic = time.monotonic()
        started_cpu = time.process_time()
        samples: list[LoadSample | None] = [None] * profile.operations
        next_operation = 0
        lock = asyncio.Lock()

        async def worker() -> None:
            nonlocal next_operation
            while True:
                async with lock:
                    if next_operation >= profile.operations:
                        return
                    operation = next_operation
                    next_operation += 1
                samples[operation] = await self._execute_one(profile, driver, operation)

        workers = [
            asyncio.create_task(worker(), name=f"load-worker-{index}")
            for index in range(min(profile.concurrency, profile.operations))
        ]
        await asyncio.gather(*workers)
        completed = tuple(sample for sample in samples if sample is not None)
        if len(completed) != profile.operations:
            raise RuntimeError("load runner did not produce one sample per operation")
        completed_at = datetime.now(UTC)
        wall_seconds = max(time.monotonic() - started_monotonic, sys.float_info.epsilon)
        cpu_seconds = max(time.process_time() - started_cpu, 0)
        return LoadReport(
            synthetic=driver.mode == "simulation",
            result_claim="simulation_only" if driver.mode == "simulation" else "measurement",
            campaign_id=campaign_id,
            profile=profile,
            started_at=started_at,
            completed_at=completed_at,
            methodology=(
                "Fixed-count workload executed by a bounded set of asyncio worker tasks.",
                "Each operation has an independent timeout and produces one validated sample.",
                "Percentiles use nearest-rank selection over successful and failed samples.",
                (
                    "Synthetic timings validate harness behavior only."
                    if driver.mode == "simulation"
                    else "Live timings were observed from the configured test deployment."
                ),
            ),
            environment=_environment(),
            source=_source_state(),
            resources=ResourceUtilization(
                wall_seconds=wall_seconds,
                process_cpu_seconds=cpu_seconds,
                process_cpu_percent=cpu_seconds / wall_seconds * 100,
                process_max_rss_bytes=_max_rss_bytes(),
            ),
            aggregate=_aggregate(completed, wall_seconds=wall_seconds),
        )

    @staticmethod
    async def _execute_one(
        profile: LoadProfile,
        driver: LoadDriver,
        operation: int,
    ) -> LoadSample:
        started = time.monotonic()
        try:
            async with asyncio.timeout(profile.operation_timeout_seconds):
                sample = await driver.execute(profile, operation)
        except TimeoutError:
            return LoadSample(
                operation=operation,
                success=False,
                total_latency_seconds=time.monotonic() - started,
                error_category="timeout",
            )
        except Exception:
            return LoadSample(
                operation=operation,
                success=False,
                total_latency_seconds=time.monotonic() - started,
                error_category="driver_error",
            )
        if sample.operation != operation:
            raise ValueError("load driver returned a mismatched operation identifier")
        return sample


def load_profiles(path: Path) -> tuple[LoadProfile, ...]:
    payload = path.read_bytes()
    if not payload or len(payload) > MAX_PROFILE_BYTES:
        raise ValueError("load profile file must be between 1 byte and 1 MiB")
    value = yaml.safe_load(payload)
    profiles = tuple(_PROFILE_ADAPTER.validate_python(value))
    if not profiles or len({profile.name for profile in profiles}) != len(profiles):
        raise ValueError("load profiles must be nonempty and uniquely named")
    return profiles


def write_report(report: LoadReport, path: Path) -> None:
    if path.suffix != ".json" or not path.parent.is_dir():
        raise ValueError("report path must be a JSON file in an existing directory")
    payload = (report.model_dump_json(indent=2) + "\n").encode("utf-8")
    if len(payload) > MAX_REPORT_BYTES:
        raise ValueError("load report exceeded 16 MiB")
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            delete=False,
        ) as temporary:
            temporary_name = temporary.name
            temporary.write(payload)
            temporary.flush()
            os.fsync(temporary.fileno())
        Path(temporary_name).replace(path)
    finally:
        if temporary_name is not None:
            with suppress(FileNotFoundError):
                Path(temporary_name).unlink()


def _aggregate(samples: tuple[LoadSample, ...], *, wall_seconds: float) -> LoadAggregate:
    tests = tuple(sample.tests_passed for sample in samples if sample.tests_passed is not None)
    errors = Counter(
        sample.error_category or "uncategorized" for sample in samples if not sample.success
    )
    return LoadAggregate(
        attempted=len(samples),
        succeeded=sum(sample.success for sample in samples),
        task_succeeded=sum(sample.task_success for sample in samples),
        tests_passed=sum(tests),
        tests_observed=len(tests),
        errors=dict(sorted(errors.items())),
        error_rate=sum(not sample.success for sample in samples) / len(samples),
        throughput_per_second=len(samples) / wall_seconds,
        latency_seconds=_summary(tuple(item.total_latency_seconds for item in samples)),
        queue_wait_seconds=_summary(
            tuple(
                item.queue_wait_seconds for item in samples if item.queue_wait_seconds is not None
            )
        ),
        first_token_seconds=_summary(
            tuple(
                item.first_token_seconds for item in samples if item.first_token_seconds is not None
            )
        ),
        iterations=_summary(tuple(float(item.iterations) for item in samples)),
        tool_calls=_summary(tuple(float(item.tool_calls) for item in samples)),
        input_tokens=_summary(
            tuple(float(item.input_tokens) for item in samples if item.input_tokens is not None)
        ),
        output_tokens=_summary(
            tuple(float(item.output_tokens) for item in samples if item.output_tokens is not None)
        ),
        cost_usd=_summary(tuple(item.cost_usd for item in samples if item.cost_usd is not None)),
        retries=_summary(tuple(float(item.retries) for item in samples)),
        fallbacks=_summary(
            tuple(float(item.fallbacks) for item in samples if item.fallbacks is not None)
        ),
        permission_denials=_summary(tuple(float(item.permission_denials) for item in samples)),
        events_received=_summary(tuple(float(item.events_received) for item in samples)),
    )


def _summary(values: tuple[float, ...]) -> MetricSummary:
    if not values:
        return MetricSummary(count=0, minimum=0, p50=0, p95=0, p99=0, maximum=0, mean=0)
    ordered = tuple(sorted(values))
    return MetricSummary(
        count=len(ordered),
        minimum=ordered[0],
        p50=_nearest_rank(ordered, 0.50),
        p95=_nearest_rank(ordered, 0.95),
        p99=_nearest_rank(ordered, 0.99),
        maximum=ordered[-1],
        mean=math.fsum(ordered) / len(ordered),
    )


def _nearest_rank(ordered: tuple[float, ...], percentile: float) -> float:
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _environment() -> BenchmarkEnvironment:
    logical_cpus = os.cpu_count()
    memory: int | None = None
    with suppress(OSError, TypeError, ValueError):
        memory = int(os.sysconf("SC_PAGE_SIZE")) * int(os.sysconf("SC_PHYS_PAGES"))
    return BenchmarkEnvironment(
        system=platform.system() or "unknown",
        release=platform.release() or "unknown",
        machine=platform.machine() or "unknown",
        python=platform.python_version(),
        logical_cpus=logical_cpus,
        physical_memory_bytes=memory if memory is not None and memory > 0 else None,
    )


def _max_rss_bytes() -> int | None:
    with suppress(OSError, ValueError):
        value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        if value > 0:
            return value if sys.platform == "darwin" else value * 1024
    return None


def _source_state() -> SourceState:
    revision = os.getenv("AGENT_PLATFORM_BENCHMARK_GIT_REVISION", "unavailable").lower()
    dirty_value = os.getenv("AGENT_PLATFORM_BENCHMARK_GIT_DIRTY")
    if dirty_value is None or dirty_value == "unknown":
        dirty = None
    elif dirty_value in {"1", "true", "dirty"}:
        dirty = True
    elif dirty_value in {"0", "false", "clean"}:
        dirty = False
    else:
        raise ValueError("AGENT_PLATFORM_BENCHMARK_GIT_DIRTY must be clean, dirty, or unknown")
    return SourceState(revision=revision, dirty=dirty)


def _websocket_url(base_url: str, run_id: str) -> str:
    base = base_url.rstrip("/")
    if base.startswith("https://"):
        base = "wss://" + base.removeprefix("https://")
    elif base.startswith("http://"):
        base = "ws://" + base.removeprefix("http://")
    else:
        raise ValueError("api_base_url must use http or https")
    return f"{base}/v1/runs/{run_id}/stream"


async def _bounded_response_body(response: httpx.Response) -> bytes:
    payload = bytearray()
    async for chunk in response.aiter_bytes():
        if len(payload) + len(chunk) > MAX_API_RESPONSE_BYTES:
            raise ValueError("run-creation response exceeded the benchmark protocol limit")
        payload.extend(chunk)
    return bytes(payload)


def _bounded_stream_total(received_bytes: int, message: str | bytes) -> int:
    message_bytes = len(message) if isinstance(message, bytes) else len(message.encode("utf-8"))
    total = received_bytes + message_bytes
    if total > MAX_EVENT_STREAM_BYTES:
        raise ValueError("durable event stream exceeded the benchmark protocol limit")
    return total


def _validate_event_sequence(previous_sequence: int, event: StoredEvent) -> None:
    if event.sequence != previous_sequence + 1:
        raise ValueError("durable event stream contained a sequence gap")


def _stored_event(message: str | bytes, *, expected_run_id: str) -> StoredEvent:
    event = StoredEvent.model_validate_json(message)
    if str(event.run_id) != expected_run_id:
        raise ValueError("durable event stream returned an unexpected run identifier")
    return event


def _sample_from_run_events(
    *,
    operation: int,
    status_code: int,
    run_created_at: datetime,
    elapsed_seconds: float,
    events: tuple[StoredEvent, ...],
) -> LoadSample:
    terminal = next(
        (
            event
            for event in reversed(events)
            if event.event_type in {EventType.RUN_COMPLETED, EventType.RUN_FAILED}
        ),
        None,
    )
    run_started = next(
        (event for event in events if event.event_type is EventType.RUN_STARTED),
        None,
    )
    model_requested = next(
        (event for event in events if event.event_type is EventType.MODEL_REQUEST_STARTED),
        None,
    )
    model_output = next(
        (
            event
            for event in events
            if event.event_type in {EventType.MODEL_TEXT_DELTA, EventType.MODEL_TOOL_CALL_RECEIVED}
            and (model_requested is None or event.created_at >= model_requested.created_at)
        ),
        None,
    )
    queue_wait = (
        max(0.0, (run_started.created_at - run_created_at).total_seconds())
        if run_started is not None
        else None
    )
    first_token = (
        max(0.0, (model_output.created_at - model_requested.created_at).total_seconds())
        if model_requested is not None and model_output is not None
        else None
    )
    task_success = terminal is not None and terminal.event_type is EventType.RUN_COMPLETED
    error_category = None
    if terminal is None:
        error_category = "event_limit"
    elif terminal.event_type is EventType.RUN_FAILED:
        error_category = _terminal_error_category(terminal)
    return LoadSample(
        operation=operation,
        success=task_success,
        task_success=task_success,
        status_code=status_code,
        total_latency_seconds=(
            max(0.0, (terminal.created_at - run_created_at).total_seconds())
            if terminal is not None
            else elapsed_seconds
        ),
        queue_wait_seconds=queue_wait,
        first_token_seconds=first_token,
        iterations=sum(event.event_type is EventType.MODEL_REQUEST_STARTED for event in events),
        tool_calls=sum(event.event_type is EventType.TOOL_STARTED for event in events),
        retries=sum(event.event_type is EventType.RUN_RETRY_SCHEDULED for event in events),
        permission_denials=sum(_is_permission_denial(event) for event in events),
        events_received=len(events),
        error_category=error_category,
    )


def _terminal_error_category(event: StoredEvent) -> str:
    error = event.payload.get("error")
    code = error.get("code") if isinstance(error, Mapping) else None
    if not isinstance(code, str):
        return "task_failure"
    category = "task_failure"
    if "rate_limit" in code:
        category = "rate_limit"
    elif "timeout" in code:
        category = "timeout"
    elif "sandbox" in code or "command" in code:
        category = "sandbox_failure"
    elif "provider" in code or "gateway" in code:
        category = "provider_failure"
    elif "permission" in code or "authoriz" in code or "denied" in code:
        category = "permission_denial"
    return category


def _is_permission_denial(event: StoredEvent) -> bool:
    if event.event_type is not EventType.TOOL_COMPLETED:
        return False
    error = event.payload.get("error")
    code = error.get("code") if isinstance(error, Mapping) else None
    return isinstance(code, str) and any(
        marker in code for marker in ("permission", "authoriz", "denied", "protected")
    )


def _http_error_category(status: int) -> str:
    if status == HTTP_RATE_LIMITED:
        return "rate_limit"
    if status in {401, 403}:
        return "authorization"
    if status >= HTTP_SERVER_ERROR:
        return "server_error"
    return "request_error"


def _live_settings_from_environment() -> LiveLoadSettings:
    required = {
        "api_base_url": os.getenv("AGENT_PLATFORM_LOAD_API_URL"),
        "api_token": os.getenv("AGENT_PLATFORM_LOAD_API_TOKEN"),
        "session_id": os.getenv("AGENT_PLATFORM_LOAD_SESSION_ID"),
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ValueError(f"live load settings are missing: {', '.join(sorted(missing))}")
    return LiveLoadSettings(
        **required,
        stream_run_id=os.getenv("AGENT_PLATFORM_LOAD_RUN_ID"),
    )


async def _run_cli(arguments: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profiles", type=Path, required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--mode", choices=("simulation", "live"), required=True)
    parser.add_argument("--report", type=Path, required=True)
    options = parser.parse_args(arguments)
    profiles = {profile.name: profile for profile in load_profiles(options.profiles)}
    if options.profile not in profiles:
        raise ValueError("requested load profile was not found")
    profile = profiles[options.profile]
    runner = LoadRunner()
    if options.mode == "simulation":
        report = await runner.run(profile, DeterministicLoadDriver())
    else:
        async with LivePlatformDriver(_live_settings_from_environment()) as driver:
            report = await runner.run(profile, driver)
    write_report(report, options.report)


def main(arguments: Sequence[str] | None = None) -> None:
    asyncio.run(_run_cli(arguments))


if __name__ == "__main__":
    main(sys.argv[1:])


__all__ = [
    "BenchmarkEnvironment",
    "DeterministicLoadDriver",
    "LiveLoadSettings",
    "LivePlatformDriver",
    "LoadAggregate",
    "LoadDriver",
    "LoadProfile",
    "LoadReport",
    "LoadRunner",
    "LoadSample",
    "LoadScenario",
    "MetricSummary",
    "ResourceUtilization",
    "SourceState",
    "load_profiles",
    "main",
    "write_report",
]
