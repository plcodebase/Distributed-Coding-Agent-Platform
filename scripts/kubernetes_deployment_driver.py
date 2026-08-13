"""Concrete bounded Kubernetes rolling-deployment evidence driver.

The driver is intentionally dependency-injected at the platform task boundary: it
owns Kubernetes observation and pod removal, while a trusted deployment-specific
``LiveTaskProbe`` submits tasks and reports their durable state. Unit tests inject
both collaborators and never contact a cluster.
"""

from __future__ import annotations

import asyncio
import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, cast

from pydantic import Field, model_validator

from sandbox_runtime import BoundedProcessRunner, ProcessResult
from scripts.deployment_test import (
    BoundedName,
    DeploymentEnvironment,
    DeploymentObservation,
    DeploymentSample,
    DeploymentScenario,
    EvidenceMode,
    RemovalMode,
    _Model,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence
    from typing import Any, Self

MAX_KUBECTL_OUTPUT_BYTES = 4 * 1024 * 1024
MAX_LIVE_SAMPLES = 2_000
MIN_LIVE_SAMPLES = 2
MIN_KUBECTL_OUTPUT_BYTES = 1024
MAX_CONFIGURED_KUBECTL_OUTPUT_BYTES = 16 * 1024 * 1024
MAX_WORKER_PODS = 10_000
MAX_POD_NAME_BYTES = 100
MAX_TASK_IDENTIFIER_BYTES = 255
_NAMESPACE_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9.-]{0,61}[a-z0-9])?$")

type Sleep = Callable[[float], Awaitable[None]]
type Clock = Callable[[], datetime]


class LiveTaskState(_Model):
    """One durable accepted task observed through the platform control plane."""

    task_id: str = Field(min_length=1, max_length=MAX_TASK_IDENTIFIER_BYTES)
    status: Literal["queued", "active", "completed", "failed"]
    worker_pod: BoundedName | None = None
    commit_count: int = Field(default=0, ge=0, le=10)
    recovered_from_checkpoint: bool = False

    @model_validator(mode="after")
    def validate_assignment(self) -> Self:
        if (self.status == "active") != (self.worker_pod is not None):
            raise ValueError("only active task states may carry a worker pod")
        if self.status != "completed" and self.commit_count:
            raise ValueError("only completed tasks may report commits")
        return self


class LiveTaskSnapshot(_Model):
    """Complete bounded state of every identity accepted for one scenario."""

    states: tuple[LiveTaskState, ...] = Field(min_length=1, max_length=10_000)
    queue_depth: int = Field(ge=0, le=10_000)

    @model_validator(mode="after")
    def validate_unique_tasks(self) -> Self:
        identifiers = tuple(item.task_id for item in self.states)
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("live task snapshot identifiers must be unique")
        if self.queue_depth != sum(item.status == "queued" for item in self.states):
            raise ValueError("queue depth must match queued task states")
        return self


class LiveTaskProbe(Protocol):
    """Authenticated task API boundary supplied by deployment composition."""

    async def submit(self, count: int) -> tuple[str, ...]:
        """Durably accept exactly ``count`` unique task identities."""

    async def snapshot(self, task_ids: tuple[str, ...]) -> LiveTaskSnapshot:
        """Read one complete tenant-scoped state snapshot."""


class KubectlRunner(Protocol):
    """Narrow command boundary used for deterministic fake tests."""

    async def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
        max_output_bytes: int,
        environment: Mapping[str, str] | None = None,
    ) -> ProcessResult: ...


@dataclass(frozen=True, slots=True)
class _ClusterSnapshot:
    worker_pods: tuple[BoundedName, ...]
    desired_workers: int


class KubernetesDeploymentDriver:
    """Measure scaling and recovery against one explicitly identified cluster."""

    mode = EvidenceMode.LIVE

    def __init__(
        self,
        *,
        environment: DeploymentEnvironment,
        task_probe: LiveTaskProbe,
        kubectl_executable: Path,
        namespace: str = "agent-platform",
        runner: KubectlRunner | None = None,
        command_timeout_seconds: float = 15,
        poll_seconds: float = 1,
        max_samples: int = 600,
        max_output_bytes: int = MAX_KUBECTL_OUTPUT_BYTES,
        sleep: Sleep = asyncio.sleep,
        clock: Clock | None = None,
    ) -> None:
        if not kubectl_executable.is_absolute():
            raise ValueError("kubectl_executable must be absolute")
        executable = kubectl_executable.resolve(strict=True)
        if not executable.is_file() or not executable.stat().st_mode & 0o111:
            raise ValueError("kubectl_executable must be an absolute executable file")
        if _NAMESPACE_PATTERN.fullmatch(namespace) is None:
            raise ValueError("namespace must be a bounded DNS label")
        if environment.cluster_uid is None:
            raise ValueError("Kubernetes live evidence requires complete cluster identity")
        for name, value, maximum in (
            ("command_timeout_seconds", command_timeout_seconds, 300),
            ("poll_seconds", poll_seconds, 60),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or not 0 < value <= maximum
            ):
                raise ValueError(f"{name} must be finite, positive, and at most {maximum}")
        if type(max_samples) is not int or not (
            MIN_LIVE_SAMPLES <= max_samples <= MAX_LIVE_SAMPLES
        ):
            raise ValueError(f"max_samples must be in [2, {MAX_LIVE_SAMPLES}]")
        if type(max_output_bytes) is not int or not (
            MIN_KUBECTL_OUTPUT_BYTES <= max_output_bytes <= MAX_CONFIGURED_KUBECTL_OUTPUT_BYTES
        ):
            raise ValueError("max_output_bytes must be in [1024, 16777216]")
        if not callable(task_probe.submit) or not callable(task_probe.snapshot):
            raise TypeError("task_probe must implement submit and snapshot")
        self.environment = environment
        self._task_probe = task_probe
        self._kubectl_executable = str(executable)
        self._namespace = namespace
        self._runner = runner or BoundedProcessRunner()
        self._timeout = float(command_timeout_seconds)
        self._poll_seconds = float(poll_seconds)
        self._max_samples = max_samples
        self._max_output = max_output_bytes
        self._sleep = sleep
        self._clock = clock or (lambda: datetime.now(UTC))

    async def execute(self, scenario: DeploymentScenario) -> DeploymentObservation:
        """Submit, scale, remove one worker, and measure durable recovery."""

        await self._require_external_metrics()
        task_ids = await self._task_probe.submit(scenario.accepted_tasks)
        if len(task_ids) != scenario.accepted_tasks or len(set(task_ids)) != len(task_ids):
            raise ValueError("task probe did not return every unique accepted identity")
        removed_worker: BoundedName | None = None
        removed_active_task = False
        removed_at: datetime | None = None
        recovery_at: datetime | None = None
        peak_workers = 0
        recovered_tasks = 0
        duplicate_completions = 0
        samples: list[DeploymentSample] = []

        final_snapshot: LiveTaskSnapshot | None = None
        for tick in range(self._max_samples):
            cluster, tasks = await asyncio.gather(
                self._cluster_snapshot(),
                self._task_probe.snapshot(task_ids),
            )
            self._validate_snapshot(task_ids, tasks)
            final_snapshot = tasks
            peak_workers = max(peak_workers, len(cluster.worker_pods))
            recovered_tasks = max(
                recovered_tasks,
                sum(item.recovered_from_checkpoint for item in tasks.states),
            )
            duplicate_completions = max(
                duplicate_completions,
                sum(max(0, item.commit_count - 1) for item in tasks.states),
            )
            completed = sum(item.status == "completed" for item in tasks.states)
            samples.append(
                DeploymentSample(
                    tick=tick,
                    observed_at=self._clock(),
                    workers=len(cluster.worker_pods),
                    desired_workers=cluster.desired_workers,
                    queued=tasks.queue_depth,
                    queue_metric=tasks.queue_depth,
                    active=sum(item.status == "active" for item in tasks.states),
                    completed=completed,
                    worker_pods=cluster.worker_pods,
                )
            )

            if (
                removed_worker is None
                and tick >= scenario.remove_at_tick
                and peak_workers > scenario.initial_workers
                and cluster.worker_pods
            ):
                active_workers = {
                    item.worker_pod for item in tasks.states if item.worker_pod is not None
                }
                removed_worker = next(
                    (pod for pod in cluster.worker_pods if pod in active_workers),
                    cluster.worker_pods[0],
                )
                removed_active_task = removed_worker in active_workers
                removed_at = self._clock()
                await self._remove_worker(removed_worker, scenario.removal_mode)

            terminal = all(item.status in {"completed", "failed"} for item in tasks.states)
            if terminal and removed_worker is not None:
                recovery_at = self._clock()
                break
            await self._sleep(self._poll_seconds)

        if final_snapshot is None:
            raise RuntimeError("live deployment campaign produced no task snapshot")
        completed_tasks = sum(item.status == "completed" for item in final_snapshot.states)
        lost_tasks = scenario.accepted_tasks - completed_tasks
        recovery_seconds = (
            max(0.0, (recovery_at - removed_at).total_seconds())
            if recovery_at is not None and removed_at is not None
            else None
        )
        return DeploymentObservation(
            accepted_tasks=scenario.accepted_tasks,
            completed_tasks=completed_tasks,
            lost_tasks=lost_tasks,
            duplicate_completions=duplicate_completions,
            recovered_tasks=recovered_tasks,
            initial_workers=scenario.initial_workers,
            peak_workers=peak_workers,
            removed_worker=removed_worker is not None,
            removed_worker_name=removed_worker,
            drained_active_task=(
                scenario.removal_mode is RemovalMode.GRACEFUL and removed_active_task
            ),
            checkpoint_recovery_observed=(
                scenario.removal_mode is RemovalMode.ABRUPT and recovered_tasks > 0
            ),
            recovery_seconds=recovery_seconds,
            scale_up_observed=peak_workers > scenario.initial_workers,
            samples=tuple(samples),
        )

    async def _cluster_snapshot(self) -> _ClusterSnapshot:
        pods, hpa = await asyncio.gather(
            self._kubectl_json(
                "get",
                "pods",
                "-l",
                "app.kubernetes.io/name=agent-worker",
                "-o",
                "json",
            ),
            self._kubectl_json("get", "hpa", "agent-worker", "-o", "json"),
        )
        items = pods.get("items")
        if not isinstance(items, list) or len(items) > MAX_WORKER_PODS:
            raise ValueError("worker pod response has an invalid item set")
        names: list[BoundedName] = []
        for item in items:
            if not isinstance(item, dict):
                raise TypeError("worker pod response contains a non-object")
            metadata = item.get("metadata")
            status = item.get("status")
            if not isinstance(metadata, dict) or not isinstance(status, dict):
                raise TypeError("worker pod response is missing metadata or status")
            name = metadata.get("name")
            phase = status.get("phase")
            if (
                not isinstance(name, str)
                or len(name) > MAX_POD_NAME_BYTES
                or _NAMESPACE_PATTERN.fullmatch(name) is None
            ):
                raise ValueError("worker pod name is invalid")
            if phase == "Running" and metadata.get("deletionTimestamp") is None:
                names.append(name)
        desired = hpa.get("status", {}).get("desiredReplicas")
        if type(desired) is not int or not 0 <= desired <= MAX_WORKER_PODS:
            raise ValueError("worker HPA response has an invalid desired replica count")
        return _ClusterSnapshot(worker_pods=tuple(sorted(names)), desired_workers=desired)

    async def _require_external_metrics(self) -> None:
        for metric in (
            "agent_platform_queue_depth_total",
            "agent_platform_oldest_queued_seconds",
        ):
            payload = await self._kubectl_json(
                "get",
                "--raw",
                f"/apis/external.metrics.k8s.io/v1beta1/namespaces/{self._namespace}/{metric}",
            )
            if not isinstance(payload.get("items"), list) or not payload["items"]:
                raise ValueError(f"required external metric {metric} is unavailable")

    async def _remove_worker(self, pod: str, mode: RemovalMode) -> None:
        arguments = ["delete", "pod", pod, "--wait=false"]
        if mode is RemovalMode.ABRUPT:
            arguments.extend(("--grace-period=0", "--force"))
        await self._kubectl(*arguments)

    async def _kubectl_json(self, *arguments: str) -> dict[str, Any]:
        output = await self._kubectl(*arguments)
        try:
            value = json.loads(output)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("kubectl returned invalid JSON") from error
        if not isinstance(value, dict):
            raise TypeError("kubectl JSON response must be an object")
        return cast("dict[str, Any]", value)

    async def _kubectl(self, *arguments: str) -> bytes:
        result = await self._runner.run(
            (self._kubectl_executable, "--namespace", self._namespace, *arguments),
            cwd=Path("/"),
            timeout_seconds=self._timeout,
            max_output_bytes=self._max_output,
            environment={"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "PATH": "/usr/bin:/bin"},
        )
        if result.timed_out:
            raise TimeoutError("kubectl command timed out")
        if result.output_truncated:
            raise ValueError("kubectl output exceeded its limit")
        if result.exit_code != 0:
            raise RuntimeError("kubectl command failed")
        return "".join(
            chunk.text for chunk in result.chunks if chunk.channel.value == "stdout"
        ).encode("utf-8")

    @staticmethod
    def _validate_snapshot(task_ids: tuple[str, ...], snapshot: LiveTaskSnapshot) -> None:
        if {item.task_id for item in snapshot.states} != set(task_ids):
            raise ValueError("task probe snapshot did not contain exactly the accepted identities")


__all__ = [
    "KubectlRunner",
    "KubernetesDeploymentDriver",
    "LiveTaskProbe",
    "LiveTaskSnapshot",
    "LiveTaskState",
]
