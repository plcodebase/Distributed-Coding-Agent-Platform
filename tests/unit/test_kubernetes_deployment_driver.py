from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from scripts.deployment_test import DeploymentEnvironment, DeploymentScenario
from scripts.kubernetes_deployment_driver import (
    KubernetesDeploymentDriver,
    LiveTaskSnapshot,
    LiveTaskState,
)

from agent_core.tools import ToolOutputChannel
from sandbox_runtime import ProcessChunk, ProcessResult

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

NOW = datetime(2026, 1, 1, tzinfo=UTC)


class _Runner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    async def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
        max_output_bytes: int,
        environment: Mapping[str, str] | None = None,
    ) -> ProcessResult:
        del cwd, timeout_seconds, max_output_bytes, environment
        call = tuple(argv)
        self.calls.append(call)
        payload: dict[str, object]
        if "--raw" in call:
            payload = {"items": [{"value": "1"}]}
        elif "pods" in call:
            payload = {
                "items": [
                    {
                        "metadata": {"name": f"agent-worker-{suffix}"},
                        "status": {"phase": "Running"},
                    }
                    for suffix in ("a", "b", "c", "d")
                ]
            }
        elif "hpa" in call:
            payload = {"status": {"desiredReplicas": 4}}
        else:
            payload = {}
        return ProcessResult(
            chunks=(
                ProcessChunk(
                    channel=ToolOutputChannel.STDOUT,
                    text=json.dumps(payload),
                ),
            ),
            exit_code=0,
        )


class _TaskProbe:
    def __init__(self) -> None:
        self.snapshots = 0

    async def submit(self, count: int) -> tuple[str, ...]:
        return tuple(f"task-{index}" for index in range(count))

    async def snapshot(self, task_ids: tuple[str, ...]) -> LiveTaskSnapshot:
        self.snapshots += 1
        if self.snapshots < 3:
            states = tuple(
                LiveTaskState(
                    task_id=task_id,
                    status="active" if index == 0 else "queued",
                    worker_pod="agent-worker-a" if index == 0 else None,
                )
                for index, task_id in enumerate(task_ids)
            )
            return LiveTaskSnapshot(states=states, queue_depth=len(task_ids) - 1)
        return LiveTaskSnapshot(
            states=tuple(
                LiveTaskState(
                    task_id=task_id,
                    status="completed",
                    commit_count=1,
                    recovered_from_checkpoint=index == 0,
                )
                for index, task_id in enumerate(task_ids)
            ),
            queue_depth=0,
        )


@pytest.mark.asyncio
async def test_live_driver_measures_metrics_scale_removal_and_recovery(tmp_path: Path) -> None:
    executable = tmp_path / "kubectl"
    executable.write_text("placeholder")
    executable.chmod(0o700)
    runner = _Runner()
    ticks = 0

    def clock() -> datetime:
        nonlocal ticks
        value = NOW + timedelta(seconds=ticks)
        ticks += 1
        return value

    driver = KubernetesDeploymentDriver(
        environment=DeploymentEnvironment(
            system="Linux",
            release="1",
            machine="amd64",
            python="3.12",
            cluster_uid="cluster-a",
            kubernetes_version="v1.34",
            node_summary="four sandbox worker nodes",
        ),
        task_probe=_TaskProbe(),
        kubectl_executable=executable,
        runner=runner,
        poll_seconds=0.001,
        sleep=lambda _: _completed_sleep(),
        clock=clock,
    )
    scenario = DeploymentScenario(
        name="abrupt_worker_recovery",
        accepted_tasks=3,
        initial_workers=3,
        maximum_workers=10,
        target_tasks_per_worker=1,
        work_ticks=1,
        remove_at_tick=1,
        removal_mode="abrupt",
    )

    observation = await driver.execute(scenario)

    assert observation.completed_tasks == 3
    assert observation.lost_tasks == 0
    assert observation.scale_up_observed
    assert observation.removed_worker_name == "agent-worker-a"
    assert observation.checkpoint_recovery_observed
    delete = next(call for call in runner.calls if "delete" in call)
    assert delete[-3:] == ("--wait=false", "--grace-period=0", "--force")
    assert sum("--raw" in call for call in runner.calls) == 2


@pytest.mark.asyncio
async def test_live_driver_fails_closed_when_metric_is_missing(tmp_path: Path) -> None:
    executable = tmp_path / "kubectl"
    executable.write_text("placeholder")
    executable.chmod(0o700)
    runner = _Runner()

    async def missing_metric_run(*args: object, **kwargs: object) -> ProcessResult:
        del args, kwargs
        return ProcessResult(
            chunks=(ProcessChunk(channel=ToolOutputChannel.STDOUT, text='{"items": []}'),),
            exit_code=0,
        )

    runner.run = missing_metric_run  # type: ignore[method-assign]
    driver = KubernetesDeploymentDriver(
        environment=DeploymentEnvironment(
            system="Linux",
            release="1",
            machine="amd64",
            python="3.12",
            cluster_uid="cluster-a",
            kubernetes_version="v1.34",
            node_summary="nodes",
        ),
        task_probe=_TaskProbe(),
        kubectl_executable=executable,
        runner=runner,
    )

    with pytest.raises(ValueError, match="external metric"):
        await driver.execute(
            DeploymentScenario(
                name="graceful_worker_drain",
                accepted_tasks=2,
                initial_workers=1,
                maximum_workers=2,
                target_tasks_per_worker=1,
                work_ticks=1,
                remove_at_tick=1,
                removal_mode="graceful",
            )
        )


def test_live_driver_validates_configuration_and_task_state(tmp_path: Path) -> None:
    executable = tmp_path / "kubectl"
    executable.write_text("placeholder")
    executable.chmod(0o700)
    environment = DeploymentEnvironment(
        system="Linux",
        release="1",
        machine="amd64",
        python="3.12",
        cluster_uid="cluster-a",
        kubernetes_version="v1.34",
        node_summary="nodes",
    )
    with pytest.raises(ValueError, match="absolute"):
        KubernetesDeploymentDriver(
            environment=environment,
            task_probe=_TaskProbe(),
            kubectl_executable=Path("relative-kubectl"),
        )
    with pytest.raises(ValueError, match="namespace"):
        KubernetesDeploymentDriver(
            environment=environment,
            task_probe=_TaskProbe(),
            kubectl_executable=executable,
            namespace="INVALID_NAMESPACE",
        )
    with pytest.raises(ValueError, match="poll_seconds"):
        KubernetesDeploymentDriver(
            environment=environment,
            task_probe=_TaskProbe(),
            kubectl_executable=executable,
            poll_seconds=float("inf"),
        )
    with pytest.raises(ValueError, match="max_samples"):
        KubernetesDeploymentDriver(
            environment=environment,
            task_probe=_TaskProbe(),
            kubectl_executable=executable,
            max_samples=1,
        )
    with pytest.raises(ValueError, match="cluster identity"):
        KubernetesDeploymentDriver(
            environment=DeploymentEnvironment(
                system="Linux",
                release="1",
                machine="amd64",
                python="3.12",
            ),
            task_probe=_TaskProbe(),
            kubectl_executable=executable,
        )

    with pytest.raises(Exception, match="only active"):
        LiveTaskState(task_id="task", status="queued", worker_pod="agent-worker-a")
    with pytest.raises(Exception, match="only completed"):
        LiveTaskState(task_id="task", status="failed", commit_count=1)
    with pytest.raises(Exception, match="unique"):
        LiveTaskSnapshot(
            states=(
                LiveTaskState(task_id="task", status="queued"),
                LiveTaskState(task_id="task", status="queued"),
            ),
            queue_depth=2,
        )
    with pytest.raises(Exception, match="queue depth"):
        LiveTaskSnapshot(
            states=(LiveTaskState(task_id="task", status="queued"),),
            queue_depth=0,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("result", "expected"),
    [
        (ProcessResult(chunks=(), exit_code=-9, timed_out=True), "timed out"),
        (ProcessResult(chunks=(), exit_code=-9, output_truncated=True), "exceeded"),
        (ProcessResult(chunks=(), exit_code=1), "command failed"),
    ],
)
async def test_live_driver_maps_bounded_command_failures(
    tmp_path: Path,
    result: ProcessResult,
    expected: str,
) -> None:
    executable = tmp_path / "kubectl"
    executable.write_text("placeholder")
    executable.chmod(0o700)

    class ResultRunner(_Runner):
        async def run(self, *args: object, **kwargs: object) -> ProcessResult:
            del args, kwargs
            return result

    driver = KubernetesDeploymentDriver(
        environment=DeploymentEnvironment(
            system="Linux",
            release="1",
            machine="amd64",
            python="3.12",
            cluster_uid="cluster-a",
            kubernetes_version="v1.34",
            node_summary="nodes",
        ),
        task_probe=_TaskProbe(),
        kubectl_executable=executable,
        runner=ResultRunner(),
    )

    with pytest.raises((TimeoutError, ValueError, RuntimeError), match=expected):
        await driver._kubectl_json("get", "pods")


async def _completed_sleep() -> None:
    return None
