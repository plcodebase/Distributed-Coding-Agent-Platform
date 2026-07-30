import asyncio
import json
import os
import shutil
import subprocess
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from agent_core.sandbox import CommandCompleted, CommandOutput, CommandSpec
from sandbox_runtime import GitWorktreeManager, PodmanSandbox, PodmanSandboxConfig

pytestmark = [
    pytest.mark.security,
    pytest.mark.skipif(
        os.getenv("AGENT_PLATFORM_RUN_PODMAN_SECURITY") != "1",
        reason="set AGENT_PLATFORM_RUN_PODMAN_SECURITY=1 to exercise Podman isolation",
    ),
]

PODMAN = shutil.which("podman") or "/usr/bin/podman"
IMAGE = os.getenv(
    "AGENT_PLATFORM_SANDBOX_IMAGE",
    "localhost/agent-platform-sandbox:sequence-10",
)


def _git(repository: Path, *arguments: str) -> None:
    git = shutil.which("git") or "/usr/bin/git"
    subprocess.run(  # noqa: S603 - resolved executable and fixed test arguments
        (git, "-C", os.fspath(repository), *arguments),
        check=True,
        capture_output=True,
    )


@pytest.fixture
async def podman_sandbox(tmp_path: Path) -> AsyncIterator[PodmanSandbox]:
    source = tmp_path / "source"
    source.mkdir()
    _git(source, "init", "-q")
    _git(source, "config", "user.name", "Security Test")
    _git(source, "config", "user.email", "security@example.invalid")
    (source / "tracked.txt").write_text("workspace\n", encoding="utf-8")
    _git(source, "add", ".")
    _git(source, "commit", "-q", "-m", "initial")
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="podman-security",
    )
    sandbox = await PodmanSandbox.create(
        workspace,
        config=PodmanSandboxConfig(
            image=IMAGE,
            environment="test",
            podman_executable=PODMAN,
            cpu_limit=0.5,
            memory_limit_bytes=64 * 1024 * 1024,
            pids_limit=64,
            open_files_limit=128,
            tmpfs_limit_bytes=8 * 1024 * 1024,
        ),
    )
    try:
        yield sandbox
    finally:
        await sandbox.destroy()


async def _run(
    sandbox: PodmanSandbox,
    source: str,
    *,
    command_timeout: float = 5,
    output_limit: int = 64 * 1024,
) -> tuple[str, CommandCompleted]:
    events = [
        event
        async for event in sandbox.execute(
            CommandSpec(
                argv=("python", "-c", source),
                timeout_seconds=command_timeout,
                max_output_bytes=output_limit,
            )
        )
    ]
    output = "".join(event.chunk for event in events if isinstance(event, CommandOutput))
    terminal = events[-1]
    assert isinstance(terminal, CommandCompleted)
    return output, terminal


async def test_container_is_non_root_and_cgroup_limits_are_active(
    podman_sandbox: PodmanSandbox,
) -> None:
    output, terminal = await _run(
        podman_sandbox,
        """
import json, os, resource
def read(path):
    with open(path, encoding="utf-8") as stream:
        return stream.read().strip()
print(json.dumps({
    "uid": os.getuid(),
    "gid": os.getgid(),
    "nofile": resource.getrlimit(resource.RLIMIT_NOFILE)[0],
    "cpu": read("/sys/fs/cgroup/cpu.max"),
    "memory": read("/sys/fs/cgroup/memory.max"),
    "pids": read("/sys/fs/cgroup/pids.max"),
}))
""",
    )
    assert terminal.exit_code == 0
    limits = json.loads(output)
    assert limits["uid"] == 10001
    assert limits["gid"] == 10001
    assert limits["nofile"] == 128
    quota, period = (int(value) for value in limits["cpu"].split())
    assert quota / period <= 0.5
    assert int(limits["memory"]) == 64 * 1024 * 1024
    assert int(limits["pids"]) == 64


async def test_host_credentials_and_symlink_escape_are_inaccessible(
    podman_sandbox: PodmanSandbox,
    tmp_path: Path,
) -> None:
    credential = tmp_path / "host-credential"
    credential.write_text("host-secret-value", encoding="utf-8")
    (podman_sandbox.workspace.root / "escape").symlink_to(credential)
    script = f"""
from pathlib import Path
paths = [Path({os.fspath(credential)!r}), Path("/workspace/escape")]
for path in paths:
    try:
        print(path.read_text())
    except Exception:
        print("blocked")
"""
    output, terminal = await _run(podman_sandbox, script)
    assert terminal.exit_code == 0
    assert output.splitlines() == ["blocked", "blocked"]
    assert "host-secret-value" not in output


async def test_root_filesystem_network_and_service_socket_are_inaccessible(
    podman_sandbox: PodmanSandbox,
) -> None:
    output, terminal = await _run(
        podman_sandbox,
        """
import pathlib, socket
checks = []
try:
    pathlib.Path("/etc/agent-write").write_text("no")
except Exception:
    checks.append("root-read-only")
try:
    socket.create_connection(("1.1.1.1", 53), timeout=0.2)
except Exception:
    checks.append("network-blocked")
for path in ("/run/podman/podman.sock", "/var/run/podman/podman.sock"):
    try:
        socket.socket(socket.AF_UNIX).connect(path)
    except Exception:
        checks.append("socket-blocked")
print(",".join(checks))
""",
    )
    assert terminal.exit_code == 0
    assert set(output.strip().split(",")) == {
        "root-read-only",
        "network-blocked",
        "socket-blocked",
    }


async def test_pid_limit_is_enforced(
    podman_sandbox: PodmanSandbox,
) -> None:
    fork_output, fork_terminal = await _run(
        podman_sandbox,
        """
import os, signal, time
children = []
blocked = False
try:
    for _ in range(256):
        pid = os.fork()
        if pid == 0:
            time.sleep(5)
            os._exit(0)
        children.append(pid)
except OSError:
    blocked = True
finally:
    for pid in children:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    for pid in children:
        try:
            os.waitpid(pid, 0)
        except ChildProcessError:
            pass
print("pids-blocked" if blocked else "pids-unbounded")
""",
        command_timeout=10,
    )
    assert fork_terminal.exit_code == 0
    assert fork_output.strip() == "pids-blocked"


async def test_memory_limit_is_enforced(
    podman_sandbox: PodmanSandbox,
) -> None:
    _, memory_terminal = await _run(
        podman_sandbox,
        "value = bytearray(256 * 1024 * 1024); print(len(value))",
        command_timeout=10,
    )
    assert memory_terminal.exit_code != 0


async def test_timeout_limit_is_enforced(
    podman_sandbox: PodmanSandbox,
) -> None:
    _, timeout_terminal = await _run(
        podman_sandbox,
        "import time; time.sleep(60)",
        command_timeout=0.2,
    )
    assert timeout_terminal.timed_out is True


async def test_output_limit_is_enforced(
    podman_sandbox: PodmanSandbox,
) -> None:
    output, output_terminal = await _run(
        podman_sandbox,
        "print('x' * 1000000)",
        output_limit=1024,
    )
    assert len(output.encode("utf-8")) <= 1024
    assert output_terminal.output_truncated is True


async def test_destroy_terminates_running_container(
    podman_sandbox: PodmanSandbox,
) -> None:
    command = asyncio.create_task(
        _run(
            podman_sandbox,
            "import time; print('started', flush=True); time.sleep(60)",
            command_timeout=120,
        )
    )
    for _ in range(200):
        if podman_sandbox._active:
            break
        await asyncio.sleep(0.01)
    assert podman_sandbox._active

    await podman_sandbox.destroy()
    result = await asyncio.gather(command, return_exceptions=True)
    assert isinstance(result[0], asyncio.CancelledError)
    listed = await asyncio.create_subprocess_exec(
        PODMAN,
        "ps",
        "--all",
        "--filter",
        "name=agent-sbx-",
        "--format",
        "{{.Names}}",
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await listed.communicate()
    assert listed.returncode == 0, stderr.decode(errors="replace")
    assert stdout.decode().strip() == ""
