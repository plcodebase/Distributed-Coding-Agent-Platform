# ADR 0009: Hardened rootless Podman sandbox

- Status: Accepted
- Date: 2026-07-29

## Context

Sequence 8 deliberately exposed only a provider-neutral sandbox contract and an unsafe,
opt-in local adapter. Production execution needs an enforceable boundary for untrusted
repository commands. The boundary must survive cancellation and output overflow without
leaking child processes, credentials, host paths, or runtime administration access.

## Decision

- Implement `PodmanSandbox` in `sandbox-runtime`; keep Podman imports and process details
  outside `agent-core`.
- Require a verified rootless Podman engine and inspect the configured image before use.
  Production images must be immutable SHA-256 digest references; development and test
  compositions may use explicit local tags.
- Execute every command in a uniquely named disposable container using argv only. Never
  invoke a shell implicitly.
- Mount exactly the owned worktree at `/workspace`. Reject host paths that are ambiguous
  in Podman's mount grammar. Do not mount a home directory, credential directory, SSH
  agent, cloud configuration, or Podman service socket.
- Use a read-only root filesystem, a bounded private tmpfs, network mode `none`, a
  non-root UID/GID, rootless keep-id user namespace mapping, all-capability drop,
  `no-new-privileges`, the default seccomp policy, private PID/cgroup/IPC/UTS
  namespaces, and no generated hosts file. Ignore image-declared volumes.
- Apply explicit CPU, memory/swap, PID, open-file, timeout, output, control-output,
  direct-write, and snapshot-retention limits.
- Clear image-default environment values, disable host proxy propagation, and give the
  container a fixed minimal environment. The Podman control process receives
  only a bounded allowlist needed to reach the rootless engine; provider credentials and
  the worker environment are never copied into the command.
- Stream bounded stdout/stderr through the shared process runner. Timeout, cancellation,
  overflow, and consumer abandonment terminate the Podman process group and force-remove
  the one targeted container.
- Assign cleanup ownership atomically when cancellation and generator finalization meet.
  Retain the exact container identity until targeted removal succeeds, wait for removal
  before closing the runner, and retry retained identities during destruction. Cleanup
  blocks reuse after a partial failure.
- Serialize commands and direct workspace operations. Snapshot restore cancels active
  containers first and accepts only immutable snapshots retained by that sandbox.

## Consequences

- `PodmanSandbox` is the production command boundary; `LocalSandbox` remains explicitly
  unsafe and cannot be enabled in production.
- The adapter assumes a supported rootless Podman installation and an exclusively owned
  per-run Git worktree.
- Optional stronger runtimes such as gVisor remain future work. The default Podman
  seccomp profile is retained and is never disabled.
