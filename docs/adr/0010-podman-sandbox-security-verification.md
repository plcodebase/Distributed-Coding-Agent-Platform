# ADR 0010: Executable Podman sandbox security verification

- Status: Accepted
- Date: 2026-07-29

## Context

Container arguments alone do not prove isolation. Sequence 10 must exercise the real
rootless runtime with hostile workloads and make security regressions reproducible
without requiring external credentials or infrastructure.

## Decision

- Build a minimal non-root Python sandbox image from `services/sandbox/Containerfile`
  using Podman.
- Keep runtime security tests opt-in via
  `AGENT_PLATFORM_RUN_PODMAN_SECURITY=1`; ordinary unit and integration runs remain
  deterministic on hosts without Podman.
- Verify the effective container UID/GID, cgroup CPU/memory/PID values, and open-file
  limit from inside the sandbox. Inspect `/proc/self/status` to require zero effective
  and bounding capabilities, `NoNewPrivs=1`, and seccomp filter mode.
- Inspect the real command environment and prove that host proxy, provider-key, token,
  credential, and secret variables are absent.
- Attempt direct host credential reads and workspace symlink escapes and require both
  to fail without returning the protected value.
- Attempt root-filesystem writes, external network connections, and Podman socket
  connections and require each to fail.
- Exercise PID exhaustion, memory exhaustion, infinite execution, and oversized output.
  Require structured terminal results, enforced timeout/output flags, and bounded
  retained text.
- Destroy a sandbox during a live command, wait for cleanup, and prove that no
  platform-named command containers remain.
- Promote subprocess resource warnings to errors in runtime verification so child or
  transport leaks cannot be ignored.

## Consequences

- Security claims for Sequence 9 have executable evidence on the supported Podman
  environment.
- The suite is intentionally separate from fast tests, but `make sandbox-security`
  provides a stable invocation.
- Kernel and Podman-version behavior can vary; a failing security test blocks claiming
  production readiness until the runtime or adapter is corrected.
