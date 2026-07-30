# Sequences 9–12 review and hardening plan

- Status: Implemented and verified
- Date: 2026-07-29
- Scope: PR 09 through PR 12 only

## Review verdict

Sequences 9–12 implement the intended architecture and pass their existing unit,
integration, and real-Podman suites. The rootless sandbox, hostile-workload tests,
LiteLLM routing deployment, and typed gateway boundary are functional. No remaining P0
correctness failure was found after the prior cleanup and lifecycle hardening.

The P1 gaps found during the review were implemented and verified in this pass. PR 13
idempotency, retries, circuit breakers, and rate limits are deliberately excluded.

| Sequence | Area | Initial review result |
|---|---|---|
| 9 | Rootless Podman execution boundary | Pass with native-watchdog and log-policy gaps |
| 10 | Hostile-workload security evidence | Pass with effective-kernel-state coverage gaps |
| 11 | LiteLLM routing and fallback deployment | Pass with network and release-image gaps |
| 12 | Typed attributed gateway client | Pass with request-bound and header-safety gaps |

## Step-by-step review

### Sequence 9 — hardened Podman sandbox

Implemented and verified:

- rootless runtime verification and image inspection;
- mandatory production image digest references;
- one uniquely named disposable container per argv-only command;
- non-root keep-id mapping, all-capability drop, `no-new-privileges`, default seccomp,
  and private namespaces;
- read-only root filesystem, bounded tmpfs, offline networking, ignored image volumes,
  cleared image environment, and disabled host-proxy propagation;
- CPU, memory/swap, PID, open-file, output, command-duration, write, control, and
  snapshot limits;
- exact worktree mount, target-specific cancellation, retained cleanup identities, and
  retryable destruction;
- provider-neutral composition through the `Sandbox` protocol.

Gaps found and resolved:

- **P1 — no runtime-native watchdog:** command duration was enforced only by the
  worker-side process runner. Added Podman's rounded-up native `--timeout` while
  preserving the outer sub-second timeout and targeted forced removal.
- **P1 — default container logging:** the bounded attached stream did not prevent the
  runtime logging driver from retaining command output. Added `--log-driver=none` and
  `--restart=no`.
- **P1 — incomplete workspace bind options:** added explicit `nodev,nosuid` to the
  existing rootless, no-new-privileges workspace boundary.

### Sequence 10 — sandbox security verification

Implemented and verified:

- effective non-root UID/GID and cgroup CPU, memory, PID, and open-file limits;
- blocked host credentials, symlink escapes, root writes, network access, and Podman
  sockets;
- PID exhaustion, memory exhaustion, timeouts, output overflow, process-group
  termination, and destroy-time cleanup;
- warning-as-error subprocess leak detection;
- no remaining platform-named test containers.

Gaps found and resolved:

- **P1 — kernel security state inferred from arguments:** the real-container suite now
  parses `/proc/self/status` and requires zero effective/bounding capabilities,
  `NoNewPrivs=1`, and seccomp filter mode.
- **P1 — environment isolation not observed in-container:** the hostile suite now
  injects host proxy and provider-secret sentinels and proves they are absent from the
  executed process.
- **P2 — native watchdog/log policy construction coverage:** the deterministic argv
  test now locks the native timeout, disabled logging, restart policy, and mount
  options while hostile tests retain externally visible limit coverage.

### Sequence 11 — LiteLLM Proxy deployment

Implemented and verified:

- loopback-only LiteLLM host exposure;
- exactly five logical aliases;
- two deterministic local deployments and two production provider families;
- provider credentials scoped to LiteLLM;
- read-only configuration/root filesystem, dropped capabilities,
  `no-new-privileges`, and bounded tmpfs;
- explicit retries, cooldown, upstream timeouts, and compatible fallback;
- live route, outage/fallback, typed streaming, and cleanup tests.

Gaps found and resolved:

- **P1 — fake providers shared the general Compose network:** they now attach only to
  the internal `llm-upstreams` network.
- **P1 — release image injection was not explicit:** `LITELLM_IMAGE` is injectable,
  retains a pinned local development tag, and documents the digest requirement for
  release deployments.
- **P2 — deployment tests did not lock network membership:** tests now require the
  exact internal-upstream/dedicated-egress topology, and the live fallback suite passes
  with it.

### Sequence 12 — typed gateway client and normalized streaming

Implemented and verified:

- required tenant, session, run, turn, model-call, request, and route attribution;
- protected SDK metadata and HTTP headers;
- five-route allowlist before contacting LiteLLM;
- revalidation of all normalized events;
- one-terminal-event enforcement and incomplete/post-terminal rejection;
- cumulative stream event/UTF-8 byte limits;
- opaque failures and cancellation-safe, retryable cleanup that blocks reuse.

Gaps found and resolved:

- **P1 — direct client requests were not byte-bounded:** `GatewayClient` now enforces a
  configurable request-byte limit below a closed platform ceiling before it creates
  the delegate stream.
- **P1 — attribution IDs accepted HTTP control characters:** model-call and request IDs
  now use a header-safe identifier contract and reject unsafe input before adapter or
  network code.
- **P2 — request collection counts had only loop-level bounds:** `GatewayRequest` now
  has hard message and tool-definition collection ceilings.
- Multibyte accounting, delegate non-invocation, unsafe IDs, and configuration limits
  are covered by unit tests.

## Deferred by scope

The following remain PR 13 or later and must not be presented as Sequence 9–12 gaps:

- durable request-id idempotency and stored-result replay;
- client exponential backoff, circuit breakers, and tenant/model rate limits;
- cost persistence and distributed telemetry;
- gVisor or Firecracker execution;
- Kubernetes deployment;
- real-provider compatibility and model-quality evaluation.

## Implementation outcome

| Gap | Resolution |
|---|---|
| Worker-only command duration | Added rounded-up Podman-native `--timeout` |
| Runtime log/disk amplification | Added `--log-driver=none` and `--restart=no` |
| Incomplete workspace mount options | Added `nodev,nosuid` |
| Inferred kernel security state | Added effective capability, `NoNewPrivs`, and seccomp assertions |
| Unobserved command environment | Added proxy/provider-secret injection probes and absence assertions |
| Shared gateway network | Added dedicated `llm-egress` and internal `llm-upstreams` networks |
| Non-injectable release gateway image | Added `LITELLM_IMAGE` with a versioned local default and release-digest rule |
| Unbounded direct gateway requests | Added configurable pre-delegate request-byte enforcement |
| HTTP-unsafe attribution IDs | Added header-safe model-call/request ID contracts |
| Unbounded request collections | Added hard message and tool-definition collection ceilings |
| Gateway test resource residue | Added exact container and gateway-owned network cleanup |

## Verification results

Completed verification:

1. Focused hardening suite: 97 tests passed.
2. Ruff formatting/lint: passed.
3. Strict mypy: passed across 62 source files.
4. Unit suite: 317 tests passed with 87.09% total coverage.
5. Integration suite: 3 tests passed.
6. Real rootless Podman sandbox suite: 8 tests passed.
7. Live Podman LiteLLM route/fallback/streaming suite: 3 tests passed.
8. Pre-commit: all hooks passed.
9. Lock verification: 93 packages resolved; frozen sync checked 90 applicable packages.
10. Offline source/wheel builds: all five workspace packages passed.
11. Podman Compose configuration: passed.
12. Gateway-owned containers and networks after the live suite: none.

The dependency set and lockfile did not change during this review pass. A fresh
network-backed `pip-audit` advisory refresh could not reach PyPI, and permission to send
the dependency inventory externally was not granted. The immediately preceding
successful audit of this same 93-package lockfile remains the available vulnerability
evidence; this document does not claim a newer advisory-feed snapshot.
