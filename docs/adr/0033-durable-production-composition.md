# ADR 0033: Durable production composition and node execution boundary

## Status

Accepted

## Context

Sequences 1–32 established the platform contracts and tested components, but production startup
still depended on unspecified composition. In-memory checkpoint/transcript implementations could
not support cross-worker recovery, and an unprivileged Kubernetes worker cannot safely own a host
container-runtime socket. Replica-local worker identifiers also collide under horizontal scaling.

## Decision

- Provide reviewed API, worker, scheduler, and sandbox-node production composition roots in this
  repository. Kubernetes names the exact worker and scheduler factories rather than reading Python
  references from Secrets.
- Persist accepted tasks atomically with queued runs. PostgreSQL remains authoritative for queue,
  leases, events, transcript, tasks, approvals, compactions, memory, retry timing, audit, and artifact
  metadata. Redis carries bounded wake-up hints only.
- Store source archives, workspace snapshots, checkpoints, and final patches as immutable,
  checksum-verified object-store values. Every committing operation is tenant-scoped and fenced by
  the current run/workspace generation.
- Persist the complete normalized checkpoint message list and append all assistant/tool/correction
  transcript messages before terminal success. Persist the exact tool-call ID on every checkpoint
  and allow distinct mutations from one message sequence. Replacement workers reconstruct context
  from the completed compaction watermark, durable history, active memory, and current task plan.
- Treat Redis publication as a lossy latency hint. Failure is logged and workers continue through
  bounded PostgreSQL polling; an already-committed task is never reported failed because Redis is
  unavailable.
- Put rootless Podman behind a mutually authenticated node agent. Only that trusted DaemonSet mounts
  `/run/user/1000/podman/podman.sock`; workers and sandbox containers never do. The node-agent and
  host workspace path are both `/var/lib/agent-platform/workspaces` so host-side bind resolution is
  unambiguous.
- Derive worker, scheduler, snapshot, and memory-processing identities from a bounded digest of the
  Kubernetes pod UID. Route pricing and context budgets must name every configured route exactly and
  reject duplicate JSON keys.
- Exclude only process/bootstrap and pure dependency-construction modules from line-coverage
  measurement. Static composition and Kubernetes contract tests verify their exact environment and
  factory contracts; safety, persistence, and adapter behavior remains inside the branch-coverage
  gate.

## Consequences

- Production no longer silently falls back to process memory or an unspecified sandbox adapter.
- Worker loss can be recovered from durable transcript and object-backed workspace state without
  accepting writes from the stale generation.
- Sandbox nodes are privileged only to the extent required to control their own rootless Podman
  service; this is a smaller trust boundary than mounting runtime control into general workers.
- Cluster operators must provision private mTLS material, rootless Podman, an exact private workspace
  directory, immutable image digests, PostgreSQL, Redis, object storage, OIDC, and route policies.
- Static and component evidence still does not prove live recovery, isolation, scale, or SLOs. Those
  remain explicit release gates in the production-readiness review.
