# Production readiness review

Reviewed against the original implementation design on 2026-08-21. The user's Podman-only
requirement supersedes every Docker reference in that draft. No Docker-compatible daemon, socket,
CLI, build, or test path is part of the supported architecture.

This is a source, test, deployment, migration, and live-boundary review of the complete repository.
It distinguishes implemented behavior from production evidence:

- `contract`: typed interfaces, schemas, or manifests exist;
- `component-tested`: deterministic tests exercise the implementation;
- `integrated`: a production composition root connects durable dependencies;
- `live-verified locally`: the real boundary passed in a disposable local environment;
- `production-proven`: immutable evidence from the target release environment exists.

Static checks, simulations, and local tests are not reported as production scale, latency,
availability, recovery, cost, or coding-success measurements.

## Executive verdict

The repository is a substantial production-oriented implementation, not a prototype shell. The
agent loop, gateway adapter, workspace tools, rootless-Podman node boundary, durable API, queue,
leases, approvals, checkpoints, object storage, context management, telemetry, and Kubernetes base
are implemented. A real local distributed journey now crosses authenticated API submission, real
PostgreSQL and MinIO, generated TLS 1.3 mutual authentication, the node service, rootless Podman,
approval suspension, replacement workers, checkpoint restore, command execution, event replay,
final artifact download, and an epoch-isolated rewind with a different replacement patch.

It is **not production-ready yet**. Durable rewind now forks a monotonic execution epoch and keeps
abandoned transcript, tool, model, memory, approval, event, and artifact state out of the active
branch. Data lifecycle now includes legal holds, bounded retention/deletion jobs, export-first
tenant deletion, audit verification, and an isolated-restore consistency verifier. The remaining
blockers are release-environment work: a site overlay, production identities and providers,
managed backup/rotation configuration plus a retained restore rehearsal, a signed release/admission
run, and live load/failure/rolling proof.

The current workspace also contains the accumulated implementation as uncommitted tracked and
untracked changes. Those changes are valid review inputs, not a release source revision. They must
be split or reviewed as bounded commits and leave a clean checkout before the fail-closed release
builder will accept them.

## Design-phase review

| Original phase | Status | What is achieved | Remaining production gap |
|---|---|---|---|
| 0. Bootstrap | Integrated; release supply chain component-tested | Python 3.12 workspace, locked dependencies, Ruff, strict mypy, pytest, branch coverage, pre-commit, dependency audit, structured settings/logging, CI, Podman Compose dependencies, and health checks. A protected-runner workflow validates digest-injected bases and deny-first contexts, verifies release-tool hashes, builds/mirrors four production images with Podman, creates SPDX SBOMs, rejects unsuppressed High/Critical findings with a fresh hash-checked Grype database, signs image digests plus SPDX/SLSA predicates, signs a bounded evidence manifest, and reverifies promotion inputs. | Configure exact materials, tool paths/hashes, registry credentials, and OIDC on the protected runner; retain one successful tag-run evidence set and enforce its identity/attestations in target admission. |
| 1. Single-process agent | Integrated | Provider-neutral streamed loop, fourteen typed events, bounded turns/tools/context/output, fake gateway, Agents SDK model adapter, list/read/search/edit/command/approval/plan tools, a safe event-rendering CLI, and a bounded in-memory developer session are implemented. | Run the local CLI against the promoted gateway and sandbox image as release evidence; durable use remains the HTTP/distributed path. |
| 2. Editing/checkpoints | Live-verified locally with Podman | Detached private worktrees, descriptor-safe bounded tools, optimistic edit transactions, pre-mutation checkpoints, stable hashes, object-backed snapshots, final binary-safe patches, duplicate-outcome reuse, and epoch-isolated durable rewind are implemented. The distributed E2E completes epoch 1, rewinds to its first checkpoint, reuses a logical tool ID with different arguments in epoch 2, and proves the replacement patch is active while the abandoned patch remains auditable. | Repeat the same journey using promoted images and the target-cluster topology. |
| 3. Sandbox runtime | Live-verified locally with Podman | Rootless Podman replaces the draft runtime. The node owns the socket behind mTLS; sandboxes have no network, read-only root, one workspace mount, non-root identity, reduced capabilities, seccomp, resource/output/time limits, process-group cancellation, and targeted cleanup. | Repeat the security suite on the release kernel/node/image combination and retain evidence; evaluate a stronger boundary for hostile multi-tenant code. |
| 4. LLM gateway | Integrated; fake-upstream live verification | Agents SDK model-layer adapter, LiteLLM routes, typed normalization, durable request idempotency, timeout/retry/backoff, circuit breaker, rate and token limits, fallback tests, usage and cost attribution, and credential separation exist. | Validate at least two real production deployments, upstream fallback metadata, spend reconciliation, and provider outage behavior. |
| 5. Sessions/API | Integrated; event transport and lifecycle storage live-verified locally | PostgreSQL/Alembic models, tenant-scoped FastAPI operations, OIDC/JWKS auth, request bounds, idempotent submissions, durable messages/events/state, replay, WebSocket streaming, approvals, rewind, artifacts, audit writes, legal holds, audit export, retention/deletion jobs, and tenant tombstones exist. A real PostgreSQL test proves the lifecycle migration, hold blocking, fenced cleanup retry, tombstoning, and isolated restore/object verification. | Repeat reconnect and process replacement through the target ingress/load balancer; configure and rehearse production issuer/JWKS, credential, and certificate rotation plus managed backups. |
| 6. Distributed workers | Integrated and recovery path live-verified locally | API execution is separated; PostgreSQL `SKIP LOCKED` scheduling, workers, heartbeat leases, lost/requeue recovery, workspace fencing, cancellation, draining, epoch-fenced persistence, and Redis wake-up hints with polling fallback exist. The local two-epoch E2E resumes one run across worker instances A through E. | Demonstrate at least three simultaneously running worker processes, abrupt worker loss with lease expiry, and recovery in the deployed topology. |
| 7. Concurrency | Contract and component-tested | Worker/sandbox/gateway/provider/tenant/workspace limits, admission, priorities, quotas, backpressure, queue metrics, HPAs, and deterministic load profiles exist. | No live 10/50/100-run campaign or production p50/p95/p99/resource evidence exists; HPA scale-up has not been observed on a target cluster. |
| 8. Context/memory/tasks | Integrated and locally live-verified | Composable contributors, route budgets, token estimation, non-destructive explicit and proactive compaction, durable history, project instructions, submission-bound referenced files, a non-mutating protected-path-filtered current diff, redacted and byte-bounded recent durable tool outcomes, provenance-bound memory, extraction jobs, versioned task plans, and control APIs exist. The worker atomically schedules compaction at 3,072 post-watermark messages, before the fail-closed 4,096-message load ceiling. | Repeat the context-bearing journey with promoted images and live model routes; tune route budgets only from recorded production evidence. |
| 9. Observability/reliability | Component-tested | Structured errors/logs, secret redaction, OpenTelemetry spans, Prometheus metrics, Grafana configuration, health endpoints, and evidence schemas exist. | Target dashboards, alerts, trace export, retention, and SLO measurements have not been exercised under live failure campaigns. |
| 10. Kubernetes | Integrated contract | Separate control/event/scheduler/worker/LiteLLM workloads, node DaemonSet, Services, identities, exact secret keys, NetworkPolicies, PDBs, topology constraints, HPAs, metrics adapter, drain behavior, static validation, and a fail-closed production admission policy exist. | The production layer is deliberately not a site overlay. Real image digests, external secrets, OIDC, TLS, managed-service endpoints/CIDRs, ingress/DNS, dedicated nodes, and cluster admission activation remain external. |
| 11. Evaluation/load/chaos | Component-tested | Thirty meaningful typed coding fixtures, immutable verifier contracts, Podman evidence models, deterministic deployment/load/chaos drivers, provenance-aware reports, and final evidence compilation exist. | The checked-in reports are simulations. Live coding, load, chaos, rolling-deployment, and HPA campaigns with hardware/methodology metadata remain to be run. |

## Capability boundary

| Capability | Current evidence |
|---|---|
| Deterministic coding-agent orchestration | Integrated and extensively unit/integration tested. The platform loop, not SDK `Runner`, owns turns, tools, limits, retries, redaction, events, and duplicate suppression. |
| Real coding happy path | Live-verified locally through LiteLLM, the Agents SDK adapter, Git worktree tools, and a rootless-Podman command sandbox. |
| Distributed production-boundary path | Live-verified locally with real PostgreSQL, MinIO, TLS 1.3 mTLS node transport, rootless Podman, project instructions, explicit file references, current-diff context, two approvals, branch-scoped checkpoints, five sequential worker instances, restore, ordered replay, and distinct epoch patches. The model is deterministic and provider-neutral. |
| Durable recovery after ordinary suspension/loss | Integrated. Checkpoint messages are combined with validated journal suffixes, terminal outcomes are replayed, and worker capacity is released while suspended. |
| Arbitrary rewind | Live-verified locally. Rewind atomically seeds a new epoch from the selected prefix, restores the pre-tool workspace snapshot, preserves abandoned rows and artifacts for audit, fences stale workers, and produces a distinct active replacement patch. Target-cluster evidence remains outstanding. |
| Security enforcement | Strong component and local runtime evidence. The production admission policy is portable and namespace-label gated, but it has not been activated against a rendered site overlay. |
| Production scale and SLOs | Not proven. Only deterministic simulations exist, so no concurrency, latency, availability, recovery-time, success-rate, or cost-reduction claim is justified yet. |

## Confirmed production gaps

### P0 — close before production traffic

1. **Create and validate a real production overlay.**
   - Pin every platform, node, LiteLLM, and sandbox image by digest.
   - Supply production OIDC/JWKS, separate workload identities, generated/rotated mTLS material,
     external Secrets, managed PostgreSQL/Redis/object storage, KMS configuration, ingress, DNS,
     dedicated sandbox nodes, and exact egress CIDRs.
   - Render and validate the full graph, then label the namespace only after the fail-closed admission
     policy is installed and tested with positive and negative admission cases.

2. **Execute and enforce the release supply-chain controls.**
   - The repository now implements the Podman-native build/mirror, SPDX, fail-closed Grype,
     keyless Cosign image/attestation, signed-manifest, and independent promotion-verification
     pipeline. Third-party actions are commit-pinned and contract tests reject mutable references,
     suppressed findings, changed evidence, untrusted binaries, and unsigned predicate drift.
   - Configure the protected runner's exact executable paths/checksums, digest-pinned Python and
     LiteLLM materials, registry credentials, and OIDC identity. Run it for the release tag and
     retain the signed evidence with the source revision, lockfile, scanner policies, image
     digests, SBOMs, scans, provenance, and verification output.
   - Add a site Sigstore admission policy that requires the exact workflow identity plus the SPDX
     and SLSA attestations. Exercise positive and negative admission before production activation;
     the portable admission layer currently enforces digest and workload security, not signatures.

3. **Prove the deployed distributed and gateway topology.**
   - Run three or more workers concurrently, submit multiple isolated tenants, kill active workers,
     wait for real lease expiry, and prove no accepted run or committed mutation is lost/duplicated.
   - Exercise WebSocket reconnect through the target ingress/load balancer while replacing API pods,
     Redis loss with PostgreSQL polling, primary-provider loss with a compatible secondary, node
     loss, sandbox OOM, checkpoint interruption, and rolling deployment. Direct uvicorn/TCP replay,
     disconnect, restart, isolation, limit, and sequence-gap behavior is covered locally.
   - Exercise two real provider deployments through LiteLLM without placing provider credentials in
     workers, and reconcile gateway usage/cost with provider records.

4. **Configure and rehearse disaster recovery and rotation in the target environment.**
   - The repository now implements retention/garbage-collection jobs, legal holds, export-first
     tenant deletion, audit export verification, fail-closed tenant access, a fenced object-deletion
     outbox, an operator CLI, and a read-only PostgreSQL/object restore verifier. The verifier passed
     locally against a freshly migrated isolated PostgreSQL database and rejected altered object
     evidence and the wrong migration head.
   - Configure managed PostgreSQL PITR, object-store versioning/replication/retention, KMS ownership,
     and dual-trust key/certificate rotation under the site runbook. Rehearse a coordinated restore
     into an isolated target environment and retain proof of database metadata, every referenced
     object checksum, run/event state, tombstones, and tenant authorization. Publish RPO/RTO only
     from that timed evidence.

### P1 — required for a complete first release

1. Validate production upstream provider-attempt metadata. Continue leaving provider identity unset
   rather than fabricating it when LiteLLM cannot supply reliable provenance.
2. Raise branch-coverage headroom above the 85% minimum by targeting production composition,
   scheduler/worker lifecycle failures, node transport, Podman termination, and PostgreSQL conflicts;
   do not lower the gate or exclude core safety behavior.
3. Turn node, gateway, database, object-store, queue, and certificate health into operator runbooks
   with alert thresholds, escalation, controlled degradation, and retry/repair procedures.

### P2 — evidence and longer-term hardening

1. Run the 30-task corpus in live-model mode and publish immutable task/fixture/verifier digests,
   patch/test outcomes, iterations, tool calls, token/cost, approvals, latency, and failures.
2. Run reproducible 10/50/100-run load profiles and report throughput, queue wait, first token,
   p50/p95/p99 latency, memory/CPU, error rate, gateway saturation, and sandbox saturation.
3. Establish SLO dashboards and alerts only after live baselines exist. The design's availability,
   recovery, and success numbers remain targets, not claims.
4. Evaluate gVisor or Firecracker for hostile multi-tenant code. Rootless Podman materially reduces
   impact but is not a formal untrusted-code isolation guarantee.
5. Add warm pools only if live startup data demonstrates a material benefit and the reuse boundary
   can preserve workspace, secret, process, and network isolation.

## Recommended closure plan

### Wave 1 — correctness and branch semantics

The branch epoch migration, branch-aware rewind, stale-worker fencing, real-PostgreSQL integration
coverage, and complete local distributed fork journey are implemented. The E2E retains and checks
the first patch, rewinds to its pre-tool snapshot, applies a different replacement mutation in a
new epoch, and verifies active-artifact selection. Repeat it with promoted images in the target
cluster as part of Wave 4.

### Wave 2 — immutable release and site composition

The image inventory, scanner policy, signed evidence pipeline, and promotion verifier are
implemented. Configure and execute them on the protected runner, then build the site overlay with
exact secrets, identities, verified manifest digests, and Sigstore-aware admission activation. A
clean environment must apply migrations and deploy from only the promoted artifacts.

### Wave 3 — recovery and lifecycle operations

Retention and deletion jobs, legal holds, audit export, tenant access denial, restore verification,
and the certificate/credential/migration runbook are implemented. Configure provider-managed
database/object backups and rotations, then execute the runbook against an isolated target restore.
Treat checksum, migration, event, audit, or tenant-reference divergence as a failed restore and
retain the create-once verification report.

### Wave 4 — live acceptance campaign

Run the concurrent-worker, provider-failover, API reconnect, Redis/database/object/node loss,
sandbox limit, rolling deployment, HPA, load, chaos, and live coding campaigns. Record hardware,
cluster/runtime versions, configuration hashes, source revision, image digests, start/end times,
and raw sanitized observations.

### Wave 5 — product completeness and SLO sign-off

Add dashboards, alerts, and operator runbooks.
Exercise the implemented CLI against promoted dependencies, compile the immutable reports, and
hold a go/no-go review. Only then promote the maturity
of measured capabilities to `production-proven` or publish résumé performance numbers.

## Verification policy

Fresh local evidence for this review:

- `make release-check`: passed;
- Ruff formatting and lint: passed;
- strict mypy: passed for 220 source files;
- unit tests: 855 passed;
- branch coverage: 85.47% against an 85% gate;
- integration tests: 5 passed, including 2 real uvicorn/TCP event-gateway cases;
- pre-commit: passed;
- frozen dependency audit: no known vulnerabilities;
- lock verification: passed;
- package build: all 14 workspace packages produced an sdist and wheel;
- lifecycle persistence adapter: 93% branch coverage from deterministic repository tests;
- real-PostgreSQL security and persistence suite: 18 passed, including lifecycle and an isolated
  restore verification;
- focused real-PostgreSQL context/task/memory recomposition: 1 passed, including immutable
  submission reference recovery;
- distributed production-boundary E2E: 1 passed locally in 21.70 seconds with real PostgreSQL,
  MinIO, TLS 1.3 mTLS node transport, rootless Podman, and every production workspace-context
  source asserted in recorded model requests.

The local E2E result is functional evidence, not a latency target or production benchmark.

The release candidate should pass, from a clean checkout:

```text
make release-check
make kubernetes-contract
make postgres-security
make sandbox-security
make gateway-security
make e2e-happy
make distributed-e2e
```

It must also pass the rendered site-overlay/admission checks and the live campaigns above. Pure
process entrypoints and dependency-construction roots are excluded from line-coverage measurement;
their environment contracts and factory selection are checked by static composition tests. Core
domain, safety, persistence, and adapter behavior remains inside the branch-coverage gate.
