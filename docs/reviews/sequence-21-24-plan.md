# Sequences 21–24 Implementation Plan

**Status:** Implemented and verified
**Source of truth:** `DESIGN.md`, Phase 7 and Phase 8, and PRs 21–24
**Implementation date:** 2026-07-31

## Current-state audit

Sequences 17–20 already provide a PostgreSQL `SKIP LOCKED` run queue, bounded worker
agent-run slots, fenced run leases, recovery, idempotent tool replay, and one writer
lease per tenant workspace. Sequence 13 already provides tenant-and-route request-rate
limits, but not concurrent gateway/provider slots or provider token budgets.

The following PR 21–24 capabilities do not yet exist:

- atomic queue admission and configurable per-tenant active/queued-run quotas;
- separate process-local sandbox and gateway concurrency limits;
- distributed gateway/provider request slots and provider token-window accounting;
- named interactive, background, and evaluation priorities;
- queue thresholds, queue-age snapshots, and structured overload responses;
- a composable `ContextContributor` pipeline, route token budgets, or gateway-backed
  compaction;
- durable compaction requests/results, long-term memories, memory extraction jobs, or
  provenance;
- a closed task-plan model, compare-and-set task updates, task-plan/status APIs, or
  tenant/session memory controls.

Observability exporters, dashboards, reproducible load reports, chaos tests, Kubernetes,
and horizontal autoscaling remain PRs 25–30. PRs 21–24 will expose bounded typed queue
and capacity snapshots that those later sequences can instrument; they will not publish
unmeasured throughput or latency claims.

## Sequence 21 — bounded concurrency and tenant quotas

1. Add provider-neutral quota, admission, capacity-lease, and capacity-snapshot models
   and protocols to `agent-core`.
2. Preserve the existing worker agent-run slots and workspace writer lease. Add a shared,
   cancellation-safe async capacity limiter and a sandbox adapter wrapper so worker
   sandbox slots are independently configurable from run slots.
3. Add a gateway-capacity protocol beneath `GatewayClient`. Acquire capacity only after
   idempotency claim and request validation, before provider contact; release it on every
   success, failure, cancellation, and consumer-close path.
4. Add durable PostgreSQL quota and gateway-capacity records. Use row locks and expiring
   request leases as the global coordination authority; do not add a second correctness
   authority in Redis. Atomically enforce:
   - tenant active-run limits during queue claim;
   - tenant queued-run limits during admission;
   - tenant gateway request slots;
   - route/provider request slots;
   - route/provider token windows with conservative pre-request reservations and
     terminal-usage reconciliation.
5. Make quota defaults validated, bounded, dependency-injected settings. Support durable
   per-tenant overrides without trusting client-supplied limits.
6. Test cancellation, lease expiry, reconciliation, concurrent claims, quota isolation,
   and construction limits with deterministic fakes and real PostgreSQL acceptance.

## Sequence 22 — backpressure and priority scheduling

1. Replace the unnamed scheduling contract with a closed priority class:
   `interactive`, `background`, or `evaluation`, plus the existing bounded integer
   adjustment for ordering within a class.
2. Persist the priority class and include it in the run-creation idempotency hash.
3. Order claims by class, bounded aging, priority adjustment, creation time, and run ID.
   Aging prevents indefinite starvation while retaining predictable class preference.
4. Add a globally serialized queue-admission row and enforce a configured global queued
   threshold atomically with new run insertion. Idempotent replays are resolved before
   admission and remain successful when the queue is currently full.
5. Keep workers from claiming when their local run capacity is full; report sandbox and
   gateway exhaustion as bounded retryable errors instead of creating unbounded tasks.
6. Add typed queue snapshots containing total/class depth and oldest accepted-run age.
   PR 25 will export these as metrics.
7. Return structured `queue_overloaded`, `tenant_queue_quota_exceeded`, and capacity
   errors with bounded retry-after metadata and HTTP `429`/`503` behavior.
8. Test exact thresholds, concurrent admission, priority order, aging, tenant fairness,
   idempotent overload replay, and full-worker backpressure.

## Sequence 23 — context pipeline and compact

1. Add a provider-neutral `ContextContributor` protocol and closed context fragment,
   request, budget, build-result, and compaction models.
2. Implement contributors for system instructions, project instructions, durable
   conversation history, referenced files, active task plan, recent tool outcomes,
   long-term memory, and current Git diff. Contributors receive bounded typed inputs and
   may not fetch providers or infrastructure directly.
3. Use a conservative UTF-8 token estimator and route-specific token budgets. Assemble
   deterministic leading system context followed by valid conversation messages.
4. Preserve critical state during pressure: recent messages, explicitly active files,
   unresolved tasks, and recent errors. Fail with `context_critical_limit` when critical
   state alone cannot fit instead of silently dropping it.
5. Add a gateway-backed compressor that uses the `summarization` route through the
   existing `ModelGateway`, validates one terminal stream, rejects tool calls, bounds the
   summary, and never exposes raw provider failures.
6. Persist compaction requests and immutable completed summaries with source sequence,
   route, token counts, and timestamps. Never update or delete original message rows.
7. Integrate an optional context builder into worker execution so automatic or requested
   compaction occurs in the execution plane before `AgentLoop` input is constructed.
8. Add `POST /v1/sessions/{session_id}/compact` as a durable accepted operation. The next
   eligible worker context build owns model compression; API processes never call a
   model provider.
9. Test all contributors, deterministic ordering, multibyte accounting, forced and
   automatic compaction, critical-item survival, oversized critical state, durable
   transcript preservation, and worker integration with a fake gateway.

## Sequence 24 — long-term memory and task tracking

1. Add closed task-plan and task-item models with stable IDs, explicit statuses,
   dependency validation, bounded descriptions, and monotonically increasing versions.
2. Add tenant-safe compare-and-set task-plan persistence and APIs to display and update
   the active plan. Conflicting versions fail without overwriting another writer.
3. Add immutable memory models with kind, bounded content, content hash, source tenant,
   source session, source run, extraction time, and optional metadata.
4. Add tenant and session memory switches. Effective memory requires both to be enabled;
   disabled memory is neither extracted nor contributed to model context.
5. Atomically enqueue an idempotent memory-extraction job when a run completes. Add a
   fenced, expiring job queue so scheduler workers can recover abandoned extraction.
6. Implement a gateway-backed memory extractor using the `summarization` route and a
   strict finite JSON response schema. Persist deduplicated memories and provenance in
   the same transaction that completes the job.
7. Extend the scheduler with an optional bounded memory-job processor while preserving
   lease-recovery behavior when none is configured.
8. Add tenant-scoped APIs for session status, task-plan display/update, memory
   enable/disable, memory list/delete, and compaction status. Existing new-session and
   rewind APIs remain the canonical operations for those commands.
9. Test restart persistence, CAS conflicts, dependency cycles, extraction replay,
   abandoned-job recovery, provenance, tenant isolation, disabled memory, context
   contribution, and API authorization.

## Migration and compatibility strategy

- Add four ordered Alembic revisions (`0003` through `0006`) matching the four sequence
  boundaries and update SQLAlchemy head metadata with the same checks and indexes.
- Backfill existing runs as `interactive`; preserve their integer priority as the
  within-class adjustment.
- Keep existing run, event, tool, checkpoint, and gateway event names intact.
- Keep existing arbitrary historical task-plan JSON readable. New API writes use the
  closed task-plan contract and new version rows; no persisted data is rewritten.
- Preserve all uncommitted Sequence 17–20 hardening in the current worktree.
- Add no provider imports to `agent-core`, worker, scheduler, sandbox, or telemetry.
- Add no container-runtime dependency or invocation to implementation tests. If final
  PostgreSQL acceptance requires local infrastructure, use rootless Podman only.

## Verification gates

After each meaningful sequence:

1. run focused unit tests and strict Ruff/mypy checks for changed modules;
2. run persistence contract and migration tests;
3. run API, worker, scheduler, gateway, context, memory, and task integration tests;
4. run the full `make test` gate;
5. run pre-commit, dependency audit, frozen lock verification, and all package builds;
6. run real PostgreSQL security/acceptance tests through rootless Podman if the local
   service is available;
7. inspect `git diff --check`, complete diff, and status, including all previously
   untracked Sequence 17–20 files.

## Assumptions and explicit deferrals

- PostgreSQL is already the durable coordination authority, so PRs 21–24 use its atomic
  row locks and expiring leases instead of introducing Redis as a second source of
  truth. Redis remains available for later caching or best-effort acceleration.
- A route is the enforceable provider-capacity key at admission time because the actual
  fallback provider may not be known until the terminal gateway event. LiteLLM retains
  provider fallback; route budgets fail closed before provider contact.
- Context compression is asynchronous execution work. The compact API records a durable
  request and returns acceptance; a worker performs it on the next context build.
- Optional sandbox warm pools are not enabled by default because they increase retained
  untrusted state. The new sandbox capacity boundary permits a later pool without
  weakening isolation.
- Queue-age exporters and latency histograms are PR 25; dashboards are PR 26; 10/50/100
  reproducible load reports are PR 27; chaos reports are PR 28; Kubernetes begins at
  PR 29. PRs 21–24 add the tested mechanisms and typed observations those later PRs use.
- No Docker tooling, API, image, or runtime will be invoked. Podman is the only permitted
  container runtime for optional infrastructure verification.
