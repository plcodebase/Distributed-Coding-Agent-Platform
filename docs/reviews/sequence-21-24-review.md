# Sequences 21–24 Review and Hardening Plan

**Status:** Implemented and verified
**Review date:** 2026-07-31
**Scope:** PRs 21–24 from `DESIGN.md`; PRs 25–32 remain deferred

## Review verdict

The initial Sequences 21–24 baseline implemented the intended architecture but was not
merge-ready because several lifecycle and resource-bound claims were stronger than the
code. This hardening pass closes the confirmed P0/P1 gaps and is merge-ready for the
PR 21–24 scope, subject to the explicitly deferred later-sequence work below.

| Sequence | Area | Review result |
|---|---|---|
| 21 | Local worker/sandbox and distributed gateway capacity | Partial: limits exist, but a live request can outlast its capacity lease |
| 21 | Tenant active/queued quotas and provider token windows | Pass, with durable configuration-drift hardening needed |
| 22 | Priority classes, aging, admission, and overload responses | Pass, with configuration reconciliation needed |
| 22 | Worker backpressure and queue observations | Pass; metrics export and load evidence remain later PRs |
| 23 | Eight contributors, route budgets, and compression | Partial: resolved tasks are incorrectly critical |
| 23 | Durable non-destructive compaction | Partial: concurrent pending requests and route provenance are ambiguous |
| 24 | Versioned task plans and tenant-safe APIs | Pass, including legacy read compatibility |
| 24 | Memory provenance, extraction, and recovery | Partial: premature enqueue and source materialization violate the claims |

Current baseline evidence before this hardening pass:

- Ruff formatting/lint and strict mypy pass.
- 512 unit tests and 3 integration tests pass with 85.52% total coverage.
- 13 real PostgreSQL acceptance/security tests pass under rootless Podman.
- Alembic upgrade/downgrade/check, dependency audit, frozen lock verification, and all
  workspace package builds pass.

## Implementation outcome

- Live gateway requests renew exact distributed capacity ownership; renewal loss fails
  closed and the latest lease is released.
- Provider-route and global-admission settings reconcile current validated platform
  configuration under their existing durable locks.
- Task context is itemized: unresolved and unknown states are critical, while completed
  and cancelled history is compressible. Legacy material is byte-chunked.
- Compaction creation locks the session, permits one pending request, has a partial
  unique database index, and records the actual `summarization` route.
- Memory extraction is enqueued only by atomic durable run completion. Source messages
  stream in bounded batches, and extraction has a validated deadline shorter than its
  fenced lease.
- Final verification: 521 unit tests and 3 integration tests pass at 85%+ coverage;
  Ruff, strict mypy, pre-commit, dependency audit, frozen lock verification, all ten
  package builds, and 13 real PostgreSQL tests under rootless Podman pass.

## Step-by-step review

### Sequence 21 — bounded concurrency and tenant quotas

1. **Provider-neutral contracts — pass.** `agent-core` owns closed quota, rejection,
   lease, claim, queue-depth, and queue-snapshot models without provider imports.
2. **Independent local limits — pass.** Worker run slots, sandbox slots, and gateway
   request slots are separate bounded mechanisms. Cancellation releases acquired slots.
3. **Distributed capacity admission — partial.** The gateway claims tenant/route/token
   capacity after validation and idempotency claim and releases it on normal, failure,
   cancellation, and stream-close paths.
4. **Lease liveness — fail.** `GatewayCapacityLease` expires, but `GatewayClient` never
   renews it. A long or slowly consumed live stream can therefore lose its durable slot,
   after which another process can acquire the same nominal capacity while the original
   provider call still runs.
5. **Atomic PostgreSQL enforcement — pass.** Tenant quota and route rows serialize
   tenant slots, provider-route slots, and conservative token reservations. Expired
   leases recover request slots without refunding tokens in the active window.
6. **Configuration lifecycle — partial.** Tenant quota defaults correctly become durable
   overrides, but queue-admission and provider-route configuration is inserted only once.
   A later validated deployment configuration silently leaves old limits active.
7. **Evidence gap.** Existing tests cover acquisition, rejection, expiry, cancellation,
   and reconciliation, but do not prove renewal of live work or renewal-failure shutdown.

### Sequence 22 — backpressure and priority scheduling

1. **Closed priority classes — pass.** Runs persist `interactive`, `background`, or
   `evaluation`, plus a bounded integer adjustment included in the idempotency hash.
2. **Deterministic ordering — pass.** Claims order by class rank plus bounded aging,
   integer priority, creation time, and run ID.
3. **Tenant fairness — pass.** A candidate from a tenant at its active quota is skipped
   so another tenant can be considered, with the scan bounded at 1,000 candidates.
4. **Atomic admission — pass.** One locked singleton serializes global admission;
   tenant/global counts include queued, approval-wait, retry-wait, and lost states.
   Idempotent replay is checked before thresholds.
5. **Backpressure — pass.** Full workers do not claim. Sandbox phases are separately
   bounded, and capacity errors are structured and retryable.
6. **Queue observations — pass.** Class depth and oldest accepted-run age are typed and
   bounded for the future metrics sequence.
7. **Configuration drift — partial.** The singleton stores limits but is never reconciled
   with later injected settings, making restart configuration changes ineffective.
8. **Intentional deferral.** Queue exporters, warm-pool behavior, and reproducible
   10/50/100-run reports belong to PRs 25, 27, and 30.

### Sequence 23 — context pipeline and compact

1. **Contributor interface and sources — pass.** All eight required contributors use
   provider-neutral bounded inputs and typed fragments.
2. **Budgeting — pass.** A conservative serialized UTF-8 estimator and explicit
   per-route input/output reservations fail closed for unknown routes.
3. **Critical conversation preservation — pass.** Recent messages, assistant/tool pairs,
   active referenced files, system/project instructions, and recent errors are critical.
4. **Task preservation — fail.** `ActiveTaskPlanContributor` marks the entire plan
   critical. Hundreds of completed/cancelled tasks can therefore cause
   `context_critical_limit`, although only unresolved items must be preserved.
5. **Compression boundary — pass.** The normalized gateway uses `summarization`, rejects
   tool calls, bounds input/output, redacts secrets, closes streams, and returns opaque
   provider failures.
6. **Durability — pass.** Compaction rows retain source watermarks and usage without
   updating or deleting source messages.
7. **Pending-request concurrency — fail.** Different idempotency keys can create multiple
   pending compactions for one session. The worker processes only the oldest, allowing
   unbounded pending work and ambiguous user status.
8. **Route provenance — partial.** The API records the session coding route even though
   the actual compression call uses the `summarization` route.
9. **Composition boundary — pass with a limitation.** Worker execution accepts an
   injected durable context builder. Deployment-specific workspace data loading remains
   in the trusted factory selected by `--factory`.

### Sequence 24 — long-term memory and task tracking

1. **Task contracts — pass.** Stable IDs, explicit statuses, bounded fields, referential
   dependencies, cycle rejection, and closed schemas are enforced.
2. **Task persistence — pass.** Run-row locking provides compare-and-set versions;
   legacy arbitrary plans remain readable without rewriting history.
3. **Memory contracts — pass.** Tenant/session/run provenance, kind, bounded content,
   content hash, immutable metadata, extraction/archive order, and finite JSON are
   validated in both domain and SQL schemas.
4. **Enablement — pass.** Tenant and session policy is checked at enqueue/claim,
   completion, retrieval, and context contribution.
5. **Atomic enqueue — fail.** PostgreSQL `RunQueue.finish()` correctly inserts the job in
   the run-completion transaction, but `AgentLoopRunExecutor` also enqueues when it sees
   an in-memory `run.completed` event. A crash before durable `finish()` can therefore
   create memory work for a run that is not durably complete.
6. **Extraction queue fencing — pass.** Worker ID, random token, generation, expiry,
   attempt, stale-owner rejection, deterministic recovery ordering, and terminal cleanup
   are enforced.
7. **Extraction lease duration — partial.** The scheduler has a five-minute lease but no
   bounded extraction deadline or heartbeat. A stuck provider can outlive the lease.
8. **Source resource bound — fail.** `source_for_job()` uses `scalars()` for up to 2,000
   messages before applying the 4 MiB text ceiling. Large message rows can cause memory
   amplification far beyond the advertised extraction limit.
9. **Deduplication and archive — pass.** Completion uses one transaction and a durable
   tenant/session/kind/content-hash identity; archive is tenant/session scoped.
10. **Control APIs — pass.** Session status, compact/status, task display/update, memory
    toggle/list/archive, new session, and rewind are authenticated and tenant filtered.

## Confirmed gaps at review time (closed by this hardening pass)

### P0

- A live gateway call can outlast its distributed capacity lease and overlap a newly
  admitted request.
- Memory extraction can be scheduled from an event before the run is durably completed.
- Memory source rows are materialized before the extraction byte bound is enforced.

### P1

- Queue-admission and provider-route limits do not reconcile validated configuration on
  restart.
- Multiple pending compactions per session are possible under concurrent distinct keys.
- Compaction records advertise the coding route instead of the summarization route.
- Completed/cancelled task items consume critical context budget.
- Memory extraction has no deadline shorter than its lease.

### P2 / explicit limitations

- Worker and scheduler processes intentionally load deployment-specific factories; the
  repository does not embed credentials or one universal production composition root.
- Metrics exporters, dashboards, stored load reports, chaos evidence, Kubernetes, HPA,
  warm pools, and coding-task evaluation remain PRs 25–31.
- A bounded 1,000-candidate scheduling scan can defer a runnable item behind more than
  1,000 temporarily ineligible candidates; it prevents unbounded database work.

## Implementation plan

### 1. Renew live gateway-capacity ownership

- Add `renew()` to the provider-neutral `GatewayCapacityStore` protocol.
- Implement exact-identity, unexpired renewal in the in-memory and PostgreSQL stores.
- Add a validated heartbeat interval shorter than the lease duration.
- Run a heartbeat for the complete normalized stream lifetime, including slow consumer
  intervals. Renewal failure cancels provider work, closes the stream, releases local
  capacity, and returns an opaque retryable capacity error.
- Release the latest renewed lease, not the original lease.

### 2. Reconcile platform-owned capacity/admission configuration

- After locking the global admission singleton, update its limit/retry values from the
  current validated `QueueAdmissionPolicy` and advance `updated_at` monotonically.
- After locking a route-capacity row, reconcile request, token, and window limits from
  current gateway configuration. Durable tenant quota rows remain explicit overrides.
- Add tests proving restart configuration takes effect and clock rollback fails closed.

### 3. Correct context preservation and compaction serialization

- Split typed task plans into per-item fragments. Pending, in-progress, blocked, and
  unknown task states are critical; completed/cancelled items are compressible.
- Chunk bounded legacy task material so one historical plan cannot create an oversized
  fragment.
- Lock the session during compaction request creation and permit only one pending row.
- Add a partial unique PostgreSQL index as a defense against adapter regressions.
- Record `summarization` as the compaction route used by the model operation.

### 4. Make memory completion and extraction claims truthful

- Remove event-time memory enqueue from `AgentLoopRunExecutor` and remove the redundant
  repository API. `PostgresRunQueue.finish()` remains the sole atomic enqueue point.
- Stream extraction messages with bounded server-side batches and stop once the 4 MiB
  retained-source ceiling is reached.
- Add a validated extraction timeout strictly shorter than the extraction lease. Timeout
  cancellation produces an opaque fenced terminal error while the lease is still owned.
- Preserve generation/token fencing and content-hash deduplication.

### 5. Verification

- Add deterministic unit tests for capacity renew/release/failure, configuration
  reconciliation, task criticality, compaction concurrency, atomic enqueue ownership,
  streamed source bounds, and extraction timeout.
- Add real PostgreSQL acceptance for capacity renewal, the single-pending index,
  streamed memory source, and stale memory-job fencing.
- Run `make test`, all pre-commit hooks, dependency audit, frozen lock verification,
  all package builds, and `git diff --check`.
- Run PostgreSQL acceptance through rootless Podman only; invoke no other container
  runtime.

## Assumptions

- Sequences 21–24 mean PRs 21–24, not all of Phases 7–8 or later observability work.
- Model-route capacity remains keyed by the LiteLLM route because the final fallback
  provider is not known at admission time.
- Tenant quota records are deliberate durable overrides; global admission and route
  capacity values are platform configuration and therefore reconcile on restart.
- One pending explicit compaction per session is the simplest bounded semantic.
- No persisted production data exists for migrations 0003–0006, so the uncommitted
  revisions can be corrected directly without a follow-up migration.
