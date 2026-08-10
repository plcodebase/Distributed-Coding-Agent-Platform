# Sequences 17–20 review and hardening plan

- Status: Implemented and verified for the current homogeneous Podman deployment
- Date: 2026-07-30
- Scope: PR 17 through PR 20 only

## Review verdict

Sequences 17–20 establish the intended distributed-execution baseline: PostgreSQL is
the durable queue, workers execute outside FastAPI, run and workspace ownership use
tokens plus monotonic generations, expired work is requeued, and terminal tool/event
replay is idempotent.

The initial baseline was not merge-ready. The database claim transaction was fenced,
but the fence was not carried through all worker-owned writes or workspace operations.
A stale worker could append events and advance tool state after reassignment.
Cancellation and heartbeat failure were also observed only after recovery and
restoration finished.

This hardening pass closes those P0 paths and the directly implementable P1 recovery and
writer gaps. Merge readiness is conditional on the documented homogeneous rootless
Podman assumption. Heterogeneous capability routing, database-authoritative queue
clocks, explicit checkpoint/tool ordering, and a process-kill test with a production
snapshot materializer remain concrete follow-up work rather than completed claims.

| Sequence | Area | Initial review result |
|---|---|---|
| 17 | PostgreSQL task queue and atomic claim | Pass at claim/finish; downstream writes are not fenced |
| 18 | Worker leases, heartbeats, cancellation, draining | Partial; recovery/restore are outside heartbeat supervision |
| 19 | Recovery and idempotent replay | Partial; replay identity and byte bounds are incomplete |
| 20 | Workspace writer leases | Partial; ownership is not passed to mutation boundaries and stale release can fail open |

## Step-by-step review

### Sequence 17 — PostgreSQL task queue

Implemented:

1. A committed `QUEUED` run is the queue record; there is no ambiguous second enqueue.
2. Claim locks an active worker registration and checks durable capacity.
3. Eligible runs are ordered by priority, age, and stable run ID.
4. `FOR UPDATE SKIP LOCKED` allows concurrent workers to claim different runs.
5. Run ownership, writer ownership, worker capacity, and `QUEUED -> LEASED` commit in
   one transaction.
6. Run leases use a random token and monotonic run generation.
7. Normal finish and scheduler recovery return capacity and clear ownership
   transactionally.
8. API code does not execute the agent loop or import worker/scheduler implementations.

Confirmed gaps:

- **P0 — stale-worker event writes are not fenced.** The worker event-store call carries
  tenant/run IDs and a delivery key, but not the active `RunLease`. The PostgreSQL event
  transaction locks the run row without validating the current worker, token,
  generation, expiry, or running state. Worker A can therefore append after Worker B
  owns the run.
- **P0 — stale-worker tool-state writes are not fenced.** `save_tool_call()` has the
  same omission. Monotonic tool state prevents some overwrites but does not establish
  which lease owner is authorized to create or advance the record.
- **P1 — lease authority trusts caller clocks.** Claim, renewal, finish, and recovery
  accept worker/scheduler timestamps as the lease authority. Backward timestamps are
  rejected in selected paths, but clock skew can still delay or accelerate expiry.
  Database time should become the production authority while deterministic tests use
  an injected database-time boundary.
- **P1 — sandbox capabilities are informational only.** Workers advertise supported
  sandbox types, but runs have no durable sandbox requirement and claim does not match
  capabilities. This is safe only while every production run uses the same mandatory
  Podman sandbox composition.
- **P2 — queue contention has a fixed 32-candidate bound.** This is deterministic and
  documented, but no metric distinguishes an empty queue from transient workspace-row
  lock saturation.

Required improvements:

1. Add worker-only, active-lease-fenced event and tool persistence operations.
2. Validate tenant, run, worker, token, generation, unexpired lease, mirrored run
   ownership, and `LEASED`/`RUNNING` state in the same transaction as each write.
3. Reject a stale owner before returning an idempotent prior delivery; idempotency may
   not bypass authorization.
4. Retain unfenced event append and control-plane repositories only for explicitly
   non-worker producers.
5. Plan a schema-backed run sandbox requirement and database-time lease authority
   before heterogeneous worker pools are enabled.

### Sequence 18 — worker leases, heartbeats, cancellation, and draining

Implemented:

1. Worker registration persists ID, sandbox capabilities, slots, lifecycle status, and
   heartbeat time.
2. `WorkerService` registers before polling and observes its local slot ceiling.
3. Run and writer leases are renewed on a bounded cadence less than half the lease.
4. Cancellation visible at `start()` finishes without creating an execution context.
5. Cancellation during active execution invokes the executor cancellation boundary.
6. Draining is durable before the worker stops claiming; active attempts continue to
   heartbeat.
7. Background task failures are retained and surfaced to the service loop.
8. The local fleet launcher defaults to three independent spawned worker processes.

Confirmed gaps:

- **P0 — recovery and restoration are outside heartbeat supervision.** The heartbeat
  task is started before recovery, but the service races it only against
  `executor.execute()`. A failed heartbeat or expired lease does not cancel a blocked
  recovery load or workspace restore.
- **P0 — cancellation can be lost before executor activation.** The heartbeat marks a
  cancellation as sent even when `executor.cancel()` finds no active task during
  recovery/restore. Execution then starts and may mutate the workspace despite the
  durable cancellation flag.
- **P0 — writer ownership is not passed to workspace operations.** `WorkspaceRestorer`
  and `RunExecutor` receive only the run lease. Composition cannot require the exact
  writer token/generation at the restoration, tool, or sandbox boundary.
- **P1 — acquired writer identity is only null-checked.** A malformed adapter result
  with another run, workspace, worker, or run token could enter restoration.
- **P2 — graceful drain has no termination deadline.** It can wait indefinitely for a
  non-cooperative dependency. Kubernetes termination-grace policy belongs to the
  deployment sequences, but the runtime needs a bounded forced-cancellation phase
  before production rollout.

Required improvements:

1. Race recovery loading, workspace restoration, and loop execution independently
   against the same heartbeat task.
2. Treat durable cancellation as a control signal that cancels the currently active
   phase, not a one-shot executor notification.
3. Commit `CANCELLED` only while the run fence is still owned; silently yield to a
   replacement owner after `run_lease_lost` or `run_lease_expired`.
4. Pass `WorkspaceWriterLease` through `WorkspaceRestorer.restore()`,
   `RunExecutor.execute()`, and the `AgentLoop` composition factory.
5. Validate the complete run/writer relationship before any recovery or workspace
   action.

### Sequence 19 — failure recovery and idempotent replay

Implemented:

1. The scheduler locks expired lease rows with `SKIP LOCKED` in bounded batches.
2. Matching work transitions `RUNNING/LEASED -> LOST -> QUEUED`; requeue increments
   `attempt`.
3. Cancellation discovered during recovery commits `CANCELLED`.
4. Recovery requires the replacement run lease and selects the durable checkpoint.
5. Message count and terminal-outcome count have hard limits.
6. Reserved message metadata and invalid normalized messages fail closed.
7. Tool persistence is monotonic and rejects argument/name/terminal divergence.
8. Stable attempt/generation/event delivery keys prevent duplicate event insertion.
9. Terminal tool results are injected into the loop so matching calls are not executed
   again.

Confirmed gaps:

- **P0 — recovered duplicate identity omits the tool name.** `DurableToolOutcome`
  includes `tool_name`, but conversion to the loop's `ToolOutcome` discards it.
  Reusing the same tool-call ID and identical arguments for a different tool can return
  the wrong durable outcome without execution.
- **P1 — recovery is count-bounded but not byte-bounded.** Up to 4,096 unbounded
  message rows and 100 unbounded JSON outcomes are materialized with `.all()`. A corrupt
  or externally written database can consume excessive worker memory before Pydantic
  validation or loop request limits run.
- **P1 — checkpoint conversation recovery is run-scoped instead of session-scoped.**
  Message sequence is session-global, but recovery filters messages by current run ID.
  A checkpoint at session sequence N can therefore omit earlier conversation from
  prior runs.
- **P1 — checkpoint/tool ordering uses wall-clock comparison.** Terminal outcomes are
  associated with a checkpoint through `completed_at >= checkpoint.created_at`.
  Equal timestamps and clock skew do not provide a durable total ordering. A future
  schema must associate a mutating tool outcome with its pre-tool checkpoint
  explicitly.
- **P1 — acceptance is process-simulated, not process-kill complete.** The real
  PostgreSQL test expires Worker A's lease and executes Worker B through the real worker
  service, but does not kill an OS worker process, materialize a real snapshot, or
  compare a real final Git patch. The repository also has no production worker
  composition factory or durable snapshot-object adapter.

Required improvements:

1. Retain and compare `tool_name` together with call ID and argument hash for every
   same-attempt and recovered outcome.
2. Add aggregate UTF-8 byte ceilings for recovery messages, plans/summaries, and tool
   outcomes; preflight in PostgreSQL before materializing large values and stream rows
   incrementally.
3. Recover checkpoint conversations by tenant/session sequence. Without a checkpoint,
   use the maximum message sequence owned by the current run as the session-history
   cutoff.
4. Add an explicit durable checkpoint association/order key to tool calls in the next
   compatible migration; do not claim timestamp ordering is total.
5. Add a subprocess-kill acceptance fixture with a real restorable Git/Podman
   workspace once the production snapshot composition root exists.

### Sequence 20 — workspace writer leases

Implemented:

1. One durable writer row is keyed by `(tenant_id, workspace_id)`.
2. Claim reserves it transactionally with the run lease and worker slot.
3. The fence includes tenant/workspace/run/worker identities, run token, writer token,
   and writer generation.
4. Writer renewal is capped at the owning run lease expiry.
5. Scheduler recovery and normal completion clear only ownership associated with the
   run lease.
6. Unowned rows retain generation history.
7. A composite foreign key proves that the run targets the writer's workspace.

Confirmed gaps:

- **P0 — the database fence stops at scheduling.** Because restoration/execution do not
  receive `WorkspaceWriterLease`, a composition root cannot bind mutations to the
  exact writer generation.
- **P1 — same-owner acquisition can return an expired lease.** The replay branch
  returns the existing writer unchanged without checking or renewing its expiry.
- **P1 — stale release can fail open.** Release returns successfully when the row is
  missing or currently unowned, even if its generation proves that the supplied lease
  is stale. This contradicts the ADR's fail-closed stale-release guarantee.
- **P1 — release checks only token and generation.** Active identity fields should also
  match tenant, workspace, run, worker, and owning run token before clearing.

Required improvements:

1. Propagate the exact writer lease to every workspace-mutating boundary.
2. On same-owner acquisition, revalidate the active run fence and renew the writer up
   to the run expiry before returning it.
3. Make exact repeated release idempotent only while the retained unowned row generation
   equals the released generation.
4. Reject missing rows, successor generations, or any active identity mismatch with
   `workspace_lease_lost`.

## Implementation plan

### Phase 1 — close persistence fencing gaps

- Add a shared PostgreSQL active-run-fence validator using database time.
- Add `append_idempotent_fenced(lease, delivery_key, draft)` to the worker event
  boundary.
- Add `save_tool_call_fenced(lease, tool_call)` to the worker tool boundary.
- Keep each fence check and write in one transaction and one lock order:
  run lease, run row, then event/tool row.
- Update the executor and in-memory fakes to pass and verify the exact lease.
- Test stale Worker A writes after Worker B reassignment, including an already-existing
  delivery key.

### Phase 2 — supervise every worker phase

- Generalize the execution/heartbeat race to any awaitable worker phase.
- Race recovery, restoration, and execution against heartbeat failure/cancellation.
- Convert heartbeat-observed cancellation into a typed worker control signal.
- Cancel the in-flight phase and executor, then finish `CANCELLED` only if still owned.
- Pass and validate `WorkspaceWriterLease` through restorer, executor, and loop factory.
- Test cancellation and lease loss during recovery, during restore, and during
  execution.

### Phase 3 — harden writer replay and tool identity

- Renew a matching same-owner writer acquisition and cap it at the active run expiry.
- Make release identity-complete and generation-aware while preserving safe exact
  repeated release.
- Add `tool_name` to the loop's duplicate outcome identity.
- Reject same ID/hash with a different tool before any handler executes.
- Add unit and real PostgreSQL stale-owner tests.

### Phase 4 — bound and correct recovery

- Add hard aggregate recovery byte constants shared with the domain contract.
- Preflight PostgreSQL message/tool JSON size before row materialization.
- Stream ordered rows with `yield_per=1`, stopping at count limits and closing cursors.
- Recover session history through an authoritative message-sequence cutoff.
- Validate the final serialized `RunRecoveryState` ceiling defensively.
- Add multibyte, oversized-row, prior-session-history, and cursor-cleanup tests.

### Phase 5 — documentation and acceptance evidence

- Correct ADR 0017's “every subsequent operation” claim to name the fenced methods.
- Correct ADR 0018/0019/0020 to document phase supervision, writer propagation,
  recovery byte limits, and the remaining checkpoint-order limitation.
- Add the process-kill/real-snapshot acceptance test after a production snapshot
  adapter and worker composition root exist; do not substitute an in-memory fake.
- Keep priority classes, tenant fairness, quotas, Kubernetes termination policy, and
  observability in Sequences 21–24.

## Test plan

### Queue and persistence fencing

- Worker A cannot append a new event after lease expiry/reassignment.
- Worker A cannot obtain an old idempotent event after losing authorization.
- Worker A cannot create or advance a tool row after reassignment.
- Worker B can replay identical event/tool writes.
- Token, generation, tenant, run, worker, expiry, mirrored owner, and run-state
  mismatches fail closed.

### Worker lifecycle

- Heartbeat failure cancels blocked recovery before restoration/execution.
- Heartbeat failure cancels blocked restoration before execution.
- Distributed cancellation during recovery, restoration, and execution commits
  `CANCELLED` without starting later phases.
- Lease loss never lets the stale worker finish or release a successor.
- Exact writer lease identity reaches the restorer, executor, and loop factory.
- Draining still claims nothing and active work continues heartbeating.

### Recovery and replay

- Same ID/hash/name reuses a terminal outcome.
- Same ID/hash with another name fails before execution.
- Session history before the current run is restored through the checkpoint/current-run
  sequence cutoff.
- Message, plan/summary, tool-outcome, and complete-state byte ceilings count compact
  UTF-8 bytes, including multibyte values.
- Incremental cursors close on success, limit failure, validation failure, and
  cancellation.
- Terminal mutating outcomes still select the latest durable workspace revision.

### Writer lease

- Same-owner acquisition renews an expired/near-expiry writer only under an active run
  lease.
- Exact repeated release is safe.
- A missing row, older generation, successor generation, token mismatch, run mismatch,
  worker mismatch, or run-token mismatch fails closed.
- Scheduler cleanup still releases the writer associated with the expired run token.

### Verification

Run:

1. focused Sequence 17–20 unit tests;
2. Ruff formatting and lint;
3. strict mypy;
4. all unit and integration tests;
5. real PostgreSQL distributed-execution tests;
6. pre-commit hooks;
7. dependency audit and frozen lock verification;
8. all package builds;
9. `git diff --check`;
10. final status and diff inspection.

No container runtime is needed for implementation. If the real PostgreSQL suite needs
its local service container, it must use Podman only.

## Deferred by scope

The following are not Sequence 17–20 defects:

- priority classes, tenant fairness, admission control, overload behavior, and quotas
  (Sequences 21–22);
- cluster autoscaling, disruption budgets, Kubernetes termination-grace policy, and
  network policy (Sequence 23);
- offline worker labelling, queue-age dashboards, tracing, alerting, and SLOs
  (Sequence 24);
- Redis as a correctness dependency (explicitly unnecessary);
- exactly-once provider billing or exactly-once run delivery.

## Implementation outcome

Implemented:

1. Added active-run-fenced worker event and tool-state persistence. Both methods lock
   and validate the exact unexpired run lease and mirrored run with PostgreSQL
   `clock_timestamp()` before reading an old delivery or writing new state.
2. Changed `WorkerService` to race recovery, restoration, and execution independently
   against the same heartbeat. Lease loss or distributed cancellation cancels the
   active phase and cannot fall through into a later phase.
3. Propagated and validated the exact `WorkspaceWriterLease` through restoration,
   execution, and `AgentLoop` construction.
4. Retained tool names in recovered loop outcomes and fail closed when a stable call ID
   and argument hash are reused for another tool.
5. Changed recovery messages from run-scoped to tenant/session-scoped sequence recovery,
   using the checkpoint sequence or the current run's maximum sequence as the cutoff.
6. Added aggregate PostgreSQL byte preflights and incremental `yield_per=1` streaming
   for messages and terminal outcomes, plus a defensive 16 MiB complete-state ceiling.
7. Renewed same-owner writer acquisition, added complete identity checks to heartbeat
   and release, preserved exact repeated release, and rejected missing/stale/successor
   generations.
8. Added deterministic unit tests and real PostgreSQL reassignment evidence for stale
   Worker A rejection, prior-session recovery, writer-fence propagation, and Worker B
   completion.
9. Updated ADRs 0017–0020, `DESIGN.md`, and `README.md` to match the actual contracts.

No dependency or lockfile change was required. A local `uv` executable was installed
only into the existing development virtual environment to run the repository's hooks;
it is not part of the project dependency set.

## Remaining limitations and risks

1. **Database-authoritative time is not universal.** Fenced worker writes use PostgreSQL
   time, but claim, heartbeat, finish, and scheduler recovery still receive a validated
   caller timestamp. Production should add a database-time provider or a bounded
   clock-skew check before multi-node rollout.
2. **Sandbox capability routing is not represented on runs.** Worker capabilities are
   durable but not used by claim selection. This is acceptable only while every
   production run requires the same rootless Podman composition.
3. **Checkpoint/tool association is timestamp-based.** Recovery still uses completion
   timestamps to select post-checkpoint outcomes. A compatible migration should store
   the pre-tool checkpoint ID or another durable order key on each mutating tool call.
4. **The strongest acceptance test is not yet an OS-process kill.** Real PostgreSQL
   proves Worker A expiry, stale-write rejection, Worker B restoration, idempotent tool
   reuse, and completion through the actual service. A production snapshot materializer
   and worker factory are still needed before the test can kill an OS worker and compare
   a materialized final Git patch.
5. **Graceful drain has no forced deadline.** Kubernetes termination-grace and forced
   cancellation policy remain Sequence 23 work.
6. **Single checkpoint/plan rows rely on validated writers plus the final state
   ceiling.** Message/tool collections are preflight before streaming, but an
   independently corrupted oversized checkpoint row is materialized before the final
   domain ceiling rejects it. A future migration can add per-row database constraints.

## Verification results

Completed on 2026-07-30:

1. Ruff format and lint: passed across application, package, script, service, and test
   sources.
2. Strict mypy: passed across 105 source files.
3. Unit suite: 466 tests passed.
4. Coverage: 86.24%, above the required 85% threshold.
5. Integration suite: 3 tests passed.
6. Real PostgreSQL suite under rootless Podman: 11 tests passed.
7. Focused Sequence 17–20 suite: 31 tests passed (21 distributed execution and 10
   PostgreSQL adapter tests).
8. Pre-commit: all hooks passed.
9. Dependency audit: no known vulnerabilities found.
10. Frozen lock verification: 109 packages resolved and a frozen all-package sync
    dry-run succeeded; the only proposed uninstall was the locally installed `uv`
    command, confirming no project dependency drift.
11. Package builds: source distributions and wheels passed for all 10 workspace
    packages.
12. Alembic upgrade/check ran successfully as part of the real PostgreSQL fixture.
13. `git diff --check`: passed.
14. Podman test containers after the suite: none.

## Assumptions

- “Sequences 17–20” means PR 17 through PR 20, not all of Phase 6 plus later
  Kubernetes/observability work.
- PostgreSQL remains the correctness authority; Redis is not introduced.
- Production workspace execution remains rootless Podman-only.
- One claimed run conservatively owns one workspace writer even for read-only work.
- Cross-process mutation safety requires both the database writer fence and a
  composition root that passes that fence into the concrete workspace/sandbox.
- The process-kill acceptance gap cannot be represented honestly as complete until a
  durable snapshot materializer and production worker factory exist.
