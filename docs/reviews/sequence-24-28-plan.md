# Sequences 24–28 Review and Hardening Plan

**Status:** Implemented and verified
**Source of truth:** `DESIGN.md`, original implementation design Phases 8, 9, and 11,
and PRs 24–28
**Review date:** 2026-08-11

## Review method

The review compared the five sequence commits and their tests against the original
design acceptance criteria. Evidence included domain models, PostgreSQL queries and
migrations, API/worker/scheduler composition, telemetry producers, provisioned
Prometheus/Grafana artifacts, load/chaos runners, manifests, and the full passing test
baseline. A claim is considered implemented only when a production path produces the
signal or invariant and a test exercises the relevant boundary.

The current baseline is strong: all five sequences are separately committed, schemas
are closed and bounded, untrusted errors are opaque, deterministic reports cannot call
themselves measurements, and the full repository passes lint, strict typing, 570 unit
tests, the 85% coverage gate, and three integration tests. The gaps below are places
where a correctness, containment, or evidence claim is stronger than its producer.

## Sequence 24 — Long-term memory and task tracking

### Review verdict

The durable baseline is correct: task plans use run-serialized compare-and-set
versions; completed runs enqueue one idempotent extraction job in the completion
transaction; extraction jobs are leased and generation-fenced; memory has tenant,
session, run, time, kind, and content-hash provenance; policy is rechecked on claim,
completion, and retrieval; and old task-plan rows remain readable without migration.

It needs hardening in four areas.

### Confirmed gaps

- **P0 — expired lease can still disclose source text:** `source_for_job()` verifies
  token and generation but has no operation time. A worker may read a transcript and
  contact the model after its lease expired; only the later durable completion is
  fenced.
- **P0 — run provenance is broader than the extracted source:** the job records one
  `source_run_id`, but the source query selects every message in the session up to the
  watermark. A memory derived from an earlier run is therefore attributed to the run
  that happened to trigger extraction.
- **P1 — dependency-inconsistent task states are valid:** a task may be in progress or
  completed while one of its declared dependencies is pending. The graph is acyclic,
  but its state is not internally coherent.
- **P1 — terminal failure retryability is misleading:** the processor converts every
  extraction exception to a generic retryable error, yet `FAILED` jobs are terminal
  and are not reclaimed. Structured non-retryable gateway validation failures are also
  discarded.

### Implementation plan

1. Add `occurred_at` to the memory source-read boundary and fence it against the active
   lease before reading any message content.
2. Restrict the extraction query to `job.run_id` while retaining the durable session
   sequence watermark and existing byte/message ceilings.
3. Require every `IN_PROGRESS` or `COMPLETED` task to have all dependencies completed.
   Keep pending, blocked, and cancelled tasks valid so clients can express work that is
   waiting or abandoned.
4. Preserve safe `DomainOperationError` code and retryability when extraction fails.
   Mark unexpected failures and timeouts terminal/non-retryable to match the current
   job lifecycle instead of implying an automatic retry that does not exist.
5. Add domain, processor, repository-unit, and PostgreSQL integration coverage for
   expired reads, run-scoped sources, task-state coherence, and structured failures.

## Sequence 25 — OpenTelemetry and Prometheus metrics

### Review verdict

W3C trace propagation, explicit provider lifecycle, content-free span attributes,
bounded label registries, opaque tenant cost labels, and instrumentation for API,
queue, worker, context, model, tools, checkpoints, and sandbox startup are present.
Provider-specific imports remain outside `agent-core`.

The telemetry is incomplete around error semantics and reliability evidence.

### Confirmed gaps

- **P0 — handled API failures are not structured telemetry errors:** exception handlers
  return safe responses, but do not mark the request span or increment the stable error
  counter. Authentication, validation, capacity, conflict, and persistence failures
  therefore appear only as HTTP status series.
- **P1 — escaping exceptions leave spans unset:** automatic exception recording is
  correctly disabled for privacy, but the span helper does not set a content-free error
  status when an exception or cancellation escapes.
- **P1 — accepted-run and recovery evidence is absent:** there are no stable counters
  for newly accepted runs, terminal run outcomes, expired-lease recoveries, reconnects,
  or idempotent replays. The reliability dashboard cannot directly show those design
  invariants.
- **P1 — some declared series have no producer:** fallback and tenant-cost metrics are
  defined, but the current normalized gateway response does not expose authoritative
  fallback or price metadata. Producing zero would fabricate evidence.
- **P2 — worker and scheduler registries are not yet scrapeable in a deployed process:**
  telemetry is injectable, but production worker/scheduler composition and service
  discovery belong to Sequences 29–30. This remains an explicit deployment limitation,
  not a local metric claim.

### Implementation plan

1. Map domain error codes and request-validation failures to the stable telemetry error
   categories in the API handlers; mark the active request span without recording
   message/details content.
2. Make the span context manager mark uncaught exceptions and cancellations with
   content-free status/attributes while continuing to suppress exception events.
3. Add bounded counters for accepted runs, run outcomes, run recoveries, event-stream
   reconnects, and idempotent replays. Instrument the API, worker, scheduler, same-run
   tool replay, and durable gateway replay producers.
4. Keep cost and fallback series observation-only. Do not emit a zero until an
   authoritative gateway value exists; document their unavailable state.
5. Extend telemetry tests for classification, exception status, replay accounting, and
   fixed label sets. Keep raw run/session/call IDs out of metrics.

## Sequence 26 — Grafana dashboards and Prometheus rules

### Review verdict

Provisioning is immutable, uses stable UIDs, disables anonymous access, mounts rule
files read-only, covers every Phase 9 dashboard category, and statically rejects metric
references that have no exporter or recording rule.

Two rule semantics and the reliability view need correction.

### Confirmed gaps

- **P0 — total provider failure disappears:** the provider-success numerator selects
  only `outcome="success"`. When a route has failures and no successes, Prometheus has
  no matching numerator series, so the ratio and low-success alert can disappear.
- **P1 — alert thresholds contradict the design targets:** the project targets gateway
  success above 99% and API availability of 99.5%; current alerts use 95% provider
  success and a 5% API error threshold.
- **P1 — recovery and replay are absent from the reliability dashboard:** worker lease
  recovery, event reconnect/replay, terminal run outcomes, and duplicate suppression
  are required failure evidence but have no panels.
- **P2 — validation is structural, not runtime PromQL evaluation:** query names and
  provisioning are checked, but an actual Prometheus evaluator remains part of the
  later live-stack verification.

### Implementation plan

1. Zero-fill the success numerator from the all-attempt route vector so all-failure
   routes retain a zero-valued success-ratio series.
2. Align alert thresholds with the documented test objectives: provider success below
   99% and API server-error ratio above 0.5%, while continuing to label them test
   targets rather than production guarantees.
3. Add recording rules and reliability panels for accepted/terminal runs, recoveries,
   reconnects, and replay suppression using the new Sequence 25 metrics.
4. Strengthen unit assertions for the all-failure expression, threshold values, stable
   panel titles, and the complete alert set.

## Sequence 27 — Load-test suite

### Review verdict

The runner uses a fixed task ceiling, per-operation deadlines, closed versioned
profiles, opaque errors, atomic bounded reports, p50/p95/p99 aggregation, and a real API
plus durable-WebSocket live path. Synthetic output is correctly labelled
`simulation_only`, and unavailable usage is not replaced with fabricated zeroes.

The live protocol has four correctness/resource gaps.

### Confirmed gaps

- **P0 — campaign replays can invalidate measurements:** live idempotency keys contain
  only profile name and operation index. Re-running the same profile against a session
  may replay old runs rather than create the requested workload.
- **P0 — HTTP and event retention are bounded too late or too loosely:** `httpx.post()`
  buffers the complete response before the 1 MiB check. Event count is bounded, but up
  to 100,000 individually large events can be retained in one list.
- **P1 — live URLs can embed credentials or ambiguous components:** prefix checking
  accepts userinfo, query strings, and fragments, which can leak into report/config
  representations or alter endpoint construction.
- **P1 — stream profiles do not verify sequence continuity:** submitted-run streams do,
  but standalone WebSocket/event-throughput profiles only count messages and mark the
  connection successful even when sequences gap.
- **P1 — resource scope is ambiguous:** CPU and RSS describe the load-generator process,
  not the distributed deployment, but the report does not encode that scope.
- **P2 — token, cost, and fallback data remain unavailable from durable run events:**
  summaries correctly use `count=0`. Adding authoritative per-run attribution requires
  a later event/gateway contract and must not be guessed here.

### Implementation plan

1. Add a validated campaign identifier to drivers and reports. Generate a unique live
   campaign ID and include it in every idempotency key; use a stable simulation ID.
2. Stream run-creation responses with cumulative byte accounting before model
   validation. Add a hard cumulative event-stream byte ceiling independent of event
   count and enforce it for both submitted and standalone stream paths.
3. Parse live URLs, require an HTTP(S) origin, and reject credentials, paths, queries,
   and fragments.
4. Validate first sequence, strict continuity, run identity, and terminal semantics for
   standalone streams. Distinguish connection success from task success without
   reporting a failed/gapped stream as successful.
5. Add an explicit `load_generator_process` resource scope and document deployment
   resource collection as future live-stack evidence.
6. Test unique campaign keys, incremental response limits, cumulative multibyte event
   limits, URL rejection, sequence gaps, and report invariants.

## Sequence 28 — Chaos-test suite

### Review verdict

All ten design failures have closed manifests and expected outcomes. Scenarios execute
serially; cleanup is shielded and independently bounded; reports are closed and
simulation-only in CI; service operations use an absolute executable, exact targets,
no shell, a minimal environment, and bounded process output.

The evidence lifecycle needs stronger ownership by the harness.

### Confirmed gaps

- **P0 — recovery time is driver-asserted:** the runner trusts
  `observation.recovery_seconds`; a live driver can under-report the deadline even when
  `recover()` and `observe()` took longer.
- **P0 — zero-valued evidence passes:** required dashboard signals are checked only by
  enum presence, so an observation with value zero satisfies the invariant.
- **P0 — some service faults are restored before resilience is observed:** the generic
  service `recover()` starts a terminated worker or disabled provider before the probe,
  which can turn a fallback/replacement-worker test into a normal healthy-path test.
- **P1 — fault injection has no independent deadline:** one recovery timeout wraps
  inject, recover, and observe, making recovery timing ambiguous.
- **P1 — manifest targets are descriptive rather than enforced:** action is tied to the
  scenario, but `fault.target` may be any bounded name and can disagree with the
  reviewed scenario contract.
- **P1 — no live CLI composition path exists:** live driver classes exist, but the CLI
  accepts only simulation, so operators cannot run a trusted injected live driver
  through the versioned manifest/report path.
- **P1 — evidence provenance is untyped:** a live report cannot distinguish a
  Prometheus observation from simulation or other probe data.

### Implementation plan

1. Add an independent bounded fault timeout. Measure recovery from immediately after
   successful injection through recovery and observation in the runner, then overwrite
   any driver-supplied duration with the harness measurement.
2. Require positive finite signal values and typed evidence sources. Simulation uses a
   simulation source; live required dashboard signals must use Prometheus provenance.
3. Make service recovery scenario-aware: restart Redis and restore PostgreSQL before
   observation where required, but keep the killed worker and disabled primary provider
   fault active until the probe has demonstrated replacement/fallback behavior. Cleanup
   restores every active target afterward.
4. Enforce the exact reviewed target for each scenario in the manifest model.
5. Add a bounded trusted `module:factory` live CLI composition path, validate the
   returned driver contract/mode, and close it with an independent deadline.
6. Test injection timeout, harness-owned timing, positive/provenanced evidence,
   scenario-specific command ordering, target mismatches, live factory loading, and
   cleanup/close failures.

## Deferred limitations and non-goals

- No live load or chaos result will be created during this hardening. Simulation
  reports remain harness tests, not scale or recovery measurements.
- Worker/scheduler scrape service composition, Prometheus runtime rule evaluation,
  Kubernetes recovery, and horizontal scaling remain Sequences 29–30.
- Authoritative per-run price and upstream-provider fallback metadata are not exposed by
  the current normalized gateway contract. Cost/fallback series remain absent rather
  than fabricated.
- Coding-task evaluation and the final committed benchmark report remain Sequences
  31–32.
- No container runtime is required or invoked by this hardening work.

## Verification plan

1. Run focused Sequence 24–28 unit tests after each implementation slice.
2. Run Ruff formatting/lint and strict mypy after contract changes.
3. Run deterministic load and chaos CLIs and verify their reports remain explicitly
   simulation-only.
4. Run the full `make test` gate, pre-commit hooks, dependency audit, frozen lock check,
   all package builds, migration-head check, and `git diff --check`.
5. Inspect the complete diff and map every confirmed gap to code plus a regression test.

## Implementation outcome

- Sequence 24 now fences extraction source reads by current time and exact lease,
  selects only source-run messages, rejects dependency-incoherent task states, and
  preserves safe structured extraction failures.
- Sequence 25 now classifies handled and escaping errors consistently and exports
  accepted-run, terminal-state, recovery, reconnect, and replay counters from their
  authoritative producers.
- Sequence 26 now retains provider routes with zero successes, uses the documented test
  thresholds, and displays accepted/terminal runs, recoveries, reconnects, and replay
  suppression.
- Sequence 27 now uses campaign-scoped idempotency, incrementally bounds HTTP and event
  streams, validates strict live origins and durable sequence continuity, and labels
  resource measurements as load-generator-process scope.
- Sequence 28 now owns recovery timing and independent phase deadlines, requires
  positive provenanced evidence, validates exact semantic targets, preserves faults
  through observation where necessary, and supports trusted live-driver composition.

## Verification outcome

- Ruff formatting and lint: passed.
- Strict mypy over applications, packages, scripts, and tests: passed.
- Unit tests: 580 passed; total coverage 85.47% against the 85% gate.
- Integration tests: 3 passed.
- Pre-commit hooks: passed after configuring repository-local tool caches and the
  absolute existing `rg` directory.
- Dependency audit: no known vulnerabilities.
- Frozen workspace sync: passed with no lockfile change.
- All ten workspace packages built as source distributions and wheels.
- Alembic reports one migration head: `0007`.
- Deterministic load and chaos CLIs produced only `simulation_only` reports; all ten
  simulated chaos scenarios passed their harness invariants.
- `git diff --check`: passed after removing review-document trailing whitespace.
- No container runtime was invoked.
