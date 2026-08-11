# Sequences 24–28 Implementation Plan

**Status:** Implemented and verified
**Source of truth:** `DESIGN.md`, Phases 8, 9, and 11, and PRs 24–28
**Implementation date:** 2026-08-10

## Current-state audit

Sequence 24 is implemented and verified. The repository already provides versioned task
plans, tenant/session memory controls, provenance-bearing long-term memory, bounded and
fenced extraction jobs, and the corresponding API and PostgreSQL contracts. This work
is the compatibility baseline rather than a second Sequence 24 redesign.

The remaining PR 25–28 gaps are:

- structured logs exist, but there is no OpenTelemetry SDK, trace propagation, platform
  span model, Prometheus registry, or application `/metrics` endpoint;
- Prometheus scrapes LiteLLM and the host API, but the platform exports no metrics;
- Grafana has a datasource only, with no provisioned dashboards or alert rules;
- no reproducible load runner records percentile, throughput, resource, queue, token,
  fallback, or permission-denial evidence;
- no declarative chaos runner records expected outcomes and proves recovery, loss, and
  duplicate-commit invariants across the design's failure scenarios.

Kubernetes, HPA, graceful deployment draining, coding-task evaluation, and the final
benchmark/architecture report remain PRs 29–32.

## Sequence 25 — OpenTelemetry and Prometheus metrics

1. Extend `platform-telemetry` with validated settings, OpenTelemetry SDK construction,
   W3C trace-context propagation, an in-memory test exporter, and explicit shutdown.
2. Add a small injected observer interface. Disabled/no-op construction must remain safe;
   provider-neutral packages must not import exporter or HTTP-server implementations.
3. Create secret-safe spans for queue wait/claim, context construction, model request,
   gateway wait, tool execution, Podman sandbox startup, and checkpoint creation.
4. Propagate trace, tenant, session, run, turn, model-call, and tool-call identity through
   context variables and carrier injection/extraction. IDs are span attributes, never
   unbounded Prometheus labels.
5. Export bounded Prometheus counters, gauges, and histograms for API requests, run/queue
   state, workers, sandbox startup, provider/model outcomes, tokens, cost, retries,
   fallbacks, circuit state, checkpoints, and tool calls.
6. Bound tenant-cost label cardinality with opaque tenant hashes and one overflow label.
   Never export source code, prompts, tool arguments/results, secrets, exception text, or
   raw authentication data.
7. Expose `/metrics` from the API through the injected registry and update Prometheus
   scrape configuration. Metrics export must remain available without an OTLP collector.
8. Add deterministic tests for trace ancestry, propagation, cardinality, redaction,
   histogram/counter output, cancellation/error status, cleanup, and package boundaries.

## Sequence 26 — Grafana dashboards

1. Add file-based Grafana dashboard provisioning with stable UIDs and read-only sources.
2. Add Prometheus recording and alert rules for request success, queue age, worker
   saturation, provider success, model latency, token rate, retry/fallback rate, and
   circuit state.
3. Provision dashboards for every Phase 9 signal: active runs, queue depth, oldest wait,
   worker utilization, sandbox startup, provider success, model latency, token rate,
   bounded cost by tenant, retries, fallbacks, and circuit breakers.
4. Add a reliability dashboard for the initial project targets while labelling them as
   test-deployment objectives rather than published production guarantees.
5. Validate dashboard JSON, datasource UIDs, PromQL references, rule syntax, compose
   mounts, and anonymous/unsafe Grafana settings in unit tests.

## Sequence 27 — load-test suite

1. Add a closed benchmark model and runner with bounded concurrency, duration, samples,
   response bytes, report bytes, and per-operation timeouts.
2. Provide canonical 10, 50, and 100 concurrent-run profiles plus gateway and sandbox
   saturation profiles.
3. Record throughput, queue wait, time to first token, p50/p95/p99 run latency, resource
   utilization, error rate, tokens, retries, fallbacks, and permission denials.
4. Provide deterministic in-process adapters for CI and explicit live adapters for API,
   event stream, gateway, PostgreSQL, Redis, worker, and sandbox exercises. CI results are
   labelled smoke evidence and cannot be presented as production performance.
5. Write canonical JSON reports atomically under `benchmarks/reports`, including git
   revision, clean/dirty state, UTC time, methodology, platform/Python information,
   target kind, and parameter set. Refuse ambiguous or unbounded output.
6. Add tests for percentile calculation, cancellation, timeout, partial failure,
   concurrency bounds, report determinism, hardware metadata, and secret-safe failures.

## Sequence 28 — chaos-test suite

1. Define closed chaos scenario, fault, expected-outcome, observation, and report models.
2. Cover worker termination, Redis restart, PostgreSQL interruption, primary-provider
   disablement, gateway 429, event-client disconnect, sandbox OOM, duplicate task
   delivery, duplicate tool calls, and checkpoint interruption.
3. Implement safe in-process injectors for deterministic correctness tests and a
   Podman-only live injector with an absolute executable, allowlisted service/container
   identifiers, no shell, bounded output, process-group termination, and cleanup.
4. Require every scenario to assert accepted-run visibility, recovery deadline,
   committed-patch uniqueness, durable event continuity, and expected retry/error class
   as applicable. A timeout or missing observation fails closed.
5. Persist bounded reports with methodology and environment metadata. Never claim a
   scenario passed merely because the fault command succeeded.
6. Add deterministic tests for cleanup after cancellation/failure, duplicate suppression,
   worker lease recovery, provider fallback, event replay, checkpoint fencing, and report
   validation. Live Podman scenarios remain explicitly opt-in.

## Cross-sequence security and compatibility

- Podman is the sole supported live container runtime; no alternate runtime interface,
  socket, image format wrapper, or compatibility layer may be introduced or invoked.
- Telemetry defaults to excluding content. Source snippets, prompts, arguments, results,
  environment values, and exception strings are never metric labels or span attributes.
- Metric names and label sets are fixed. Dynamic IDs are trace attributes only; the one
  tenant metric uses a bounded opaque-label registry with overflow aggregation.
- Existing events, APIs, migrations, tool schemas, replay semantics, and Sequence 24
  persistence remain compatible.
- Load and chaos reports distinguish deterministic CI evidence from live deployment
  evidence and do not manufacture unmeasured résumé claims.

## Verification gates

After each sequence, run focused Ruff, strict mypy, and unit/integration tests. Before
completion, run the full test and coverage gates, pre-commit hooks, dependency audit,
frozen lock verification, package builds, `git diff --check`, and final status/diff
inspection. Optional service-level verification uses rootless Podman only.
