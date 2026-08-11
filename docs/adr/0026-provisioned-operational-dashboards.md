# ADR 0026: Provisioned operational dashboards and rules

- Status: Accepted
- Date: 2026-08-10
- Sequence: 26

## Context

Dashboards assembled manually in a running Grafana instance are not reviewable,
repeatable, or testable. Queries can also drift from exported metric names and create a
false impression that a reliability signal exists.

## Decision

- Provision a stable Prometheus datasource UID and immutable file-backed dashboards
  from the Podman Compose deployment tree.
- Split views into platform overview and reliability dashboards. Cover active runs,
  queue pressure, worker utilization, sandbox startup, provider success, model latency
  and first output, token rate, opaque-tenant cost, retries, fallbacks, circuit state,
  structured errors, API errors, tool outcomes, checkpoints, accepted/terminal runs,
  scheduler recoveries, event reconnects, and idempotent replays.
- Load versioned Prometheus recording and alert rules through a read-only mount.
  Recording rules centralize p95 and ratio expressions; alerts include a nonzero hold
  duration and stable warning or critical severity.
- Treat the documented SLO values as test targets rather than guarantees. Dashboards
  display measurements; they do not claim that an SLO has been met without load-test
  evidence.
- Preserve provider routes that have attempts but zero successes when computing success
  ratios. Alert against the design test targets of 99% provider success and 0.5% API
  server errors; do not substitute looser thresholds silently.
- Validate JSON/YAML provisioning, stable UIDs, panel coverage, rule metadata, and every
  platform metric reference in unit tests. Prompts, source code, arguments, results,
  and credentials are prohibited from dashboard and alert definitions.

## Consequences

The same dashboards and rules are recreated after every local stack restart and are
reviewed with normal code changes. Missing or renamed metrics fail tests before a
dashboard is shipped. Runtime PromQL evaluation still depends on Prometheus and is
verified by the later stack and load-test sequences.
