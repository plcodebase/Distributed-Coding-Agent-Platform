# ADR 0028: Fail-closed chaos suite

- Status: Accepted
- Date: 2026-08-10
- Sequence: 28

## Context

Executing a fault command proves neither recovery nor correctness. A worker can stop
successfully while its accepted run silently disappears; duplicate delivery can appear
healthy while committing the same edit twice. Fault automation also creates a new
privileged operational boundary if it discovers broad targets or leaves services down
after cancellation.

## Decision

- Define closed, versioned contracts for the ten design scenarios. Each includes an
  exact fault, preconditions, independent recovery and cleanup deadlines, allowed final
  states, retry class when applicable, accepted-task visibility, durable-event
  continuity, single-commit bounds, and required metric evidence.
- Run scenarios serially. Evaluate observations after recovery and fail every unmet
  invariant explicitly. A fault-command exit code is never a pass condition.
- Always invoke cleanup after success, timeout, driver failure, or cancellation. Cleanup
  is shielded, independently bounded, and reported as its own invariant failure. A
  failed injection does not start or mutate the target during cleanup.
- Keep deterministic simulation visibly separate from live evidence. It verifies the
  harness and report contracts but always emits `simulation_only`.
- Provide an opt-in Podman service injector for only worker, Redis, PostgreSQL, and fake
  provider faults. Require an absolute executable and exact allowlisted container
  identifiers; use no shell, bounded output, process-group termination, a minimal
  environment, and no runtime socket or privileged operation. Other fault types use
  injected platform test seams.
- Store only bounded categories and numeric metric observations. Exception text,
  credentials, service output, prompts, tool values, and source content never enter a
  chaos report.

## Consequences

CI can catch regressions in scenario definitions, invariant evaluation, cancellation,
cleanup, and command construction without mutating infrastructure. Live acceptance
still requires an isolated deployment and a probe that reads durable run/events and
the Sequence 25 metrics. Kubernetes fault injection and horizontal recovery evidence
remain part of the deployment and final benchmark sequences.
