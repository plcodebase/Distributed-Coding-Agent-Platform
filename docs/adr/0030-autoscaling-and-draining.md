# ADR 0030: Autoscaling and graceful draining

- Status: Accepted
- Date: 2026-08-11
- Sequence: 30

## Context

CPU alone is not a useful worker demand signal, and pod termination must stop claims before active
lease owners disappear.

## Decision

- Scale API from CPU/request rate, workers from queue depth/oldest age, and LiteLLM from active
  requests/p95 latency through `autoscaling/v2` and external metrics.
- Provide Prometheus Adapter rules but treat adapter installation as a cluster prerequisite.
- Add a bounded operations server. Liveness follows the process; readiness follows durable service
  readiness and becomes false immediately on an idempotent loopback drain request.
- Keep a failed drain callback retryable and reject ambiguous HTTP framing at the loopback boundary.
- Invoke drain from an in-pod `preStop` action. Preserve forced-termination recovery through the
  existing lease/checkpoint scheduler.
- Separate deterministic state-machine evidence from live cluster measurements with typed drivers.
- Provide a concrete bounded Kubernetes driver that preflights custom metrics, observes HPA desired
  replicas, pod identities, queue/task state, recovery time, and commit counts, then removes the
  exact observed worker. Authenticated task submission/status remains an injected deployment
  dependency.

## Consequences

CI proves scaling, conservation, graceful drain, and lease-recovery logic without claiming observed
Kubernetes behavior. Production acceptance still requires real external metrics and pod removal.
