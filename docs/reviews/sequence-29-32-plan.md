# Sequences 29–32 Implementation Plan

**Status:** Implemented; live acceptance pending external infrastructure
**Scope:** PRs 29–32 from the original implementation sequence
**Runtime rule:** Kubernetes is the deployment target; Podman is the only permitted local
container runtime. This work does not invoke a container runtime.

## Current-state review

The repository already contains the provider-neutral agent loop, hardened Podman sandbox,
PostgreSQL queue and leases, worker draining, recovery scheduler, API health endpoints,
central telemetry, bounded gateway load tests, and deterministic chaos tests. The remaining
original-plan work is operational packaging and evidence:

- there are no Kubernetes manifests, workload identities, network policies, autoscalers, or
  deployment-contract tests;
- worker draining exists in the service layer, but Kubernetes has no readiness/drain endpoint or
  termination hook;
- there is no deterministic 30–100 task coding evaluation corpus or evaluation report contract;
- there is no evidence-gated final benchmark report or consolidated production architecture guide;
- no live Kubernetes cluster, external metrics adapter, managed PostgreSQL/Redis/object store, or
  provider credentials are available in this workspace. Live scale and recovery results therefore
  cannot be truthfully manufactured during CI.

## Cross-cutting implementation rules

1. Preserve the established package boundaries. Deployment validation and benchmark orchestration
   live in `scripts/`; core agent packages do not import Kubernetes clients or infrastructure code.
2. Keep all manifests declarative and compatible with `kubectl apply -k`. Do not require Helm or a
   generated manifest toolchain to inspect them.
3. Reference externally provisioned Secrets. Do not commit credentials, credential-shaped sample
   values, or provider keys.
4. Default deny network access. Only the LiteLLM gateway may reach public model-provider endpoints;
   workers receive only the internal gateway credential, never provider credentials.
5. Make offline verification deterministic and label it as simulation. Only reports from an
   explicitly live driver may support production performance claims.
6. Bound manifest input, command output, task counts, strings, samples, report sizes, and subprocess
   durations. Invoke subprocesses with argument vectors and no shell.
7. Fail closed on malformed, unknown, duplicate, incomplete, or internally inconsistent evidence.
8. Do not invoke any container runtime. Future image examples and operator guidance
   use Podman.

## Sequence 29 — Kubernetes manifests

### Deliverables

1. Add a production OCI image definition for the Python workspace and Kubernetes base manifests for:
   Agent API, event gateway, scheduler, agent worker, and LiteLLM.
2. Add a namespace, immutable non-secret configuration, individual ServiceAccounts, Services,
   Deployments, and PodDisruptionBudgets.
3. Apply non-root, read-only, seccomp, dropped-capability, resource, probe, rollout, topology-spread,
   and anti-affinity defaults to every workload.
4. Schedule workers onto dedicated sandbox nodes by label and toleration. Keep the Podman sandbox's
   default-deny network policy as the command-execution boundary.
5. Reference two separately managed Secrets:
   - platform runtime connectivity, usable by API/scheduler/worker as narrowly required;
   - provider credentials, mounted only into LiteLLM.
6. Add default-deny ingress/egress policies and explicit DNS, internal service, managed-data-service,
   and LiteLLM-provider paths. Document the cluster-specific CIDR overlay required for managed
   services rather than silently opening worker egress to the Internet.
7. Add a strict static manifest validator that proves workload coverage, identity separation,
   secret non-disclosure, security contexts, probes, resources, policies, and service wiring.

### Verification

- Unit tests load and validate every YAML document without requiring a cluster.
- `kubectl kustomize` is an optional operator check, not a CI dependency.
- Secret-name and environment-variable assertions prove provider keys are absent from worker pods.

## Sequence 30 — autoscaling, graceful draining, and deployment tests

### Deliverables

1. Add a bounded worker operations server with:
   - liveness while the process is alive;
   - readiness only while claims are accepted;
   - Prometheus metrics;
   - an authenticated-by-loopback drain endpoint used by the Kubernetes `preStop` hook.
2. Integrate it with the worker process lifecycle so drain marks the durable worker record, stops new
   claims, waits for active attempts, and only then reports completion. A hard termination remains
   recoverable through existing leases/checkpoints.
3. Add `autoscaling/v2` HPAs:
   - API: CPU plus request-rate metric;
   - worker: queue depth and oldest queued age;
   - LiteLLM: active request and latency metrics.
4. Add Prometheus Adapter rule configuration for the named external metrics and state the adapter as
   an external cluster prerequisite.
5. Add an offline deployment simulator for queue-driven scale-up, graceful worker removal, expired
   lease recovery, and accepted-task conservation.
6. Add an opt-in live deployment verifier contract. It records cluster identity, commands, timings,
   workload replicas, task outcomes, and failures in a bounded evidence report; it never converts
   offline simulation into a live claim.

### Verification

- Tests exercise operations-server health transitions and concurrent drain requests.
- Manifest tests validate HPA metric names, replica bounds, termination grace, and `preStop` wiring.
- Simulation tests require zero lost accepted tasks and zero duplicate completions.
- Live scale-up and rolling-removal acceptance remain pending until a real cluster and metrics adapter
  are supplied.

## Sequence 31 — deterministic coding-task evaluation

### Deliverables

1. Add a versioned manifest of 30 deterministic repository tasks covering the ten original task
   families: failing tests, API endpoints, type errors, multi-file renames, validation, duplicate
   refactors, dependency usage, concurrency bugs, missing tests, and exception diagnosis.
2. Define closed typed task, attempt, per-task result, metric, environment, and campaign-report
   contracts. Enforce the original 30–100 task campaign size.
3. Materialize every fixture into an isolated temporary workspace using contained relative paths,
   exact file modes, byte/count ceilings, and no symlink support.
4. Drive the agent through an injected trusted evaluation driver. The harness does not import a model
   provider and does not run model-authored commands on the host.
5. Record success, test result, iterations, tool calls, token input/output, cost, queue wait,
   first-token/total latency, retries, fallbacks, permission denials, final status, and structured
   failure reason.
6. Add a deterministic fake driver for CI and a trusted `module:attribute` live-driver composition
   hook for a deployed platform. Reports are atomically written, size-bounded, and explicitly marked
   `simulation` or `live`.
7. Require every failure to match a declared expected outcome. Detect duplicate task IDs, missing
   results, duplicate results, and inconsistent totals.

### Verification

- Run all 30 fixtures through the fake driver without external credentials.
- Test malformed manifests, traversal, oversized fixtures, invalid metrics, missing outcomes,
  duplicate results, driver failures, timeouts, and report bounds.
- A real coding-success percentage remains pending a live agent evaluation; the CI result is evidence
  of harness correctness only.

## Sequence 32 — final evidence report and architecture documentation

### Deliverables

1. Add an evidence compiler that accepts bounded, typed deployment, gateway-load, chaos, coding-task,
   and quality-gate reports.
2. Verify source revision, environment, mode, task conservation, duplicate counts, required metric
   coverage, and report integrity before admitting evidence.
3. Produce a machine-readable final report and a Markdown summary with:
   methodology, software revision, environment/hardware, limitations, pass/fail acceptance matrix,
   and measured metrics with source attribution.
4. Enforce claim provenance:
   - simulation can establish deterministic correctness only;
   - live measurements are required for replicas, RPM, p95 latency, recovery time, cost/routing, coding
     success, sandbox startup, and resource claims;
   - missing live evidence yields `incomplete`, never an inferred or zero-valued metric.
5. Commit an honest baseline report that identifies which Definition-of-Done items are verified in CI
   and which require external infrastructure.
6. Add consolidated architecture, deployment, reliability, evaluation, and operator documentation,
   plus ADRs 0029–0032 and status updates in `README.md` and `DESIGN.md`.

### Verification

- Tests reject mixed revisions, simulation-backed performance claims, lost accepted tasks, duplicate
  commits, missing methodology/hardware, stale schemas, and tampered evidence.
- A deterministic baseline report is reproducible from repository-owned inputs.
- Full unit/integration/security tests, Ruff, strict mypy, pre-commit, dependency audit, frozen lock,
  package builds, `git diff --check`, and final status/diff inspection must pass.

## External acceptance boundary

The implementation can make deployment and benchmark execution ready and prove contracts offline.
The following original acceptance statements require an actual Kubernetes cluster and real provider
traffic and will remain explicitly unverified unless that environment is supplied:

- observed worker scale-up from queue metrics;
- rolling pod removal with measured recovery time;
- multi-provider fallback and routing-cost improvement under real traffic;
- measured coding-task success, RPM, p95 latencies, cost, sandbox startup time, and resource use.

This is not a scope reduction: the live verifier and evidence slots are part of the implementation.
It prevents simulated values from being presented as production measurements.
