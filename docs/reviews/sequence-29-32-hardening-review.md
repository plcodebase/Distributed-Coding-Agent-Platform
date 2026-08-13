# Sequences 29–32 Review and Hardening Plan

## Scope and evidence basis

This review covers PR 29 through PR 32 only:

- Sequence 29: Kubernetes deployment manifests and production composition.
- Sequence 30: autoscaling, graceful drain, and rolling-deployment verification.
- Sequence 31: the deterministic coding-task evaluation corpus and harness.
- Sequence 32: final benchmark compilation, evidence provenance, and architecture documentation.

The review compares the current worktree with `DESIGN.md`, ADRs 0029–0032, the
Kubernetes base, the deployment/coding/benchmark scripts, and their unit tests. It
does not treat simulation output as production proof. No live cluster, provider
credentials, managed data services, or production object store were supplied, so
live acceptance criteria remain externally blocked until signed evidence is
captured in the target environment.

No container runtime is required for this hardening review or its local test suite.
Runtime isolation tests remain explicit Podman-only deployment work.

## Overall verdict

The four sequences form a useful implementation baseline, but they are not yet
production-acceptance ready. Static Kubernetes contracts, deterministic deployment
simulation, a 30-task corpus, and a provenance-aware final report exist. However,
several claims are currently stronger than the evidence:

| Sequence | Baseline result | Merge verdict |
|---|---|---|
| 29 | Kubernetes resources, policies, probes, services, HPAs, and a platform image exist | Not ready: role separation, secret isolation, metrics discovery, and runnable production composition have P0 gaps |
| 30 | Drain endpoints, worker drain state, scaling simulation, and deployment reports exist | Not ready: no concrete live Kubernetes driver and no end-to-end metrics proof |
| 31 | Thirty typed tasks and a bounded harness exist | Not ready: most fixtures have placeholder tests and driver claims are not independently verified |
| 32 | Final evidence compiler and generated report exist | Not ready: quality-gate evidence and scenario/category completeness are not enforced |

The current repository-wide verification baseline is green: Ruff, strict mypy,
618 unit tests, three integration tests, 85.02% total coverage, pre-commit,
dependency audit, frozen-lock verification, and all workspace package builds passed
before this review. Those results must be rerun after hardening and recorded as a
machine-readable Sequence 32 input.

## Sequence 29 — Kubernetes deployment and production composition

### Step-by-step review

1. **Workload inventory — partial pass.** Deployments and services exist for the
   API, event gateway, scheduler, workers, and LiteLLM. PostgreSQL, Redis, and
   object storage remain managed dependencies as intended.
2. **Role separation — fail.** The event-gateway Deployment starts the same full
   API application factory as the control-plane API. It therefore exposes control
   routes instead of a least-privilege event-only surface.
3. **Credential isolation — fail.** Non-gateway workloads use `envFrom` against
   broad runtime Secrets. A provider key accidentally added to one of those Secrets
   becomes available to the workload even though the Secret object name passes the
   static validator.
4. **Network isolation — partial.** Default deny and explicit egress policies are
   present, but data-service access is shared too broadly across roles. The
   LiteLLM ingress selector also admits roles that do not need direct model access.
5. **Metrics and autoscaling plumbing — fail.** HPAs reference custom metrics, but
   the manifests do not establish complete Prometheus discovery for each metrics
   endpoint, and monitoring ingress is incomplete. A metrics adapter may therefore
   have no time series to serve.
6. **Scheduling and availability — partial.** Resource limits, disruption budgets,
   topology spreading, dedicated worker placement, and probes are present. Explicit
   anti-affinity, service-link isolation, and startup probes are missing.
7. **Gateway route compatibility — partial.** The deployed LiteLLM route list does
   not preserve every stable route used by the platform configuration.
8. **Production executability — fail closed.** Scheduler and worker composition
   factories are referenced through configuration, but the platform image does not
   ship complete production factories. More importantly, a durable source/snapshot
   adapter and safe Podman worker composition are not supplied. `emptyDir`
   workspaces cannot restore Git checkpoints on another pod after node loss.
9. **Static validation — partial.** The validator checks individual YAML documents,
   but not the fully rendered Kustomize graph, exact Secret keys, service/port
   relationships, scrape coverage, or least-privilege policy edges.

### Confirmed gaps

#### P0 — event-gateway privilege is not separated

The event gateway launches the full API factory. It must have a dedicated
application factory, route set, Secret, ServiceAccount, and data-access contract.
An event-only pod must not be able to create runs, approve work, or invoke control
operations.

#### P0 — broad Secret imports defeat provider-key isolation

`envFrom` makes isolation depend on operator discipline instead of a closed
manifest contract. Every workload must use explicit `secretKeyRef` entries from an
allowlisted key set. Provider-shaped names and provider Secret references must be
rejected outside LiteLLM, including in init containers.

#### P0 — custom autoscaling metrics are not discoverable end to end

The API, worker, scheduler, event gateway, and LiteLLM need explicit scrape
metadata (or equivalent monitor resources), matching metrics ports, and monitoring
NetworkPolicy ingress. The rendered validator must prove that every referenced HPA
metric has a declared producer and scrape path.

#### P0 — distributed checkpoint recovery is not deployable

Git checkpoint metadata currently depends on the source repository's common Git
directory. An ephemeral per-pod worktree cannot be restored by a replacement pod.
A production worker requires an object-backed workspace snapshot/checkpoint adapter
with integrity metadata. Until that adapter and a Podman-only worker composition are
configured, worker readiness must fail rather than silently advertise a usable
runtime.

#### P1 — network and scheduling policies are too coarse

Database, queue, object-store, and gateway edges should be role-specific. Explicit
pod anti-affinity should complement topology spread. Workloads should disable
service-link environment injection and explicitly disable host namespaces.

#### P1 — the base image is a template, not an immutable release

Image tags are intentionally replaceable in the base. Production overlays must use
immutable digests and a separately built Podman-capable worker image. This is a
deployment requirement, not evidence that the base can run production tasks.

### Improvements to implement

1. Add an event-only FastAPI factory and a closed event-gateway dependency contract.
2. Replace broad Secret imports with exact environment-key references and expand
   validation to all containers.
3. Add scrape annotations, metrics ports, and monitoring ingress for every metric
   producer; cross-check them against HPA metrics in the rendered graph.
4. Split role-specific data-service and gateway policies, add anti-affinity, and
   harden pod namespace/service-link settings.
5. Restore all stable LiteLLM aliases in the deployment configuration.
6. Add startup validation that makes missing durable-workspace and Podman worker
   composition explicit. Do not introduce host runtime sockets or privileged pods.
7. Validate the rendered Kustomize output, selectors, ports, Secrets, RBAC, HPA
   producers, and policy edges.

## Sequence 30 — autoscaling, graceful drain, and deployment verification

### Step-by-step review

1. **Drain state — pass with a callback gap.** Workers stop accepting new claims and
   expose readiness/drain endpoints. A failed drain callback is remembered as a
   requested drain, causing later requests to appear successful.
2. **Lease behavior — pass at component level.** Active executions can complete and
   leasing stops during drain. The generic executor contract does not create a new
   checkpoint at drain time; recovery depends on earlier durable side-effect
   checkpoints.
3. **Autoscaling simulation — pass as simulation only.** It deterministically
   demonstrates queue-driven replica changes, but it does not prove Kubernetes HPA
   behavior or metrics-adapter availability.
4. **Live deployment path — fail.** The current live verifier is only a protocol and
   factory boundary. There is no concrete bounded Kubernetes driver that observes
   deployments, HPAs, pods, queue state, task terminals, and worker removal.
5. **Evidence richness — partial.** Reports contain tick samples and task totals but
   lack pod identity, HPA desired/current values, exact removal time, queue-metric
   samples, recovery time, and duplicate-side-effect evidence.
6. **Operations HTTP boundary — partial.** It is loopback-only and bounded, but
   duplicate framing headers and transfer encodings are not rejected explicitly.

### Confirmed gaps

#### P0 — live rolling-deployment acceptance cannot be executed

A concrete driver must perform an opt-in live campaign through bounded `kubectl`
calls and a typed task-probe client. It must collect enough evidence to prove worker
scale-up, graceful and abrupt removal, terminal task recovery, and absence of
duplicate commits. Unit tests must use injected fakes and never contact a cluster.

#### P0 — HPA proof depends on missing Sequence 29 metrics plumbing

The live campaign must fail early when required custom metrics are absent. A
simulation cannot substitute for this precondition.

#### P1 — drain failure and evidence semantics are incomplete

Drain callback failures must remain retryable and visible as unhealthy. Reports
must distinguish graceful completion from checkpoint recovery; they must not claim
that an active checkpoint was taken unless a durable checkpoint event was observed.

### Improvements to implement

1. Add a concrete Kubernetes deployment driver with injected bounded command and
   task clients, schema-validated JSON, deadlines, output ceilings, and no shell.
2. Capture deployment/HPA/pod snapshots and timestamped task/queue observations.
3. Require observable recovery evidence for both graceful and abrupt modes.
4. Make drain callback failures retryable and reject ambiguous HTTP framing.
5. Keep simulation reports labeled `sim`; live acceptance accepts only `live`
   reports produced by the concrete driver.

## Sequence 31 — deterministic coding-task evaluation

### Step-by-step review

1. **Corpus size and categories — nominal pass.** Thirty tasks cover ten declared
   families with bounded typed manifests.
2. **Fixture validity — fail.** Nine task families use a placeholder
   `assert True` test. The repository therefore does not actually prove API work,
   type repair, rename, validation, refactor, dependency use, concurrency, missing
   tests, or diagnosis.
3. **Hidden acceptance — fail.** Verification files live inside the writable task
   workspace or are absent. An agent can alter tests, and the missing-tests family
   is not meaningfully distinguished.
4. **Independent mutation evidence — fail.** The driver can report arbitrary
   `changed_paths`, commit counts, and `tests_passed`; the harness does not compare
   pre/post workspace manifests or bind verification commands to evidence.
5. **Sandbox evidence — fail.** A live result does not contain typed proof that each
   configured verification command ran in Podman with a bounded exit result.
6. **Aggregate integrity — partial.** Result schemas are closed, but report
   aggregates and category coverage are not recomputed from the results. A forged
   aggregate can pass validation.
7. **Corpus provenance — partial.** The report records task IDs but not a corpus
   digest, per-fixture digest, or hidden-verifier digest.
8. **Resource limits — pass at harness level.** Task timeout, exception isolation,
   and typed metrics exist. Those controls must be retained while verification is
   strengthened.

### Confirmed gaps

#### P0 — the corpus does not test the advertised coding behaviors

Every category requires an initially failing or objectively deficient fixture, a
meaningful visible test where appropriate, and a hidden immutable verifier outside
the writable workspace. Placeholder assertions must be eliminated.

#### P0 — task outcomes are trusted rather than measured

The harness must independently fingerprint the workspace before and after the
driver, derive changed paths, reject symlinks/special files, and compare the result
with the driver's claim. Completion requires at least one actual change and all
configured verification evidence.

#### P0 — Podman verification evidence is not part of the contract

Each live verification needs exact argv, backend, exit status, duration, timeout,
bounded-output digest, and truncation status. `tests_passed` must be derived from the
verification records and cannot be independently supplied as a trusted boolean.

#### P1 — report and corpus provenance are under-specified

Bind results to task-spec, fixture, and hidden-verifier hashes. Recompute all
aggregates, categories, and task counts during model validation.

### Improvements to implement

1. Replace every placeholder fixture with a deterministic behavioral failure and
   meaningful visible tests.
2. Add hidden immutable verification assets and fingerprints for all categories.
3. Add closed `VerificationResult` records and derive pass/fail from them.
4. Independently calculate pre/post workspace manifests and changed paths.
5. Require exactly one commit and no duplicate side effect for completed tasks.
6. Bind reports to a canonical corpus digest and recompute aggregate metrics.
7. Make the simulation driver apply deterministic reference solutions and run the
   same content-level acceptance checks; label its evidence as simulation.

## Sequence 32 — final benchmark and acceptance compilation

### Step-by-step review

1. **Artifact provenance — pass at a basic level.** The compiler hashes source
   files, validates report schemas, and rejects mixed source revisions.
2. **Quality-gate evidence — fail.** No evidence type records Ruff, mypy, tests,
   pre-commit, audit, lock, build, and diff results, despite the implementation plan
   promising machine-readable quality gates.
3. **Scenario completeness — fail.** A single artifact of each broad kind satisfies
   coverage. The compiler does not require all load scenarios, all chaos scenarios,
   both deployment modes, all coding categories, or the minimum task count.
4. **Load pass semantics — fail.** `succeeded == attempted` is used universally,
   which is incorrect for intentional rate-limit/error campaigns and ignores each
   scenario's expected outcome.
5. **Metric claim coverage — partial.** Final metrics omit several design claims,
   including queue and latency percentiles, token/cost evidence, fallback/routing
   outcomes, sandbox startup/resource data, and per-scenario recovery.
6. **Reproducibility — fail.** Compilation defaults to the current time. Recompiling
   unchanged committed source reports changes the final report digest.
7. **Acceptance matrix — partial.** Broad statuses exist, but they do not enumerate
   the complete final definition of done or link every claim to an exact artifact
   and measurement.
8. **Honest baseline status — pass.** Simulation artifacts are labeled and the
   baseline does not claim live production verification. That distinction must be
   preserved.

### Confirmed gaps

#### P0 — quality and completeness are not evidence-backed

Add a signed-by-content quality report with fixed gate names, argv identity, exit
status, duration, and bounded-output digest. Final coverage must require that report
and the complete expected campaign matrix.

#### P0 — the final status can be satisfied by incomplete campaigns

Coverage must require:

- both graceful and abrupt deployment modes;
- all ten coding categories and at least thirty task results;
- every load scenario required by the design;
- every chaos scenario required by the design;
- every quality gate;
- live evidence for production verification.

Missing or simulation-only evidence must produce `unverified`, never `verified`.

#### P0 — unchanged inputs do not produce unchanged output

Default `generated_at` must be derived deterministically from validated source
artifacts, or supplied explicitly. A compile-only target must reproduce the
committed final report byte-for-byte from unchanged source reports.

### Improvements to implement

1. Add a closed quality-gate report schema and bounded fixed-command recorder.
2. Add `quality` evidence to the final compiler and require all named gates.
3. Validate scenario/mode/category completeness before assigning acceptance status.
4. Derive load outcomes from scenario-specific expected behavior.
5. Build a claim-to-artifact matrix and expose only measured metrics.
6. Make compilation deterministic and separate source-report generation from final
   report compilation.
7. Keep source revision, dirty state, environment, methodology, timestamps, and
   digests explicit. Never upgrade simulation evidence to live proof.

## Ordered implementation plan

### Phase A — contracts that prevent false claims

1. Add exact Sequence 29 Secret/role/metrics contracts and rendered-manifest
   validation.
2. Add Sequence 31 corpus fingerprints, workspace-diff verification, and typed
   command evidence.
3. Add Sequence 32 quality evidence and complete campaign coverage checks.

These changes come first because later reports must not be able to encode success
without the corresponding evidence.

### Phase B — production-boundary hardening

4. Separate the event-gateway application and least-privilege dependencies.
5. Split NetworkPolicies, add scrape discovery, anti-affinity, and pod hardening.
6. Add a concrete, injected Kubernetes live-deployment driver and richer evidence.
7. Correct drain failure/retry behavior and HTTP framing validation.

### Phase C — meaningful evaluation corpus

8. Replace all placeholder task fixtures and add hidden immutable verifiers.
9. Make the reference simulation driver perform actual changes and measured
   verification.
10. Recompute evaluation aggregates and validate category/task completeness.

### Phase D — reproducible acceptance evidence

11. Record fresh quality-gate evidence after the implementation is stable.
12. Regenerate simulation sources, compile the final report deterministically, and
    verify byte-for-byte reproduction.
13. Run all repository gates and inspect the final diff/status.

## Verification plan

### Sequence 29

- Render the Kustomize base and validate every document as a graph.
- Prove the event gateway cannot expose control-plane routes.
- Reject broad Secret imports and provider-shaped keys outside LiteLLM.
- Verify every HPA metric has a scrapeable producer and allowed monitoring ingress.
- Verify service selectors/ports, RBAC subjects, anti-affinity, host namespace
  settings, and role-specific egress edges.
- Prove missing durable-workspace or Podman worker composition fails readiness.

### Sequence 30

- Fake all bounded Kubernetes command responses and malformed/oversized variants.
- Verify scale-up/down, graceful removal, abrupt removal, terminal recovery, and
  duplicate-side-effect checks.
- Verify missing metrics or timeouts fail closed.
- Verify drain callback retry and ambiguous HTTP framing behavior.
- Never contact a real cluster from unit or integration tests.

### Sequence 31

- Assert every fixture begins in its intended failing/deficient state.
- Exercise reference solutions for every category and variant.
- Reject fabricated changed paths, no-op completions, test modification, symlinks,
  special files, mismatched fixture hashes, and missing command evidence.
- Verify bounded multibyte output accounting, timeouts, and cancellation.
- Recompute report aggregates and enforce ten-category/thirty-task coverage.

### Sequence 32

- Reject missing/failed/duplicate quality gates.
- Reject incomplete deployment, coding, load, or chaos matrices.
- Validate rate-limit and fallback scenarios using scenario-aware outcomes.
- Reject mixed revisions, dirty/live ambiguity, bad digests, and absolute artifact
  paths.
- Compile unchanged inputs twice and compare exact bytes.

### Repository-wide gates

- `make test`
- all pre-commit hooks
- dependency audit
- frozen lock verification
- all package builds
- `git diff --check`
- final status and diff inspection

## External blockers and honest remaining limitations

The following cannot be completed or claimed from this local repository alone:

- live Kubernetes HPA and rolling-deployment proof;
- managed PostgreSQL, Redis, and object-store connectivity;
- provider routing/fallback proof with production credentials;
- a production Podman worker image and cluster runtime policy supplied by the
  deployment environment;
- cross-node checkpoint recovery until the durable workspace snapshot adapter is
  configured;
- production load, chaos, cost, and latency measurements.

The implementation will provide fail-closed contracts and live drivers for these
capabilities. Final production status must remain `unverified` until operators run
those drivers in the target environment and compile the resulting live artifacts.

## Assumptions

- “Sequences 29–32” means PRs 29, 30, 31, and 32 from the original sequence plan.
- Existing uncommitted implementation is the baseline and will be preserved unless
  a change is required by this hardening plan.
- Breaking report-schema corrections require no migration because the current
  artifacts are draft/simulation evidence.
- Kubernetes is the deployment target and Podman is the only supported runtime for
  sandbox execution and runtime-local validation.
- No privileged pod, host runtime socket, ambient provider credential, or broad
  Secret import will be introduced to make a local demo appear runnable.
