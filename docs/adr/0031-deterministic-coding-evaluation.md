# ADR 0031: Deterministic coding evaluation

- Status: Accepted
- Date: 2026-08-11
- Sequence: 31

## Context

Coding success is meaningless without a stable corpus, explicit verification, complete failure
accounting, and model/token/cost/latency metadata.

## Decision

- Version 30 tasks: three variants across each of the ten design categories.
- Require every fixture to begin with a meaningful behavioral, typing, concurrency, validation, or
  coverage deficiency; placeholder assertions are prohibited.
- Materialize bounded fixtures privately and reject traversal, duplicate IDs, missing categories, and
  malformed commands.
- Keep hidden reference verifiers outside the writable task workspace and bind their digest, the
  fixture digest, and the task-manifest digest into each result and the corpus digest.
- Delegate platform submission and sandboxed verification to a trusted injected driver. Every live
  verification identifies the Podman backend, exact configured argv, exit status,
  timeout/truncation state, duration, and bounded-output digest. Never run model-authored commands
  in the host harness.
- Independently fingerprint the workspace before and after execution and derive changed paths. A
  driver's claimed paths or passing boolean cannot substitute for observed mutations and command
  evidence.
- Record outcome, tests, iterations, tools, tokens, cost, queue/first-token/total latency, retries,
  fallbacks, denials, changed paths, and committed changes for every task.
- Keep deterministic CI simulation permanently labeled `simulation_only`; live reports require a
  deployment identity.

## Consequences

The corpus and accounting pipeline are reproducible and aggregate fields are recomputed from task
results. A measured success rate requires executing the campaign against a real deployed agent.
