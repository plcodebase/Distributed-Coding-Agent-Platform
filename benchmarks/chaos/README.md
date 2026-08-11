# Chaos scenarios

`scenarios.yaml` is the versioned contract for the ten failure cases in the design.
Every case defines its fault, preconditions, recovery deadline, accepted-task and
single-commit expectations, durable-event requirement, allowed final states, and
required observable signals.

Run the deterministic harness check with:

```shell
uv run python -m scripts.chaos_test \
  --scenarios benchmarks/chaos/scenarios.yaml \
  --mode simulation \
  --report benchmarks/reports/chaos-simulation.json
```

Simulation reports validate orchestration and invariant evaluation only. They are
always labelled `simulation_only` and are not evidence that a live fault recovered.

Live service faults are composed in Python from `PodmanServiceInjector`,
`PodmanServiceChaosDriver`, and an injected `ChaosObservationProbe`. The operator must
provide an absolute Podman executable and an explicit mapping from a supported scenario
to one exact container identifier. The adapter does not perform container discovery,
does not use a service socket, never enables privileged execution, and refuses
non-service fault scenarios. A live probe must measure run/event state and metric
evidence; successful fault-command execution by itself cannot produce a passing report.

Live chaos runs belong only on an isolated test deployment. The four supported Podman
service faults are worker termination, Redis restart, PostgreSQL interruption, and
primary-provider disablement. Gateway responses, WebSocket disconnects, sandbox OOM,
duplicate deliveries, and checkpoint interruption are injected at their typed platform
test seams rather than through broad container operations.
