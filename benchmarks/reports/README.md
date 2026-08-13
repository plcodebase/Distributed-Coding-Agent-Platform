# Benchmark reports

This directory accepts versioned JSON reports from `scripts.load_test`,
`scripts.chaos_test`, `scripts.deployment_test`, `scripts.coding_evaluation`, and
`scripts.quality_gate_report`.
`scripts.final_benchmark_report` validates and compiles them. A report is performance
evidence only when its live/synthetic fields permit `result_claim=measurement`.
Simulation reports validate scheduling, aggregation, timeouts, invariant evaluation,
and serialization; they must never be quoted as platform throughput, latency, scale,
coding success, or live recovery evidence.

Every report records its exact profile or scenario, preconditions, methodology,
timestamps, operating-system release, architecture, Python version, logical CPU count,
and physical memory when the host exposes it. CI or the operator supplies the source
revision and clean/dirty state through `AGENT_PLATFORM_BENCHMARK_GIT_REVISION` and
`AGENT_PLATFORM_BENCHMARK_GIT_DIRTY`; missing values are recorded explicitly as
unavailable/unknown rather than guessed.

Example deterministic load invocation:

```shell
uv run python -m scripts.load_test \
  --profiles benchmarks/gateway_load/profiles.yaml \
  --profile simulation-10 \
  --mode simulation \
  --report benchmarks/reports/load-simulation.json
```

Live load mode reads credentials only from `AGENT_PLATFORM_LOAD_API_TOKEN`; the API
URL, session ID, and optional streaming run ID use the corresponding
`AGENT_PLATFORM_LOAD_*` environment variables. Accepted submissions are followed
through their durable event stream. Metric summaries include an observation count; a
zero count means the selected interface did not expose that measurement and must not be
interpreted as a measured value of zero.

Chaos reports follow the same evidence rule. A passing simulation proves only that the
runner evaluates the versioned expectations. A passing live scenario must also contain
the measured recovery observation and bounded metric evidence required by that
scenario; successful fault injection alone is insufficient. Generated JSON reports
are normally not committed. The Sequence 32 baseline artifacts are committed because
the final report explicitly labels them `simulation_only`, records their integrity,
and demonstrates that the compiler refuses to fabricate measurements.

`make final-compile` only compiles existing sources and is deterministic: unchanged input bytes
produce unchanged final JSON and Markdown. `make final-baseline` regenerates simulation and quality
sources first, so its timestamps intentionally reflect the new executions. Quality evidence records
fixed argv, exit status, timeout/truncation state, duration, and bounded-output digest; it never
stores raw command output or credentials.
