# Benchmark reports

This directory accepts generated JSON reports from `scripts/load_test`. A report is
evidence only when `synthetic` is `false` and
`result_claim` is `measurement`. Simulation reports validate scheduling, aggregation,
timeouts, invariant evaluation, and serialization; they must never be quoted as
platform throughput, latency, or live recovery evidence.

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

Generated JSON reports are intentionally not committed by default.
