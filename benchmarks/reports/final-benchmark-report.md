# Final benchmark and acceptance report

Status: **incomplete**
Source revision: `unavailable`
Generated: `2026-08-13T19:42:45.935039+00:00`

## Methodology

- Validate each input with its versioned closed report schema.
- Require one source revision and retain the SHA-256 digest of every artifact.
- Use simulation only for deterministic contract evidence and correctness checks.
- Admit performance, capacity, latency, cost, and success-rate metrics only from live evidence.

## Acceptance

| Requirement | Status | Evidence |
|---|---|---|
| queue_scale_and_worker_recovery | verified_simulation | deployment-baseline-deployment |
| coding_task_campaign | verified_simulation | coding-baseline-coding |
| bounded_load_campaign | verified_simulation | load-simulation-api-50, load-simulation-websocket-50, load-simulation-event-throughput-50, load-simulation-worker-saturation-100, load-simulation-gateway-rate-limit-50, load-simulation-provider-fallback-50, load-simulation-postgres-contention-50, load-simulation-redis-contention-50, load-simulation-sandbox-saturation-50 |
| failure_recovery_campaign | verified_simulation | chaos-d96748bf3f07 |
| repository_quality_gates | verified | quality-45ab34814d35 |
| production_measurement_coverage | unverified | quality-45ab34814d35 |

## Measured metrics

No production measurements were admitted; supplied evidence is simulation-only.

## Environment and hardware

- deployment-baseline-deployment: system=Darwin, release=25.5.0, machine=arm64, python=3.12.13
- coding-baseline-coding: system=Darwin, release=25.5.0, machine=arm64, python=3.12.13, logical_cpus=8
- load-simulation-api-50: system=Darwin, release=25.5.0, machine=arm64, python=3.12.13, logical_cpus=8, physical_memory_bytes=34359738368
- load-simulation-websocket-50: system=Darwin, release=25.5.0, machine=arm64, python=3.12.13, logical_cpus=8, physical_memory_bytes=34359738368
- load-simulation-event-throughput-50: system=Darwin, release=25.5.0, machine=arm64, python=3.12.13, logical_cpus=8, physical_memory_bytes=34359738368
- load-simulation-worker-saturation-100: system=Darwin, release=25.5.0, machine=arm64, python=3.12.13, logical_cpus=8, physical_memory_bytes=34359738368
- load-simulation-gateway-rate-limit-50: system=Darwin, release=25.5.0, machine=arm64, python=3.12.13, logical_cpus=8, physical_memory_bytes=34359738368
- load-simulation-provider-fallback-50: system=Darwin, release=25.5.0, machine=arm64, python=3.12.13, logical_cpus=8, physical_memory_bytes=34359738368
- load-simulation-postgres-contention-50: system=Darwin, release=25.5.0, machine=arm64, python=3.12.13, logical_cpus=8, physical_memory_bytes=34359738368
- load-simulation-redis-contention-50: system=Darwin, release=25.5.0, machine=arm64, python=3.12.13, logical_cpus=8, physical_memory_bytes=34359738368
- load-simulation-sandbox-saturation-50: system=Darwin, release=25.5.0, machine=arm64, python=3.12.13, logical_cpus=8, physical_memory_bytes=34359738368
- chaos-d96748bf3f07: system=Darwin, release=25.5.0, machine=arm64, python=3.12.13, logical_cpus=8, physical_memory_bytes=34359738368
- quality-45ab34814d35: system=Darwin, release=25.5.0, machine=arm64, python=3.12.13

## Limitations

- Simulation-only evidence cannot support production performance claims: chaos, coding, deployment, load.
- The evidence set is not bound to a committed source revision.
- A live Kubernetes cluster, external metrics adapter, managed data services, and real provider traffic are required to complete production acceptance.

## Artifact integrity

- `benchmarks/reports/deployment-simulation.json` — `de46f50ff2d6230f9c4ccbd3555b3a65df62909a98c89c05d779823763e6c6c5` (simulation)
- `benchmarks/reports/coding-simulation.json` — `f57e5d39d8818af572ee265f3fed580457362ade43656cd42ca81c684263af8b` (simulation)
- `benchmarks/reports/load-simulation-api.json` — `2430c160d2db348ba720a1582df1480e662dbf0d29d7b9d0b533684f3ec1e9aa` (simulation)
- `benchmarks/reports/load-simulation-websocket.json` — `0c3d20f2422a6b70831fb891e4dd11518bd9c448e99ab2a080cc330cfc01c059` (simulation)
- `benchmarks/reports/load-simulation-events.json` — `467d2a67f12e6dbebcb676df9adc196347a8f77c645db9cd268f03709870f78a` (simulation)
- `benchmarks/reports/load-simulation-workers.json` — `ff84a49c5b5b607a9cce9237300d8d06d628cdfa47cacb4ec8a2c17b02bc20e7` (simulation)
- `benchmarks/reports/load-simulation-rate-limit.json` — `1ca7b9a4488fec59de2f67fc87d837da59e55442787f23129a9c8197d25def12` (simulation)
- `benchmarks/reports/load-simulation-fallback.json` — `226013aa47313b8982afe4f6327a2128263512a40e8fa5dc10c3c06209e389be` (simulation)
- `benchmarks/reports/load-simulation-postgres.json` — `b225bb049edbf5c28aa818e5499c7f7d702fd05c1e610da5c24f71859e39d641` (simulation)
- `benchmarks/reports/load-simulation-redis.json` — `fa5482d8a202ddeb27cef2ffbd467cd84212bdb2578c26751a8109a1489a41ff` (simulation)
- `benchmarks/reports/load-simulation-sandbox.json` — `f652859085453b860b259d79befc406cbb0cebdf36c91ac6ce5a21725ac35a97` (simulation)
- `benchmarks/reports/chaos-simulation.json` — `d96748bf3f07c213d4fc5033a87c5167f5f63be86368728987eefc94db4c84a3` (simulation)
- `benchmarks/reports/quality-gates.json` — `45ab34814d3559b03069fc035799388081ce646888406ba8a550a1a077fd801e` (live)
