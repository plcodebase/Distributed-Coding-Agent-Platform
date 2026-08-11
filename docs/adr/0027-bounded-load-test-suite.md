# ADR 0027: Bounded load-test suite

- Status: Accepted
- Date: 2026-08-10
- Sequence: 27

## Context

The platform needs reproducible evidence for concurrency and backpressure behavior.
Ad-hoc client scripts can create one task per operation, run forever, leak credentials
into failures, or report synthetic timings as measured platform performance. Such
results cannot support the design's throughput or latency claims.

## Decision

- Use versioned, closed load profiles with explicit operation, concurrency, timeout,
  event, and report bounds. Profiles cover API submission, WebSocket connections,
  event throughput, worker and sandbox saturation, gateway limits, provider fallback,
  and PostgreSQL and Redis contention.
- Schedule operations through a fixed number of worker tasks. Every operation produces
  one validated sample or an opaque timeout/driver-error sample.
- Record throughput, error rate, p50/p95/p99 total latency, queue wait, first output,
  task/test outcomes, iterations, tool calls, tokens, cost, retries, fallbacks,
  permission denials, event counts, process CPU, peak resident memory, and hardware
  metadata. Every summary carries an observation count; data that the selected live
  interface does not expose is unobserved, never serialized as a fabricated zero.
- Keep deterministic CI simulation and live measurement adapters separate. Reports
  carry both `synthetic` and `result_claim`; simulation output is never performance
  evidence.
- Give every live driver instance a validated campaign identity and include it in every
  API idempotency key and report. Operation numbers are unique only inside that
  campaign and therefore cannot replay an earlier benchmark accidentally.
- Read live credentials only from runtime environment variables and serialize no
  credential or exception text. Write validated JSON reports atomically with a 16 MiB
  ceiling.
- Accept only an HTTP(S) origin without credentials, path, query, or fragment. Consume
  successful HTTP bodies incrementally under a 1 MiB limit and cap cumulative
  WebSocket event bytes at 16 MiB in addition to the event-count ceiling.
- Treat database contention, provider failure, and saturation as test-deployment
  preconditions controlled outside the client. The runner observes the result but does
  not silently mutate shared infrastructure.
- Follow every accepted API submission through its bounded durable WebSocket event
  stream. API acceptance is not task success; terminal failure, event gaps, and event
  ceilings are load errors. Queue wait, first model output, retries, tools, permission
  denials, and total run latency come from validated durable event timestamps.
- Require streams requested from `after=0` to begin at sequence 1 and remain contiguous.
  Keep connection success distinct from terminal task success, but never count a
  terminal failed run as a successful stream operation.
- Label CPU and resident-memory observations explicitly as
  `load_generator_process`; deployment-wide utilization remains an external metrics
  correlation task rather than a claim made by the client process.

## Consequences

Harness scheduling, aggregation, and failure accounting can be verified in CI without
fabricated platform claims. Live reports are comparable because their profile,
methodology, timestamps, and hardware are embedded in the artifact. Demonstrating
horizontal scaling still requires a live deployment and is completed by the deployment
and final benchmark sequences.
