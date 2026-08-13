# Reliability model

An accepted run is durable before the API returns. Queue claims use row locking, lease tokens, and
monotonic generations. Workers heartbeat both run and workspace ownership. The scheduler moves an
expired owner through `LOST` and requeues one new attempt; terminal tool results and delivery keys
make retries idempotent.

Workspace mutations require an exclusive fenced writer lease and a checkpoint before each side
effect. Recovery restores the selected immutable workspace revision, bounded transcript, plan,
summary, and terminal tool outcomes. Same-ID/same-hash tool calls reuse the terminal outcome;
conflicting IDs fail closed.

Capacity is bounded independently at tenant run, queue, worker run, sandbox, gateway request,
provider request, and provider-token levels. Priority aging prevents starvation. Suspended approval
and retry states release workers and reacquire through the durable queue.

Operational evidence is divided deliberately:

- unit/integration/security tests prove local invariants;
- deterministic deployment/load/chaos/coding simulations prove harness behavior only;
- live reports measure the deployed platform and carry revision, environment/hardware, methodology,
  timestamps, and artifact integrity;
- the final compiler refuses to turn simulation into throughput, latency, cost, scale, recovery, or
  coding-success claims.
- final acceptance also requires complete deployment modes, coding categories, load/chaos scenarios,
  and fixed repository quality gates; report compilation is deterministic for unchanged artifacts.

The current repository has no live Kubernetes evidence. Horizontal scale, rolling worker loss,
provider traffic, and measured coding performance remain unverified until an external test cluster
and credentials are supplied.
