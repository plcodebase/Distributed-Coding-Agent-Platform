# Reliability model

An accepted run is durable before the API returns. Queue claims use row locking, lease tokens, and
monotonic generations. Workers heartbeat both run and workspace ownership. The scheduler moves an
expired owner through `LOST` and requeues one new attempt; terminal tool results and delivery keys
make retries idempotent.

The production executor composes workspace/run fences, immutable source and checkpoint artifacts,
the normalized transcript journal, plans, summaries, approvals, memories, terminal tool outcomes,
and final-patch persistence. Each checkpoint is keyed to the exact logical tool-call ID, including
multiple mutations emitted in one model turn. Rewind restores both the object-backed workspace
revision and its full normalized message checkpoint. Retry delays and approval waits release worker
capacity and return through the durable queue. A real-PostgreSQL integration journey proves
lost-lease recovery on a replacement worker without repeating a completed mutation. Live recovery
through separate pods, S3-compatible storage, and the mTLS node agent remains a release gate.

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

Redis wake-ups are bounded hints, not durable ownership. A publish failure or Redis outage falls
back to PostgreSQL polling. Context history is also bounded: production atomically schedules
compaction at 3,072 post-watermark messages and still fails closed if more than 4,096 messages are
requested. The original transcript remains append-only.

The event transport has a real-process regression boundary: a fresh uvicorn subprocess is contacted
over TCP with HTTP and WebSocket clients. It proves cursor replay, client-disconnect cleanup,
same-port process restart and resumption, authentication and tenant isolation, body-size rejection,
fail-closed sequence-gap handling, and graceful application shutdown. Target ingress/load-balancer
replacement remains part of the external live acceptance campaign.

The current repository has no live Kubernetes evidence. Horizontal scale, rolling worker loss,
provider traffic, and measured coding performance remain unverified until an external test cluster
and credentials are supplied.
