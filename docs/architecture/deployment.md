# Production deployment guide

The Kubernetes base uses separate Deployments, Services, ServiceAccounts, disruption budgets,
security contexts, topology rules, and network policies for API, event gateway, scheduler, workers,
and LiteLLM. Workloads have no Kubernetes API permissions and do not mount service-account tokens.

The event gateway exposes only event replay/streaming plus health and metrics; it has no control-plane
mutation routes. Every Secret value is referenced by exact key and only LiteLLM receives provider
credentials. The base is a reviewed contract, not a universal cluster overlay. Operators must pin images by
digest, provision external Secrets, replace managed-service CIDRs, install a metrics adapter, provide
ingress/TLS, and implement the injected worker/scheduler composition. Worker composition must
validate a Podman-only sandbox boundary and object-backed workspace snapshot/checkpoint adapter
before becoming ready. Workers are scheduled only to dedicated labeled/tainted nodes.

Rollout order:

1. provision PostgreSQL, Redis, object storage, DNS, TLS, external secrets, and telemetry;
2. run Alembic migrations from a separately authorized one-shot administrative job;
3. deploy LiteLLM and verify routes/fallback without exposing provider credentials;
4. deploy API/event gateway and verify database readiness;
5. deploy scheduler, then the minimum three workers;
6. install Prometheus Adapter rules and confirm every HPA metric through the external metrics API;
7. run the live deployment verifier before enabling production traffic;
8. run bounded load, chaos, and coding campaigns and compile the final evidence report.

Worker termination is cooperative first and lease-recoverable second. The loopback `POST /drain`
endpoint is invoked by `preStop`; readiness immediately fails, new claims stop, and the worker waits
for active attempts. If the grace period expires, the scheduler requeues the expired fenced lease and
another worker restores the latest checkpoint. Stale workers cannot commit through either run or
workspace-writer fences.

NetworkPolicy enforcement depends on the cluster CNI. Managed endpoints outside the cluster need
explicit CIDRs or CNI-specific policies. Public egress belongs only to LiteLLM over TCP 443. Podman
sandboxes remain network-disabled independently of the Kubernetes pod network.
