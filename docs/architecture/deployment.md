# Production deployment guide

The Kubernetes base uses separate Deployments, Services, ServiceAccounts, disruption budgets,
security contexts, topology rules, and network policies for API, event gateway, scheduler, workers,
and LiteLLM, plus a sandbox-node DaemonSet. Workloads have no Kubernetes API permissions and do not
mount service-account tokens.

The event gateway exposes only event replay/streaming plus health and metrics; it has no control-plane
mutation routes. Every Secret value is referenced by exact key and only LiteLLM receives provider
credentials. The base is a reviewed contract, not a universal cluster overlay. Operators must pin
images by digest, provision external Secrets, replace managed-service CIDRs, install a metrics
adapter, and provide OIDC, ingress, private node mTLS, and encrypted object storage. Reviewed
worker/scheduler production composition is built into the platform image and is named directly by
the manifests. Workers are scheduled only to dedicated labeled/tainted nodes.

The checked-in production safety layer adds a deny-mode `ValidatingAdmissionPolicy`. It enforces
digest-pinned pod images, the reviewed non-root/read-only/seccomp contract, the exclusive rootless
Podman socket boundary, and the LiteLLM-only provider Secret boundary. The binding is deliberately
gated by an explicit production namespace label so operators can render and review site values
before admission becomes active.

Release images come only from a verified `agent-release-evidence-v1` manifest. The Podman-native
release workflow builds or mirrors the four production images from exact material digests,
generates SPDX SBOMs, rejects High/Critical scan findings, attaches signed SPDX and SLSA predicates,
and signs the manifest itself. Operators must repeat `make release-evidence-verify` in the promotion
environment and copy only its immutable `image_reference` values into the site overlay. The portable
admission layer checks digest pinning and workload security; the site overlay must add Sigstore
identity and attestation enforcement before namespace activation. See the
[release runbook](../operations/release-supply-chain.md).

Sandbox nodes preprovision a private `/var/lib/agent-platform/workspaces` directory and rootless
Podman socket for UID/GID 1000. The trusted node agent is the only workload that mounts the socket;
workers reach it over mTLS and untrusted containers never receive runtime control. The identical
host/container workspace path is an intentional host-namespace bind-mount requirement.

Rollout order:

1. provision PostgreSQL, Redis, object storage, DNS, TLS, external secrets, and telemetry;
2. run Alembic migrations from a separately authorized one-shot administrative job;
3. deploy LiteLLM and verify routes/fallback without exposing provider credentials;
4. deploy API/event gateway and verify database readiness;
5. verify the rootless Podman node service and deploy the sandbox-node DaemonSet;
6. deploy scheduler, then the minimum three workers;
7. install Prometheus Adapter rules and confirm every HPA metric through the external metrics API;
8. run the live deployment verifier before enabling production traffic;
9. run bounded load, chaos, and coding campaigns and compile the final evidence report.

Before step 1 is accepted, enable managed PostgreSQL PITR and object-store versioning/replication.
Before traffic, restore both into an isolated environment and run `make recovery-verify`; checksum,
tenant-residue, migration, audit, or event divergence is a failed deployment gate. Configure the
retention/cleanup schedule and rehearse the legal-hold and tenant-deletion procedure. Certificate
and credential rotations use overlapping trust followed by rolling drain/restart. See the
[data lifecycle and recovery runbook](../operations/data-lifecycle-and-recovery.md).

Worker termination is cooperative first and lease-recoverable second. The loopback `POST /drain`
endpoint is invoked by `preStop`; readiness immediately fails, new claims stop, and the worker waits
for active attempts. If the grace period expires, the scheduler requeues the expired fenced lease and
another worker restores the latest checkpoint. Stale workers cannot commit through either run or
workspace-writer fences.

NetworkPolicy enforcement depends on the cluster CNI. Managed endpoints outside the cluster need
explicit CIDRs or CNI-specific policies. Public egress belongs only to LiteLLM over TCP 443. Podman
sandboxes remain network-disabled independently of the Kubernetes pod network.
