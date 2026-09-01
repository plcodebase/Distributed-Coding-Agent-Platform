# Kubernetes deployment

The base Kustomize deployment contains five Deployments—Agent API, event gateway, scheduler,
agent worker, and LiteLLM—and one sandbox-node DaemonSet. PostgreSQL, Redis, object storage, OIDC,
and the rootless Podman node service are managed dependencies and are not installed by these
manifests.

## Required production overlay

Do not apply the base unchanged. A production overlay must:

1. replace the platform, node-agent, LiteLLM, and sandbox images with immutable registry digests;
2. replace the documentation-only `192.0.2.0/24` managed-data CIDR;
3. provision the externally managed Secrets described below;
4. replace the documentation OIDC issuer/JWKS URLs and provide ingress certificates;
5. install and configure a Prometheus Adapter using the supplied external metric rules;
6. label and taint dedicated worker nodes with
   `agent-platform.openai.com/sandbox-worker=true`;
7. label only the ingress and monitoring namespaces authorized by the NetworkPolicies;
8. preprovision `/var/lib/agent-platform/workspaces` on sandbox nodes as a private directory owned by
   UID/GID 1000 and start the rootless Podman service socket at
   `/run/user/1000/podman/podman.sock`;
9. provision separate worker-client and node-server certificates signed by the private node CA;
10. configure encrypted object storage and database backup/restore policy.

The repository's `production/` layer installs a fail-closed admission policy for the portable
invariants above. It becomes active only after the target namespace receives the explicit
`agent-platform.openai.com/production=true` label. A site overlay still supplies all
organization-specific values; the repository cannot safely guess them.

The reviewed composition roots are fixed in the manifests:
`agent_worker.production:create_production_worker` and
`agent_scheduler.production:create_production_scheduler`. They fail closed when durable stores,
exact route pricing/context budgets, mTLS, or the node boundary are unavailable. Each pod UID is
part of its worker/scheduler identity, preventing replica collisions.

Only the trusted node-agent DaemonSet mounts the rootless Podman service socket. The worker and every
untrusted sandbox do not. The worktree path is deliberately identical in host and node-agent mount
namespaces (`/var/lib/agent-platform/workspaces`) because Podman resolves bind sources on the host.

## Secret boundary

Secrets are created by an external secret manager or the cluster operator and are never committed.
The manifests expect:

| Name | Workload | Required contents |
|---|---|---|
| `agent-platform-api-runtime` | API | `database-url`, `redis-url`, `s3-endpoint`, `s3-access-key`, `s3-secret-key` |
| `agent-platform-event-runtime` | event gateway | `database-url` |
| `agent-platform-scheduler-runtime` | scheduler | `database-url`, `redis-url`, `s3-endpoint`, `s3-access-key`, `s3-secret-key`, `gateway-api-key`, `route-prices-json` |
| `agent-platform-worker-runtime` | worker | `database-url`, `redis-url`, `gateway-api-key`, `route-prices-json`, `route-context-budgets-json` |
| `agent-platform-node-runtime` | node agent | `database-url`, `s3-endpoint`, `s3-access-key`, `s3-secret-key`, `sandbox-image-digest` |
| `agent-platform-worker-node-tls` | worker | private CA, client certificate, and client key (`ca.crt`, `tls.crt`, `tls.key`) |
| `agent-platform-node-tls` | node agent | private CA, server certificate, and server key (`ca.crt`, `tls.crt`, `tls.key`) |
| `agent-platform-provider` | LiteLLM only | LiteLLM master key, provider API keys, provider model names |

Provider API keys must never appear in the API, scheduler, worker, sandbox, ConfigMap, image, or
benchmark environment. Every Secret value is mounted through an exact `secretKeyRef`; broad Secret
imports are rejected. LiteLLM alone receives provider keys. Workers receive only the gateway key.

## Build and validate with Podman

```console
podman build -f services/platform/Containerfile -t registry.example/agent-platform:<revision> .
podman build -f services/node/Containerfile -t registry.example/agent-platform-node:<revision> .
podman push registry.example/agent-platform:<revision>
podman push registry.example/agent-platform-node:<revision>
python -m scripts.kubernetes_contracts deployments/kubernetes/base
kubectl kustomize deployments/kubernetes/base > /tmp/agent-platform.yaml
```

The last command is an optional rendering check. The repository test suite parses and validates the
base without requiring `kubectl` or a cluster. Do not treat static rendering or the deterministic
deployment simulation as live scaling evidence.

## Autoscaling and termination

- API scales from CPU and request rate.
- Workers scale from total queue depth and oldest queued age.
- LiteLLM scales from active requests and p95 upstream latency.
- Worker `preStop` calls the loopback-only drain operation from inside the pod. Readiness drops before
  the callback signals the service, new claims stop, durable worker state becomes draining, and active
  work is given 180 seconds. A forced exit is recovered through lease expiry and checkpoints.
- HPAs depend on the cluster's metrics pipeline. Applying their YAML does not prove that the external
  metrics API is populated.

Run the offline state-machine deployment test with:

```console
python -m scripts.deployment_test \
  --mode simulation \
  --output /tmp/deployment-simulation.json
```

For acceptance, supply a trusted live driver that creates real tasks, observes HPA replicas, removes
an active worker, and reads durable outcomes. `scripts.kubernetes_deployment_driver` supplies the
bounded Kubernetes half of that driver and requires an injected authenticated task probe. Live
drivers must provide cluster UID, Kubernetes
version, and node/hardware summary before the report schema permits a measurement claim.
