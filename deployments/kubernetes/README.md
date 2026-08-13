# Kubernetes deployment

The base Kustomize deployment contains five independently scalable workloads: Agent API, event
gateway, scheduler, agent worker, and LiteLLM. PostgreSQL, Redis, and object storage are managed
dependencies and are not installed into the cluster by these manifests.

## Required production overlay

Do not apply the base unchanged. A production overlay must:

1. replace `agent-platform:0.1.0` and the LiteLLM image with immutable registry digests;
2. replace the documentation-only `192.0.2.0/24` managed-data CIDR;
3. provision the externally managed Secrets described below;
4. provide trusted `deployment_composition:create_worker` and `create_scheduler` factories in the
   application image;
5. install and configure a Prometheus Adapter using the supplied external metric rules;
6. label and taint dedicated worker nodes with
   `agent-platform.openai.com/sandbox-worker=true`;
7. label only the ingress and monitoring namespaces authorized by the NetworkPolicies;
8. configure an object-backed workspace snapshot/checkpoint adapter so a replacement pod can
   restore another pod's attempt;
9. configure a Podman-backed sandbox adapter appropriate to the cluster. The base does not grant a
   worker a host runtime socket or privileged nested-container access.

Those composition requirements are fail-closed deployment prerequisites: the repository
intentionally keeps application
composition injected, and an unprivileged Kubernetes pod cannot safely inherit a node-wide Podman
control socket. A cluster-specific adapter must preserve the Sequence 9 containment contract.
The base's `emptyDir` is scratch capacity, not durable checkpoint storage. Until the worker factory
validates both the snapshot adapter and the cluster Podman boundary, the worker must remain unready.

## Secret boundary

Secrets are created by an external secret manager or the cluster operator and are never committed.
The manifests expect:

| Name | Workload | Required contents |
|---|---|---|
| `agent-platform-api-runtime` | API | `api-credentials-json`, `database-url` |
| `agent-platform-event-runtime` | event gateway | `event-credentials-json`, `database-url` |
| `agent-platform-scheduler-runtime` | scheduler | `database-url`, `redis-url`, `scheduler-factory` |
| `agent-platform-worker-runtime` | worker | `database-url`, `redis-url`, object-store URL/credentials, `gateway-api-key`, `workspace-snapshot-url`, `worker-factory` |
| `agent-platform-provider` | LiteLLM only | LiteLLM master key, provider API keys, provider model names |

Provider API keys must never appear in the API, scheduler, worker, sandbox, ConfigMap, image, or
benchmark environment. Every Secret value is mounted through an exact `secretKeyRef`; broad Secret
imports are rejected. LiteLLM alone receives provider keys. Workers receive only the gateway key.

## Build and validate with Podman

```console
podman build -f services/platform/Containerfile -t registry.example/agent-platform:<revision> .
podman push registry.example/agent-platform:<revision>
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
