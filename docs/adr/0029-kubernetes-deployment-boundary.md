# ADR 0029: Kubernetes deployment boundary

- Status: Accepted; amended by ADR 0033
- Date: 2026-08-11
- Sequence: 29

## Context

The five platform services need independent scaling and stronger identity/network defaults without
moving provider credentials into workers or silently deploying stateful dependencies as development
containers.

## Decision

- Use inspectable Kustomize base manifests for API, event gateway, scheduler, workers, and LiteLLM.
- Keep PostgreSQL, Redis, object storage, credentials, ingress, and TLS externally managed.
- Give every workload a separate token-free ServiceAccount with an empty RBAC Role.
- Require non-root, read-only, seccomp, dropped-capability, resource, probe, disruption, and topology
  policies. Workers target dedicated nodes.
- Default deny ingress and egress. Only LiteLLM receives provider credentials and public provider
  egress. Managed-service CIDRs must be set by an overlay.
- Validate the static contract in CI. Static validation is not live deployment evidence.

## Consequences

The deployment security intent is reviewable without a cluster or Kubernetes client dependency.
ADR 0033 supplies the reviewed sandbox composition. The trusted sandbox-node DaemonSet mounts only
the rootless Podman service socket and exposes a private mTLS API; general workers and untrusted
sandboxes receive neither a host runtime socket nor privileged nested-container access.
