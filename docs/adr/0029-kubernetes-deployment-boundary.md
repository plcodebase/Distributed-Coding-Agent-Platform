# ADR 0029: Kubernetes deployment boundary

- Status: Accepted
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
Operators must supply a safe cluster-specific sandbox composition; the base never grants a host
runtime socket or privileged nested-container access.
