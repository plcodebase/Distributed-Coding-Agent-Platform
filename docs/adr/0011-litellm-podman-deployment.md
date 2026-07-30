# ADR 0011: Central LiteLLM deployment through Podman Compose

- Status: Accepted
- Date: 2026-07-29

## Context

Workers must never receive model-provider credentials or contact providers directly.
Sequence 11 establishes LiteLLM as the central routing boundary and must remain
verifiable without paid provider access.

## Decision

- Run LiteLLM through the pinned Podman Compose deployment, bound to loopback for local
  development. Mount configuration read-only, drop all capabilities, set
  `no-new-privileges`, use a read-only root filesystem, and provide a bounded tmpfs.
- Configure exactly five stable logical routes: `coding-default`, `coding-fast`,
  `coding-strong`, `summarization`, and `code-review`.
- Map production routes across OpenAI and Anthropic environment-backed deployments.
  Provider credentials exist only in the LiteLLM service environment.
- Map local routes across two deterministic OpenAI-compatible fake deployments so route
  and fallback behavior can be tested without provider credentials.
- Put fake deployments exclusively on an internal `llm-upstreams` network. LiteLLM
  joins that network and a dedicated `llm-egress` network; neither gateway component
  shares PostgreSQL/Redis/MinIO's default network, and fake providers cannot reach
  external networks.
- Configure bounded router retries, cooldown, and explicit compatible fallbacks.
  Fake upstream calls use a two-second timeout so an unavailable primary reaches the
  configured secondary within the verification bound.
- Build local fake-provider images with Podman before composition. Do not expose fake
  providers on host ports.
- Permit `LITELLM_IMAGE` injection for release composition. The local default remains a
  versioned tag; release values must be immutable digest references.
- Validate deployment YAML as a closed architectural contract and run an opt-in live
  suite that checks all five routes, disables the primary, proves secondary fallback,
  restores the primary, and cleans up the targeted services.

## Consequences

- Workers need only the platform gateway URL and gateway key; provider keys remain
  centralized.
- The gateway owns provider routing and the baseline fallback policy. Client-side
  idempotency, retry backoff, circuit breaking, and rate limits remain Sequence 13.
- The local fake deployment proves routing semantics, not provider compatibility or
  model quality for real credentials.
