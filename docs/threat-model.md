# Initial threat model

This document records sequence-1 trust boundaries. It will be expanded with executable
sandbox tests when the Podman runtime is implemented.

## Protected assets

- provider, database, object-storage, identity, and gateway credentials;
- tenant source code and generated patches;
- durable session, run, approval, lease, and audit state;
- worker and control-plane availability.

## Trust boundaries

- Clients are untrusted and authenticated before a tenant identity is accepted.
- Model output is untrusted and must pass strict schema and policy validation.
- Repository content and commands are untrusted and later execute only in sandboxes.
- Workers are trusted platform components but never receive provider credentials.
- LiteLLM is the only component allowed to receive provider credentials or contact
  model providers.

## Sequence-1 controls

- Settings use secret-aware types and logs redact known and patterned credentials.
- Compose ports bind to loopback and sample values are explicitly non-production.
- Images and dependencies use explicit versions; release images will additionally be
  pinned by digest once the container build pipeline is available.

The project does not claim a formal isolation guarantee.
