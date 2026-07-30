# Initial threat model

This document records the trust boundaries implemented through Sequence 12.

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

## Implemented controls

- Settings use secret-aware types and logs redact known and patterned credentials.
- Compose ports bind to loopback and sample values are explicitly non-production.
- Images and dependencies use explicit versions; release images will additionally be
  pinned by digest once the container build pipeline is available.
- Repository paths are descriptor-contained, reject symlink/traversal escapes, and deny
  repository metadata plus a conservative sensitive-path set.
- Model-generated arguments cross closed typed schemas before any tool executes.
  Tool output, results, structured errors, and model feedback cross bounded redaction
  boundaries.
- Production commands run in a verified rootless Podman engine with a non-root user,
  dropped capabilities, `no-new-privileges`, read-only root, offline networking, a
  bounded tmpfs, and only the isolated worktree mounted writable.
- CPU, memory/swap, PID, open-file, duration, and output limits are explicit and tested
  against the real runtime. Cancellation and destroy force-remove targeted containers.
- Host credentials, workspace symlink escapes, root writes, external network access,
  and Podman socket access are exercised as negative security tests.
- Provider credentials are scoped only to the LiteLLM service. Fake providers and
  workers receive no OpenAI or Anthropic keys.
- Gateway requests require tenant, session, run, turn, model-call, and request
  identifiers. Routes and normalized stream events are allowlisted, validated, and
  cumulatively bounded.

## Residual risk and deferred controls

- Rootless Podman plus the default seccomp profile is the current production isolation
  boundary; a stronger optional runtime such as gVisor is deferred.
- A hostile process with equivalent host-user privileges is outside the container
  boundary. Worktrees therefore require exclusive platform ownership.
- Pattern and path-based secret controls reduce exposure but cannot identify every
  unknown credential embedded in an otherwise legitimate source file.
- Durable gateway request idempotency, client retry/backoff, circuit breakers, tenant
  quotas, persistence, worker leases, Kubernetes policies, and chaos testing belong to
  later sequences.

The project does not claim a formal proof of isolation. It claims only the controls
covered by the executable tests above.
