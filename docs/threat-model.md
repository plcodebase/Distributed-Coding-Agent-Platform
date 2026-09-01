# Initial threat model

This document records the trust boundaries implemented through the production-composition closure
and separates tested controls from live deployment evidence.

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
- Images and dependencies use explicit versions. Production configuration requires the sandbox
  image digest, and the release overlay must pin every workload image by digest.
- Repository paths are descriptor-contained, reject symlink/traversal escapes, and deny
  repository metadata plus a conservative sensitive-path set.
- Model-generated arguments cross closed typed schemas before any tool executes.
  Tool output, results, structured errors, and model feedback cross bounded redaction
  boundaries. Model-text redaction retains a bounded suffix across provider fragments so
  configured and patterned credentials cannot evade matching by splitting across deltas.
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
- Immutable source archives omit repository metadata and protected paths before untrusted commands
  can access the workspace.
- Workers reach the sandbox node over private mutual TLS. Only the trusted node-agent DaemonSet
  mounts the rootless Podman socket; workers and sandbox containers never receive it.
- Durable tenant-scoped state changes use run/workspace generations, idempotency keys, and immutable
  object checksums. Every mutation checkpoint retains the exact logical tool-call ID. Redis is a
  lossy wake-up hint; publication failure degrades to PostgreSQL polling and cannot make an accepted
  task disappear.
- Tenant lifecycle state is checked after authentication by both control and event planes. Legal
  holds block retention and deletion; export-first deletion uses checksum-verified audit evidence,
  cooling-off, a fenced deletion outbox, and a retained tombstone. An isolated-restore verifier
  checks migration, event, tenant-residue, and object-integrity invariants without mutating data.

## Residual risk and deferred controls

- Rootless Podman plus the default seccomp profile is the current production isolation
  boundary; a stronger optional runtime such as gVisor is deferred.
- A hostile process with equivalent host-user privileges is outside the container
  boundary. Worktrees therefore require exclusive platform ownership.
- Pattern and path-based secret controls reduce exposure but cannot identify every
  unknown credential embedded in an otherwise legitimate source file.
- Production composition connects gateway policies, quotas, persistence, leases, immutable
  artifacts, approvals, checkpoints, Kubernetes policies, and the node boundary, but the complete
  path has not been exercised in a live distributed deployment.
- The node agent expands the trusted computing base. Compromise of the node-agent identity or the
  UID that owns the rootless Podman service can control that user's containers and workspaces.
- CNI policy, kernel namespaces, cgroups, seccomp, rootless storage, and mTLS behavior vary by target
  cluster and must pass the security campaign on every supported node image.
- Lifecycle, retention, tenant deletion, audit export, restore verification, supply-chain scanning,
  and portable admission contracts are implemented and component-tested. Target managed backups,
  object replication/versioning, KMS and certificate/key rotation, site signature admission, and a
  retained recovery rehearsal remain deployment work.

The project does not claim a formal proof of isolation. It claims only the controls
covered by the executable tests above.
