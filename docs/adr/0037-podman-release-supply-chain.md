# ADR 0037: Podman-native release supply chain

## Status

Accepted.

## Context

The design requires production images to be digest-pinned and image scanning to run in CI. The
existing dependency audit and source-filesystem scan did not prove the contents of the images that
would be promoted. Production builds also inherited mutable base-image references and ambient
scanner configuration.

## Decision

- Version one closed inventory for the four deployable image identities: platform, node, sandbox,
  and a locally mirrored LiteLLM image. The fake model remains test-only and is not promoted.
- Build only with rootless Podman. Production base and mirror inputs must be complete registry
  SHA-256 digest references. Containerfiles keep development defaults but accept their production
  base through explicit build arguments; release validation rejects every non-injected `FROM`.
- Use exact, deny-first `.containerignore` allowlists. The build operation itself reruns inventory,
  scanner-policy, Containerfile, and context validation instead of relying on an earlier CI step.
  Credentials, local environments, caches, Git metadata, and unrelated repository files never
  enter a build context.
- Require a clean source revision before and after the build. Resolve Git, Podman, Syft, Grype, and
  Cosign to regular executables and compare each binary with a separately configured SHA-256 before
  it is used.
- Produce an SPDX 2.x JSON SBOM from a temporary OCI archive. Scan that SBOM with Grype and reject
  High or Critical findings. Grype uses a checked-in explicit policy: no ignored matches, excludes,
  VEX, or fix-state suppression; database update checks and hash/age validation are mandatory and
  the database may be at most 48 hours old. Temporary image archives are removed after SBOM
  generation rather than uploaded as release artifacts.
- Inspect the local image identity before scanning and again before publication. Publish once, use
  only the returned registry digest as the release identity, and never place a mutable tag in the
  evidence manifest or production overlay.
- Sign every digest through the workflow OIDC identity. Attach the exact SPDX document and SLSA
  v0.2 build predicate, verify their signatures and decoded predicate/subject content, and sign the
  complete release manifest as a blob. Promotion repeats local checksum validation and remote
  signature/attestation verification against an operator-supplied exact identity and issuer.
- Run the release workflow without cancellable concurrency on a protected rootless-Podman runner.
  Registry credentials use a temporary authentication file and are not part of persisted evidence.

## Consequences

- A failed scanner, stale database, changed source tree, untrusted executable, mutable material,
  malformed output, missing signature, wrong workflow identity, or predicate mismatch stops the
  release and leaves an `INCOMPLETE` marker. A partial registry upload has no signed release manifest
  and must never be promoted.
- The repository provides the pipeline and deterministic contract tests, but production proof still
  requires configured registry credentials, exact material digests, trusted executable hashes, an
  OIDC-capable release run, retained evidence, and admission enforcement in the site cluster.
- Tool updates are explicit trust changes: update the protected runner, review the release, and then
  rotate the configured executable checksum.
