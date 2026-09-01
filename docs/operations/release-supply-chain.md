# Release supply-chain runbook

The release path builds and mirrors four production images with rootless Podman, generates SPDX
SBOMs, rejects High or Critical vulnerability findings, signs each registry digest, attaches SPDX
and SLSA attestations, signs the complete evidence manifest, and immediately reverifies everything.
It never treats a mutable tag as a promotion input.

## Protected runner prerequisites

Use a dedicated Linux runner labeled `self-hosted`, `linux`, `podman`, `rootless`, and `release`.
Install Git, Podman, Syft, Grype, and Cosign as regular executable files. Record the absolute path and
SHA-256 of each reviewed binary as repository variables:

- `RELEASE_GIT_PATH`, `RELEASE_GIT_SHA256`;
- `RELEASE_PODMAN_PATH`, `RELEASE_PODMAN_SHA256`;
- `RELEASE_SYFT_PATH`, `RELEASE_SYFT_SHA256`;
- `RELEASE_GRYPE_PATH`, `RELEASE_GRYPE_SHA256`;
- `RELEASE_COSIGN_PATH`, `RELEASE_COSIGN_SHA256`.

Also configure:

- `RELEASE_REGISTRY_HOST` and `RELEASE_REGISTRY_PREFIX`;
- `RELEASE_PYTHON_SLIM_IMAGE` and `RELEASE_PYTHON_ALPINE_IMAGE` as complete digest references;
- `RELEASE_LITELLM_IMAGE` as the reviewed upstream digest to mirror;
- `RELEASE_REGISTRY_USERNAME` and `RELEASE_REGISTRY_PASSWORD` as encrypted workflow secrets.

The workflow has `id-token: write` only so Cosign can obtain the short-lived workflow identity. Do
not configure a long-lived signing key on the runner. Protect changes to the workflow, image
inventory, scanner policy, Containerfiles, runner labels, repository variables, and release tags.

## Preflight

From a clean checkout, run:

```console
make release-check
make kubernetes-contract
make release-contract
make release-tool-contract
make podman-preflight
```

`release-tool-contract` requires the same path and checksum variables used by the workflow. The
release contract validates all four image definitions, digest-injectable bases, and exact
deny-first build contexts. The build command repeats that validation internally, so skipping the
standalone preflight does not weaken the release boundary. Grype must update and hash-check its
database, prove it is no more than 48 hours old, and complete its update check. Suppression and VEX
inputs are prohibited in this release gate.

## Build and verification

Push a protected `v*` tag or manually dispatch `release-supply-chain.yml`. The job:

1. reruns repository and Kubernetes gates;
2. verifies every release executable before invoking Podman;
3. authenticates with a temporary registry file;
4. pulls only exact material digests;
5. builds platform, node, and sandbox images and mirrors the exact LiteLLM input;
6. generates and validates SPDX documents;
7. scans with the fixed Grype policy before publishing the affected image;
8. publishes, signs, attests, and verifies each immutable digest;
9. signs and verifies `release-manifest.json`;
10. uploads the bounded evidence directory and removes registry credentials.

An unsuccessful run may have partial, unreachable registry objects. It cannot produce a valid
signed manifest. Never reconstruct a manifest manually and never promote an `INCOMPLETE` evidence
directory.

## Promotion

Download the evidence artifact into a new trusted environment. Set the expected release commit,
exact workflow certificate identity, issuer, Cosign path, and reviewed Cosign checksum, then run:

```console
make release-evidence-verify RELEASE_EVIDENCE_DIR=/path/to/evidence
```

The verifier rehashes every bundled file, rejects a different source revision or Cosign binary,
verifies the signed manifest, verifies each registry digest, decodes both attestation types, and
requires their predicates and subjects to match the local SBOM/provenance and image digest.

Only the four `image_reference` values from the verified manifest may be placed in a site overlay.
The current portable admission policy enforces immutable digests and workload hardening; the site
must additionally install a trusted Sigstore admission policy that requires the exact release
workflow identity plus both SPDX and SLSA attestations before production namespace activation.

Retain the signed evidence, workflow run identity, source tag, scan database metadata, and registry
retention policy for the organization's audit period. Registry garbage collection must preserve
promoted digests and their signature/attestation referrers.
