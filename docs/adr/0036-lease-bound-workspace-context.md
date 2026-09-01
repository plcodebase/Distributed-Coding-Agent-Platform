# ADR 0036: Lease-bound production workspace context

## Status

Accepted

## Context

The Phase 8 context pipeline had typed contributors for project instructions, explicitly referenced
files, and the current Git diff, but the production worker supplied empty values. Loading those
sources directly from the worker host would bypass the workspace capability and containment policy.
Using final-patch export for current context would also mutate Git state by creating a commit.

## Decision

- Run submissions accept at most 32 canonical workspace-relative file references. References are
  part of the idempotency hash and are stored in the immutable initial-message metadata. Runs
  created before this contract contribute no explicit references and require no schema migration.
- The worker loads workspace context only after its loop factory has created the active sandbox.
  Tenant, run, workspace, and run-lease tokens must match the current writer lease.
- Project instructions come from an ordered, composition-time list of at most eight files. Missing
  instruction files are ignored; other read failures fail closed. Explicit references are strict:
  missing, protected, oversized, or non-UTF-8 files fail the context build.
- The worker-to-node channel uses the sandbox's opaque capability over mTLS. File responses and the
  current patch carry bounded sizes and SHA-256 checksums. Workers never read the node worktree or
  receive its rootless Podman socket.
- Context patches compare the worktree against its immutable baseline without staging or committing.
  Changed paths are enumerated under count and byte ceilings. Protected paths are omitted before Git
  emits patch content. Tracked changes are included; untracked file names are listed while their
  contents remain omitted from automatic context.
- Project instructions, referenced content, and patch text must be strict UTF-8, recursively pass
  through the platform redactor, satisfy per-source and aggregate byte limits, and still fit the
  route's normal context budget. The existing contributor priority and compaction policy remains the
  sole selection mechanism.

## Consequences

Production context now implements every source named by Phase 8 without adding a second filesystem
or orchestration boundary. Explicit reference order is meaningful and therefore affects request
idempotency. Arbitrary untracked contents are not injected automatically; the agent can inspect an
allowed file through validated tools or the caller can reference it on a later run once it exists.
Protected-path filtering and redaction are defenses in depth, not a claim that arbitrary repository
content can always be recognized as a secret.
