# ADR 0007: Reliable Git workspaces and checkpoint compatibility

- Status: Accepted
- Date: 2026-07-28
- Hardened: 2026-07-29

## Context

Sequences 6 and 7 require the agent to work from the user's current repository state,
including staged, unstaged, and untracked files, without mutating the source checkout.
Repository data may be private, source state may change during capture, and repository
Git configuration is untrusted input. File edits must not lose another platform-owned
write between optimistic validation and replacement.

Git commands, edit diffs, and final patches can also amplify bounded inputs into
unbounded CPU, memory, or output. Sequence 6 therefore establishes a deterministic Git
and mutation boundary that Sequence 7 checkpoints can safely consume.

## Sequence 6 decision

### Edit transactions

- `edit_file` creates a missing file explicitly or replaces text in an existing file
  only when supplied the SHA-256 returned by `read_file`. Creation rejects
  `replace_all`; existing edits reject absent, ambiguous, stale, or oversized changes.
- Read, expected-hash validation, replacement, size validation, canonical patch
  identity, staging, final identity/hash validation, and replacement execute in one
  synchronous per-workspace mutation transaction off the event loop.
- Parent traversal, target access, temporary creation, final replacement, and cleanup
  are descriptor-relative and no-follow. The staged file receives its final mode before
  its file durability barrier; the containing directory is flushed after installation.
- The edit result is a closed immutable model using the canonical workspace path. It
  contains pre/post hashes, replacement count, bytes written, and the constant-size
  identity:

  `SHA256(length-framed("agent-edit-v1", path, previous-hash-or-create, new-hash))`

  It never materializes or returns an unbounded unified diff. The tool-call ID remains
  in the typed tool event envelope rather than being duplicated in the result.

### Private source capture

- Each run receives a hashed filesystem/commit token and a private `0700` allocation
  directory. Git creates the detached worktree as a child of that reservation; the
  reservation is never deleted and reclaimed by pathname before worktree creation.
- `git stash create` captures the tracked worktree and index trees without resetting
  the source. Untracked paths are NUL-parsed with count and output limits, then copied
  through descriptor-relative no-follow reads into a private staging tree.
- Untracked capture accepts regular files only, streams bytes through per-file and
  aggregate limits, and records a sorted manifest of canonical path, mode, actual size,
  and SHA-256. The tracked trees and untracked manifest are recomputed before return.
  Any mismatch fails with `source_repository_changed` and triggers targeted cleanup.
- The baseline revision is immutable at workspace construction. Linked worktrees may
  create Git objects and their own administration metadata in the common Git directory,
  but must not modify source branches, index state, checkout files, or status.

### Deterministic Git boundary

- Git is resolved once at manager construction to an absolute regular executable.
  Every invocation uses fixed argv with no shell, bounded stdout and stderr, a validated
  timeout, process-group termination, and a minimal locale environment.
- System/global configuration, prompts, pagers, hooks, signing, and filesystem monitors
  are disabled. Final patches additionally use `--no-ext-diff` and `--no-textconv`.
- Effective `filter.*.clean`, `filter.*.smudge`, and `filter.*.process` configuration
  is rejected with `workspace_external_filter_unsupported` before snapshot or staging
  operations. Trusted filter execution remains deferred to the hardened Podman
  boundary.
- Final patches remain binary, full-index, baseline-relative bytes. Output is consumed
  incrementally and fails at its byte ceiling. A zero-byte patch for differing
  revisions is a protocol error.

### Lifecycle

- Cleanup removes only the current linked worktree and its exact private allocation
  root. It never performs repository-wide `git worktree prune`.
- Construction failures close or remove resources created by that construction.
  Destruction becomes final only after both targeted Git administration and private
  filesystem cleanup succeed; failures return retryable `workspace_cleanup_failed`.
  Concurrent destroy callers serialize, and caller cancellation waits for the shielded
  cleanup attempt before propagating so no uncoordinated remover is left running.
- Run IDs and snapshot labels must be bounded valid UTF-8. Only a SHA-256-derived run
  token enters allocation paths and platform-generated baseline commit labels.
- One platform owner mutates a worktree sequentially. Cross-process writer leases are
  deferred to Sequence 20, and containment from hostile concurrent host processes is a
  hardened Podman responsibility.

## Sequence 7 decision

- Every registered tool declares its effect as read-only, workspace mutation, command,
  or interaction. The loop requires a `CheckpointCoordinator` before a mutation or
  command and emits `checkpoint.created` first.
- A checkpoint contains run/session identity, transcript position, exact pre-tool Git
  revision, task plan, context summary, and creation time. The loop validates the
  returned run identity, transcript position, plan, and summary before it starts the
  tool. A malformed coordinator response fails closed.
- The in-memory coordinator serializes creation, completion, rollback, and rewind. It
  binds each immutable checkpoint to one tool-call ID, rejects duplicate checkpoint
  IDs, validates exact checkpoint identity on later operations, and bounds checkpoint
  count, message count, and serialized state bytes.
- Success commits and validates a bounded workspace revision before it enters tool
  result metadata. Tool failure, finalization failure, and task cancellation restore
  the pre-tool revision before the error or cancellation is propagated.
- Rewind cancels active work, restores the Git revision, and returns the exact messages,
  plan, summary, and revision stored at the checkpoint. Checkpoints created after the
  rewind target are discarded so an abandoned future branch cannot be restored.
- Same-run duplicate suppression reuses an identical terminal tool outcome without a
  second checkpoint or repeated mutation.
- The current coordinator remains in-memory. Durable checkpoint, event, message, and
  replay storage, cross-worker ownership, and retention beyond one process belong to
  later persistence sequences.

## Consequences

- The user's checkout remains unchanged until a future explicit patch-application
  workflow.
- Bounded staged, unstaged, and untracked starting content is visible in a private
  worktree, and source changes during capture fail closed.
- Repository configuration cannot invoke host diff, text-conversion, filter, hook,
  filesystem-monitor, pager, prompt, or signing commands through this adapter.
- Patch identity calculation is constant-size, while final patch bytes remain bounded
  and suitable for user review.
- Descriptor safety and the platform mutation lock close platform-owned races. A
  hardened Podman sandbox and distributed writer leases remain required for hostile
  host concurrency and multi-worker ownership.
