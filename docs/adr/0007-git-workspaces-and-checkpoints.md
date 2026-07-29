# ADR 0007: Git workspaces and pre-mutation checkpoints

- Status: Accepted
- Date: 2026-07-28

## Context

Sequences 6 and 7 require the agent to work from the user's current repository state,
including staged, unstaged, and untracked files, without mutating the source checkout.
Every side effect must be recoverable, duplicate tool delivery must not repeat a
mutation, and rewind must restore repository and conversational state together.

Git commands and final patches can themselves produce large output, so checkpointing
must not weaken the resource bounds established by the core loop.

## Decision

- Create one detached linked Git worktree per run. Use `git stash create` to capture the
  tracked index/worktree state without changing the source checkout, then copy only
  bounded, non-ignored, regular untracked files.
- Commit the captured state as an isolated baseline. Verify the source HEAD and exact
  porcelain status before returning the workspace; fail and clean up if either changed.
- Bound every Git subprocess by an absolute executable path, fixed argv construction,
  timeout, process-group cleanup, and incrementally read output ceiling.
- Keep all agent commits on the detached worktree. Generate a binary full-index final
  patch relative to the captured baseline and terminate generation at its byte ceiling.
- Declare every registered tool's effect as read-only, workspace mutation, command, or
  interaction. The agent loop requires a `CheckpointCoordinator` before executing a
  mutation or command and emits `checkpoint.created` first.
- A checkpoint contains run/session identity, transcript position, exact pre-tool Git
  revision, task plan, context summary, and creation time. Successful operations commit
  a new workspace revision into their result; failed operations restore the pre-tool
  revision.
- Rewind first cancels active work, restores the Git revision, and returns the exact
  messages, plan, summary, and revision stored at the checkpoint.
- Retain same-run duplicate suppression in the core loop. An identical repeated tool
  call reuses its terminal outcome and creates neither a second checkpoint nor a second
  mutation.
- Use an in-memory checkpoint coordinator only for the current single-process sequence.
  Durable checkpoint/event/message persistence remains a later sequence.

## Consequences

- The user's checkout remains unchanged until a future explicit patch-application
  workflow is invoked.
- Staged, unstaged, and bounded untracked starting state is visible to the agent without
  stashing or resetting the user's checkout.
- Every mutation and command is associated with a pre-operation recovery point, and a
  failed tool cannot leave its workspace changes behind when restoration succeeds.
- Git object creation occurs in the repository object database, but no source branch,
  index, working-tree file, or status entry is changed.
- Checkpoint records are not yet durable across worker or process loss.

