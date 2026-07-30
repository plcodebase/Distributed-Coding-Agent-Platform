# ADR 0006: Contained workspace tools

- Status: Accepted
- Date: 2026-07-28

## Context

Sequence 5 introduces the first tools that inspect a real repository. Model-generated
paths, patterns, ranges, and result sizes are untrusted. A normal path join or a
post-materialization size check would allow traversal, symlink escape, repository
metadata access, excessive memory use, or oversized model context.

## Decision

- Put concrete repository tools in the separate `sandbox-runtime` adapter package.
  `agent-core` retains only provider-neutral tool and sandbox contracts.
- Expose `list_files`, `read_file`, and `search_files` through closed Pydantic argument
  models and closed immutable result models. Validate arguments before filesystem work
  and validate normalized results before they enter an agent event.
- Make the registry read-only by default. Mutation and command registrations require
  independent composition-time capability flags.
- Resolve paths relative to one canonical workspace root. Reject absolute paths, parent
  traversal, NUL bytes, external symlink targets, and `.git` components using
  case-insensitive matching. Repository metadata can never be allowlisted.
- Apply one injected access policy to listing, reads, and search. Deny conservative
  credential paths by default; permit only exact file or directory exceptions selected
  by the composition root. Omit protected entries and report policy filtering.
- Use descriptor-relative, `O_NOFOLLOW` opens for direct reads and directory traversal.
  Construction fails when the required POSIX primitives are unavailable.
- Bound directory enumeration independently of returned entries. Scan no more than
  20,000 entries, then sort the bounded set and return at most 2,000 entries at depth 20.
- Return only regular UTF-8 text files. Scan bounded decoded text without materializing
  a Python object for every line. Return complete lines only and provide a lossless
  `next_start_line`; reject an individually oversized line.
- Fit serialized JSON with linear string/item accounting off the event loop. Bound
  source files, line numbers, search time, process output, match count, match preview,
  and final UTF-8 result bytes independently.
- Resolve `rg` once to an absolute regular executable. Invoke it without a shell or
  ambient configuration, without following symlinks, with explicit file/preview limits,
  final protected-path exclusions, and a minimal locale environment.
- Treat the ripgrep JSON stream as a strict adapter protocol. Validate paths, line
  numbers, UTF-8 byte offsets, and event shapes; convert offsets into one-based Unicode
  character columns; fail closed on malformed output.

## Consequences

- Repository inspection is contained and independently testable without the model SDK
  or agent loop.
- Protected-path filtering is defense in depth, not a claim that arbitrary secret text
  can always be detected. Result redaction remains a second boundary.
- Descriptor-relative direct operations prevent symlink-swap traversal. External search
  assumes the platform exclusively owns the per-run worktree while `rg` executes.
- Search depends on a trusted absolute ripgrep executable supplied by the composition
  root.
- File mutation, atomic edit semantics, and Git worktree behavior are Sequence 6
  decisions recorded by ADR 0007.
