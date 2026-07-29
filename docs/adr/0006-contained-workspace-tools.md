# ADR 0006: Contained workspace tools

- Status: Accepted
- Date: 2026-07-28

## Context

Sequence 5 introduces the first tools that inspect a real repository. Model-generated
paths, patterns, ranges, and result sizes are untrusted. A normal path join or a
post-materialization size check would allow traversal, symlink escape, repository
metadata access, excessive memory use, or oversized model context.

Sequence 6 adds file mutation. An edit based only on model-supplied text can silently
overwrite a concurrently changed file or replace an unintended repeated match.

## Decision

- Put concrete repository tools in the separate `sandbox-runtime` adapter package.
  `agent-core` retains only provider-neutral tool and sandbox contracts.
- Expose `list_files`, `read_file`, and `search_files` through closed Pydantic argument
  models and the existing `ToolRegistry`. All model arguments are validated before a
  filesystem operation starts.
- Resolve paths relative to one canonical workspace root. Reject absolute paths, parent
  traversal, NUL bytes, `.git` access, external symlink targets, and any write traversing
  a symlink. Listings skip repository metadata and external symlinks.
- Return only regular UTF-8 text files. Bound source file reads, line ranges, listing
  entry counts, recursion depth, search time, search process output, match count, match
  text, and the final serialized result. Measure limits in UTF-8 bytes.
- Invoke a configured `rg` executable with an argv sequence, a contained working
  directory, fixed safe flags, and no shell. Parse its JSON event stream into normalized
  path, line, column, and text results.
- Require `edit_file` to operate in one of two explicit modes:
  - create a missing file without an expected hash or old text;
  - replace text in an existing file only with the SHA-256 returned by `read_file`.
- Reject stale hashes, missing old text, absent matches, and ambiguous repeated matches
  unless `replace_all` is explicit. Limit both input and resulting file bytes.
- Write through a same-directory temporary file, flush file data, preserve existing
  mode bits, atomically replace the destination, and flush the parent directory.
- Return pre/post content hashes, patch hash, replacement count, and bytes written
  without returning an unbounded patch body.

## Consequences

- Repository reads and writes are contained by construction and remain independently
  testable without the model SDK or agent loop.
- A successful edit cannot silently apply to a file version different from the one the
  agent read.
- The tool layer does not claim protection from a hostile host process. Command
  isolation is a separate sandbox responsibility.
- Search depends on a trusted ripgrep executable supplied by the composition root.

