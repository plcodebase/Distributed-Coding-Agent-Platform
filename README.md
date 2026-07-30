# Distributed Coding Agent Platform

This repository implements the system specified in [`DESIGN.md`](DESIGN.md). Work is
delivered phase by phase so each layer has a runnable verification gate before later
distributed-system and sandbox features are added.

## Implemented sequences

### Sequence 1: repository bootstrap and CI

Sequence 1 provides:

- a Python 3.12 `uv` workspace with locked dependencies;
- lint, type-check, unit-test, coverage, pre-commit, and CI configuration;
- local PostgreSQL, Redis, S3-compatible storage, LiteLLM, Prometheus, Grafana, and
  deterministic fake-provider services orchestrated by Podman;
- Pydantic Settings, structured JSON logging, and recursive secret redaction.

### Sequence 2: core agent events and domain models

Sequence 2 provides:

- immutable, validated session, run, tool-call, checkpoint, and model-call models;
- a centrally enforced run state machine with structured transition errors;
- canonical tool-argument hashes for stable idempotency comparisons;
- typed payload contracts for every agent event named in the design;
- finite, immutable domain JSON with ordinary JSON wire serialization;
- closed-schema parsing, 1 MiB event-payload limits, and UTC-aware timestamps;
- explicit approval policies and lease-free approval/retry suspension states.

### Sequence 3: fake model and deterministic agent loop

Sequence 3 provides:

- a provider-neutral streaming model-gateway contract;
- a deterministic scripted gateway that requires no credentials or network access;
- a bounded async agent loop with typed event emission and injected clocks/IDs;
- Pydantic tool schemas that validate model-generated arguments before execution;
- incrementally bounded, ordered tool streams with producer cancellation at output limits;
- sanitized tool output/results/errors and safe malformed-call retry feedback;
- same-run duplicate suppression plus context, request, turn, call, retry, timeout, and
  UTF-8 byte limits;
- unit and integration tests for final text, tool calls, malformed arguments, failures,
  and termination limits.

### Sequence 4: OpenAI Agents SDK integration

Sequence 4 provides:

- an isolated adapter package using the SDK model and provider interfaces without
  introducing provider imports into `agent-core`;
- OpenAI-compatible Chat Completions routing through the configured centralized gateway;
- exact conversion of normalized messages and tool schemas into SDK inputs;
- safe normalization of streamed text, completed tool calls, usage, refusals, and
  terminal failures into the existing gateway event union;
- finite JSON parsing, sanitized malformed-call feedback, disabled hidden retries and
  SDK tracing, cancellation cleanup, and explicit client lifecycle ownership;
- deterministic SDK unit tests plus a real fragmented-SSE integration test using an
  in-process server and no external credentials.

### Sequence 5: contained read and search tools

Sequence 5 provides:

- closed, typed `list_files`, `read_file`, and `search_files` argument schemas;
- immutable, closed result schemas and a read-only-by-default tool registry;
- descriptor-relative workspace containment with traversal, case-insensitive `.git`,
  external-symlink, and symlink-swap rejection;
- conservative protected-path filtering with exact composition-time allowlists and
  permanent repository-metadata denial;
- deterministic listings with a 20,000-entry scan ceiling and independently bounded
  returned entries;
- complete-line UTF-8 reads with lossless `next_start_line` continuation and linear
  JSON-size accounting off the event loop;
- an absolute, configuration-independent ripgrep invocation with no symlink following,
  strict JSON protocol validation, exact match truncation, and Unicode character
  columns;
- bounded recursion, entries, files, line ranges, search time, process output, result
  counts, match text, and serialized UTF-8 result payloads;
- binary, invalid-UTF-8, oversized-file, and oversized-result failures as structured
  domain errors.

### Sequence 6: reliable edits and isolated Git worktrees

Sequence 6 provides:

- private `0700`, per-run detached Git worktrees that capture staged, unstaged, and
  bounded regular untracked source state without changing the user's checkout;
- tracked-tree and untracked path/mode/size/content fingerprints that reject a source
  repository changing during capture;
- hash-checked exact or explicitly repeated text replacement in one per-workspace,
  descriptor-relative transaction, including a final target identity/hash check;
- same-directory atomic staging with mode application and file/directory durability
  barriers;
- closed edit results with canonical paths, pre/post hashes, constant-size canonical
  patch identities, and byte/replacement metadata;
- Git resolved once to an absolute executable and run with bounded stdout/stderr,
  timeouts, process-group termination, a minimal environment, and isolated
  global/system configuration;
- fail-closed rejection of Git clean/smudge/process filters and explicit suppression of
  hooks, signing, filesystem monitors, external diffs, text conversion, prompts, and
  pagers;
- isolated Git revisions, targeted retryable lifecycle cleanup, and incrementally
  bounded binary final patches relative to the captured baseline.

### Sequence 7: checkpoints and rewind

Sequence 7 provides:

- mandatory tool-effect declarations and checkpoints before every workspace mutation
  or command;
- typed checkpoint events before execution and workspace revision metadata on success;
- automatic Git restoration after failed side effects;
- rewind of active commands, workspace revision, transcript, task plan, and context
  summary;
- same-run duplicate reuse without a second checkpoint or repeated mutation.

The current checkpoint coordinator is intentionally in-memory. Durable checkpoint,
message, event, and replay storage remains a persistence-sequence responsibility.

### Sequence 8: sandbox contract and local development adapter

Sequence 8 provides:

- provider-neutral command, streaming output, terminal outcome, snapshot, and sandbox
  contracts in `agent-core`;
- an argv-only `run_command` tool with timeout, output, result, and non-zero-exit
  failures represented as structured errors;
- concurrent bounded stdout/stderr streaming and process-group cancellation;
- a minimal non-inherited process environment and contained working directory;
- an explicitly unsafe `LocalSandbox` that is disabled by default, requires development
  or test opt-in, and refuses enabled construction in production.

`LocalSandbox` is not a security boundary. Production command execution remains
disabled until the later hardened Podman sandbox and its isolation tests are complete.

## Local setup

```shell
cp .env.example .env
make bootstrap
make check
make compose-config
make compose-up
make compose-smoke
```

`make compose-up` starts shared infrastructure only and invokes the locked
`podman-compose` package with the native Podman CLI explicitly. `make compose-smoke`
verifies every exposed dependency and routes requests through LiteLLM to both
deterministic fake providers. Agent applications and workers are added in later
implementation sequences. The fake LiteLLM routes are the default local configuration
and do not need provider credentials.

Do not put real credentials in `.env.example`, source control, worker environments, or
sandbox environments.
