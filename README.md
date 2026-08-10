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
  bounded binary final patches relative to the captured baseline;
- cancellation-safe serialized destruction and bounded UTF-8 run/snapshot labels, with
  only hashed run tokens entering paths and platform-generated commit labels.

### Sequence 7: checkpoints and rewind

Sequence 7 provides:

- mandatory tool-effect declarations and checkpoints before every workspace mutation
  or command;
- typed checkpoint events before execution and workspace revision metadata on success;
- validation that coordinator-returned checkpoint state exactly matches the requested
  run, transcript position, task plan, and context summary;
- serialized, bounded, tool-call-bound in-memory checkpoint state with duplicate-ID
  and forged-checkpoint rejection;
- cancellation-safe Git restoration after failed, cancelled, or unsuccessfully
  finalized side effects;
- rewind of active commands, workspace revision, transcript, task plan, and context
  summary, with later abandoned checkpoints invalidated;
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
- lifecycle serialization across process start, cancellation, and close, including
  child termination when output consumers fail;
- bounded concurrency with queued-command invalidation on cancellation;
- a validated, bounded, minimal non-inherited process environment and contained working
  directory;
- bounded direct writes and sandbox-owned snapshots with branch truncation on restore;
- cancellation-safe, retryable cleanup that blocks reuse after partial failure;
- an explicitly unsafe `LocalSandbox` that is disabled by default, requires development
  or test opt-in, rejects unknown runtime labels, and refuses enabled construction in
  production.

`LocalSandbox` is not a security boundary. Production composition must use the
`PodmanSandbox` described next.

### Sequence 9: hardened Podman sandbox

Sequence 9 provides:

- a production `PodmanSandbox` backed by a verified rootless Podman engine;
- one disposable, uniquely named container per argv-only command;
- a non-root keep-id user namespace, all-capability drop, `no-new-privileges`, private
  PID/cgroup/IPC/UTS namespaces, and an explicitly retained default seccomp policy;
- a read-only root filesystem, bounded tmpfs, offline network mode, and exactly one
  writable mount containing the owned Git worktree, with image volumes ignored;
- cleared image defaults, disabled host-proxy propagation, fixed non-secret container
  environment values, and a bounded Podman-control environment allowlist;
- explicit CPU, memory/swap, PID, open-file, timeout, output, direct-write, and snapshot
  ceilings;
- a Podman-native watchdog, disabled runtime logging/restarts, and a `nodev,nosuid`
  workspace bind in addition to worker-side cancellation;
- cancellation-safe, single-owner targeted container removal before runner/worktree
  cleanup, with retryable partial-cleanup failures;
- mandatory digest-pinned images in production configuration.

### Sequence 10: sandbox security verification

Sequence 10 provides an opt-in executable security suite that verifies:

- effective non-root identity and cgroup/open-file limits from inside the container;
- effective zero capabilities, `no-new-privileges`, seccomp filtering, and a
  credential/proxy-free command environment;
- denial of host credential reads, symlink escapes, root-filesystem writes, external
  networking, and Podman service-socket access;
- PID exhaustion, memory exhaustion, command timeouts, and bounded output;
- removal of all platform command containers when a sandbox is destroyed mid-command.

The suite uses only Podman and local test data. Run it with `make sandbox-security`
after the sandbox image is built.

### Sequence 11: LiteLLM Proxy deployment

Sequence 11 provides:

- a hardened, loopback-only LiteLLM service deployed through Podman Compose;
- stable `coding-default`, `coding-fast`, `coding-strong`, `summarization`, and
  `code-review` aliases;
- production mappings across OpenAI and Anthropic deployments, with provider
  credentials scoped only to LiteLLM;
- deterministic local mappings across two private fake-provider services;
- an internal-only fake-provider network and digest-injectable LiteLLM release image;
- bounded retries, cooldown, upstream timeouts, and explicit compatible fallbacks;
- deployment-contract tests plus a live suite that verifies all aliases and proves
  `coding-default` reaches the secondary when the primary is unavailable.

Client-side retry, stored-result idempotency, circuit breaking, and rate-limit policy
are supplied by Sequence 13.

### Sequence 12: typed gateway client and normalized streaming

Sequence 12 provides:

- a separate `gateway-client` composition package wrapping the Agents SDK model adapter;
- required tenant, session, run, turn, model-call, and stable request identifiers on
  every typed gateway request;
- immutable attribution metadata and headers applied to every upstream call;
- a default allowlist containing exactly the five logical model routes;
- header-safe attribution IDs, hard request collection limits, and a serialized
  request-byte ceiling enforced before delegate invocation;
- revalidation of every normalized stream event, one-terminal-event enforcement, and
  rejection of malformed, incomplete, or post-terminal streams;
- cumulative UTF-8 byte and event-count limits with delegated stream cancellation;
- opaque provider failures and idempotent, cancellation-safe, retryable async lifecycle
  cleanup that blocks model traffic after partial cleanup;
- unit tests and a live streaming test through the Podman LiteLLM deployment.

### Sequence 13: gateway retry, fallback, and idempotency

Sequence 13 provides:

- tenant-scoped stable request claims with canonical payload hashes;
- completed normalized-response replay without another provider request;
- running, failed, and conflicting request-ID semantics that fail closed;
- bounded exponential backoff and injected jitter only before any response event;
- strict suppression of retries after partial streamed output;
- per-tenant/per-route admission and closed/open/half-open circuit policies;
- deterministic in-memory policies for tests plus shared PostgreSQL production
  adapters;
- durable terminal-event commit before terminal success reaches the agent;
- cancellation-safe durable completion/failure/release bookkeeping and
  close-after-terminal replay safety;
- bounded rate/circuit configuration and finite injected policy clocks;
- compatible provider fallback retained inside LiteLLM with stable logical route and
  request identity.

### Sequence 14: PostgreSQL persistence and migrations

Sequence 14 provides:

- a SQLAlchemy 2.x async `platform-persistence` package using asyncpg;
- explicit reversible Alembic migrations;
- durable sessions, runs, messages, task plans, tool calls, approvals, checkpoints,
  events, model-call accounting, gateway requests, rate windows, and circuits;
- tenant IDs in every tenant-owned query and relational constraint;
- composite ownership constraints tying run/workspace/session, message/run/session,
  checkpoint/run/session, approval/tool/run, and selected checkpoint/run identities;
- database uniqueness for run/tool/model/request idempotency and ordered records;
- database-enforced event types plus runtime validation of run and gateway idempotency
  identities before SQL construction;
- compare-and-set run transitions using the core transition policy;
- bounded connection pools, statement timeouts, UTC sessions, readiness, and explicit
  cancellation-safe engine cleanup;
- exact declarative/migration check-constraint parity tests and deterministic
  transaction-boundary tests.

### Sequence 15: FastAPI session and run APIs

Sequence 15 provides:

- dependency-injected FastAPI handlers for every Phase 5 HTTP control endpoint;
- an authentication protocol and bounded local bearer-token implementation;
- tenant-scoped session/run reads that do not reveal cross-tenant resource existence;
- API run-creation idempotency keys with matching-result replay and payload-conflict
  rejection;
- active-session enforcement for new runs;
- a pre-routing 64 KiB body ceiling that validates declared and actual streamed bytes;
- durable cancellation, approval-decision, and rewind-selection operations;
- live and PostgreSQL-backed readiness endpoints;
- duplicate-key-safe local credential parsing and explicit WebSocket authentication,
  tenant-absence, and internal-failure close codes;
- closed domain errors and opaque unexpected failures.

Run creation persists `QUEUED` work. Sequence 17 workers claim it outside the API
process.

### Sequence 16: durable event store and WebSocket replay

Sequence 16 provides:

- core `EventDraft`, `StoredEvent`, and bounded `EventPage` contracts;
- discriminated event-payload validation before persistence;
- atomic per-run sequence allocation and append in one PostgreSQL transaction;
- the unique `(run_id, sequence)` database invariant;
- ordered HTTP replay after an exclusive cursor with exact pagination;
- authenticated WebSocket catch-up followed by live durable polling;
- shared 1,000-event and 4 MiB serialized page ceilings with incrementally streamed
  database rows;
- fail-closed durable sequence-gap detection;
- awaited sends for backpressure and disconnect handling that never cancels a run.

### Sequence 17: PostgreSQL task queue

Sequence 17 provides:

- durable queued work using the existing run row as the system of record;
- atomic worker-capacity, run-lease, and workspace-writer reservation;
- `FOR UPDATE SKIP LOCKED` claims ordered by priority, age, and stable run ID;
- SQL exclusion of cancelled runs and actively owned workspaces;
- random lease tokens plus monotonic generations for stale-owner fencing;
- active-lease-fenced worker event and tool-state writes using database-time expiry
  checks;
- transactionally returned capacity and ownership on completion or recovery;
- real PostgreSQL concurrency coverage with three independent worker identities.

### Sequence 18: worker leases and heartbeats

Sequence 18 provides:

- durable worker identity, sandbox capabilities, slots, status, and heartbeat records;
- bounded run leases with periodic run and workspace renewal across recovery,
  restoration, and execution;
- a worker service that runs model/tool orchestration outside FastAPI;
- immediate pre-execution and heartbeat-observed cancellation of whichever worker phase
  is active;
- exact workspace-writer token/generation propagation into restoration, execution, and
  loop composition;
- graceful draining that rejects new claims while owned work finishes;
- surfaced background task failures instead of unobserved asyncio exceptions;
- a trusted composition-factory CLI that starts three spawned worker processes by
  default;
- a separately composed lease-recovery scheduler process.

### Sequence 19: failure recovery and idempotent replay

Sequence 19 provides:

- bounded scheduler recovery of expired leases through `LOST` and back to `QUEUED`;
- attempt increments and complete stale run/workspace lease cleanup;
- active-lease-fenced, aggregate-byte-bounded streaming of checkpoint session
  conversation, plan, summary, and terminal tool outcomes;
- restoration at the latest durable post-tool workspace revision;
- monotonic tool-call persistence that accepts predecessor replay but rejects identity
  or terminal-outcome divergence;
- reuse only when terminal tool-call ID, name, and argument hash all match;
- stable event delivery keys with database-enforced identical replay;
- real PostgreSQL Worker A loss / Worker B restoration and completion coverage.

### Sequence 20: workspace writer leases

Sequence 20 provides:

- exactly one durable writer row per tenant workspace;
- writer reservation in the same transaction as run claim;
- distinct random writer tokens and monotonic writer generations;
- renewal bounded by the owning run lease;
- stale heartbeat and release rejection;
- renewed same-owner acquisition and generation-aware idempotent release;
- a composite database foreign key proving the writer's run targets the same workspace;
- conservative serialization of all runs for a workspace until measured read sharing is
  introduced.

### Sequence 21: bounded concurrency and tenant quotas

Sequence 21 provides independent worker run/sandbox semaphores, PostgreSQL-backed
tenant active/queued-run limits, shared gateway request slots, provider-route request
and token-window limits, renewable expiring capacity claims, cancellation-safe release,
and structured capacity errors. Platform-owned route settings reconcile after restart.
All admission state is injected behind typed interfaces; no process-local fallback is
used in production composition.

### Sequence 22: backpressure and priority scheduling

Sequence 22 adds interactive, background, and evaluation priority classes; bounded
aging in atomic `SKIP LOCKED` queue claims; serialized global queue admission; HTTP 429
responses with `Retry-After`; and bounded queue snapshots containing class depth and
oldest wait. Global admission settings reconcile under the singleton lock. Worker
claims stop at configured capacity instead of accumulating unbounded in-process work.

### Sequence 23: context pipeline and compact

Sequence 23 provides all eight design contributors, conservative UTF-8 token
estimation, per-route input/output reservations, gateway-backed compression, and
explicit preservation of recent conversation/tool pairs, active files, unresolved task
items, and errors. Completed/cancelled task history is compressible. The compaction API
creates at most one pending request per session at a message watermark and records the
actual `summarization` route; workers record summary usage without updating or deleting
the source transcript.

## Local setup

```shell
cp .env.example .env
make bootstrap
make check
make compose-config
make compose-up
make migrate
make compose-smoke
make api
```

`make compose-up` starts shared infrastructure only and invokes the locked
`podman-compose` package with the native Podman CLI explicitly. `make compose-smoke`
verifies every exposed dependency and routes requests through LiteLLM to both
deterministic fake providers. `make migrate` creates the durable control-plane schema;
`make api` serves the authenticated API on `127.0.0.1:8000`. The fake LiteLLM routes are
the default local configuration and do not need provider credentials.

Worker and scheduler processes use trusted application composition factories:

```shell
uv run python -m agent_worker --factory your_app.workers:create_worker
uv run python -m agent_scheduler --factory your_app.scheduler:create_scheduler
```

The worker command starts three OS processes by default. A production factory must
inject the PostgreSQL queue/stores, gateway client, durable checkpoint coordinator,
workspace snapshot restorer, and sandbox; each worker index must map to a unique worker
ID. These are deployment composition references, not model- or user-controlled values.

Do not put real credentials in `.env.example`, source control, worker environments, or
sandbox environments.

For the opt-in runtime suites:

```shell
make podman-images
make sandbox-security
make gateway-security ENV_FILE=.env
make postgres-security
```

`gateway-security` expects the local gateway services to be available and uses only the
gateway key from the selected environment file. The test itself recreates and later
stops its three targeted LiteLLM/fake-provider services.
