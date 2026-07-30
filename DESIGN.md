# Distributed Coding Agent Platform — Implementation Design

**Status:** Draft
**Audience:** Codex implementation agent and project maintainers
**Primary language:** Python
**Deployment target:** Local Podman Compose first, Kubernetes second

---

## 1. Objective

Build a production-oriented coding-agent platform that can:

1. Inspect, edit, and test multi-file repositories through a stateful agent loop.
2. Execute untrusted commands inside isolated, resource-constrained sandboxes.
3. Run many agent sessions concurrently across distributed workers.
4. Recover tasks after worker or process failures.
5. Route all model calls through a centralized multi-provider LLM gateway.
6. Stream agent events to clients with reconnect and replay support.
7. Track latency, token usage, cost, retries, tool calls, and task outcomes.
8. Demonstrate the system through load tests, failure tests, and coding-task evaluations.

The final project should demonstrate four primary engineering capabilities:

* Coding-agent orchestration
* Secure sandboxed execution
* Distributed systems and concurrency control
* LLM infrastructure and gateway design

---

## 2. Technology Stack

### Application

* Python 3.12+
* FastAPI
* Pydantic
* SQLAlchemy 2.x
* Alembic
* `asyncio`
* OpenAI Agents SDK

### Infrastructure

* PostgreSQL as the durable system of record
* Redis for short-lived coordination, rate limiting, and caching
* Podman for local sandbox execution
* Kubernetes for distributed deployment and horizontal scaling
* MinIO or S3-compatible storage for logs, snapshots, and artifacts

### LLM Gateway

* LiteLLM Proxy
* OpenAI-compatible gateway API
* OpenAI, Anthropic, and optional local model deployments
* Model aliases and capability-based routes

### Observability

* OpenTelemetry
* Prometheus
* Grafana
* Structured JSON logs
* OpenAI Agents SDK tracing where appropriate

Use current stable package releases and commit a lockfile. Do not rely on floating dependency versions.

---

## 3. Scope

### In scope

* Terminal or HTTP-submitted coding tasks
* Persistent agent sessions
* Streaming text and tool events
* Repository inspection
* Reliable file editing
* Shell command execution
* Permission approval
* Task planning and progress tracking
* Context compression
* Checkpoints and rewind
* Podman sandbox isolation
* Distributed task scheduling
* Worker leases and heartbeats
* Checkpoint-based recovery
* Bounded concurrency and backpressure
* Multi-provider model routing
* Rate limiting, retries, fallback, and circuit breaking
* Token and cost attribution
* Load tests and failure-injection tests

### Out of scope for the first release

* Full IDE integration
* Browser-based code editor
* Arbitrary third-party plugins
* Multi-region deployment
* Custom model training
* Firecracker microVM implementation
* Semantic LLM response caching
* Autonomous production deployments
* Running arbitrary untrusted workloads with a formal security guarantee

The architecture must permit later support for gVisor or Firecracker, but the first
implementation should use hardened rootless Podman containers.

---

## 4. Architectural Principles

### 4.1 Separate control plane from execution plane

The control plane owns:

* Sessions
* Runs
* Scheduling
* Run state
* Approvals
* Quotas
* Worker leases
* Checkpoint metadata

The execution plane owns:

* Agent loops
* Tool dispatch
* Sandbox lifecycle
* Repository mutation
* Test execution
* Model calls through the gateway

### 4.2 All model calls go through the LLM gateway

Agent workers must never call OpenAI, Anthropic, or other model providers directly.

The only permitted path is:

```text
Agent Worker
    → Gateway Client
    → LiteLLM Proxy
    → Model Provider
```

This centralizes:

* Authentication
* Routing
* Rate limiting
* Retry policies
* Provider fallback
* Cost tracking
* Model health
* Request observability

### 4.3 At-least-once execution with idempotent side effects

Do not claim exactly-once task delivery.

Tasks and events may be delivered more than once. Correctness must come from:

* Stable operation IDs
* Idempotency keys
* Workspace version checks
* Database uniqueness constraints
* Recorded tool results
* Safe replay semantics

### 4.4 One writer per workspace

Different sessions may execute concurrently, but only one active run may mutate a particular workspace at a time.

Enforce this using a lease keyed by `workspace_id`.

Read-only operations may eventually support concurrency, but initial implementation should serialize all runs targeting the same workspace.

### 4.5 Durable state must not live only in process memory

A worker may disappear at any time.

All information needed for recovery must be stored in PostgreSQL or object storage:

* Run state
* Conversation state
* Tool results
* Approvals
* Checkpoint metadata
* Workspace snapshot reference
* Last emitted event sequence

### 4.6 Core logic depends on interfaces, not providers

The agent core must not import:

* Podman-compatible runtime API
* LiteLLM-specific classes
* PostgreSQL drivers
* Redis clients
* FastAPI request objects
* Kubernetes clients

These dependencies belong in adapters.

---

## 5. High-Level Architecture

```text
                           ┌───────────────────────┐
                           │ CLI / Web Client      │
                           └───────────┬───────────┘
                                       │ HTTP / WebSocket
                                       ▼
                           ┌───────────────────────┐
                           │ Agent API             │
                           │                       │
                           │ Session API           │
                           │ Run API               │
                           │ Approval API          │
                           │ Event Streaming       │
                           └───────────┬───────────┘
                                       │
                                       ▼
                           ┌───────────────────────┐
                           │ Control Plane         │
                           │                       │
                           │ Run Manager           │
                           │ Scheduler             │
                           │ Lease Manager         │
                           │ Checkpoint Service    │
                           └───────────┬───────────┘
                                       │ durable queue
                  ┌────────────────────┼────────────────────┐
                  ▼                    ▼                    ▼
          ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
          │ Agent Worker │     │ Agent Worker │     │ Agent Worker │
          │              │     │              │     │              │
          │ Agent Loop   │     │ Agent Loop   │     │ Agent Loop   │
          │ Tool Engine  │     │ Tool Engine  │     │ Tool Engine  │
          │ Sandbox      │     │ Sandbox      │     │ Sandbox      │
          └──────┬───────┘     └──────┬───────┘     └──────┬───────┘
                 └────────────────────┼────────────────────┘
                                      │
                                      ▼
                           ┌───────────────────────┐
                           │ LLM Gateway           │
                           │                       │
                           │ Unified API           │
                           │ Routing               │
                           │ Rate Limits           │
                           │ Retry / Fallback      │
                           │ Cost Accounting       │
                           └───────┬───────┬───────┘
                                   │       │
                             OpenAI       Other Providers

Shared infrastructure:

PostgreSQL:
- sessions
- runs
- messages
- tool calls
- events
- approvals
- leases
- checkpoints
- model-call metadata

Redis:
- rate-limit counters
- worker presence
- short-lived coordination
- distributed semaphores
- gateway cache

Object storage:
- workspace snapshots
- command logs
- generated patches
- evaluation artifacts
```

---

## 6. Repository Structure

```text
agent-platform/
├── apps/
│   ├── agent-api/
│   ├── agent-worker/
│   ├── scheduler/
│   └── cli/
│
├── packages/
│   ├── agent-core/
│   ├── agent-protocol/
│   ├── gateway-client/
│   ├── sandbox-runtime/
│   ├── persistence/
│   ├── event-store/
│   └── telemetry/
│
├── services/
│   └── llm-gateway/
│       ├── litellm-config.yaml
│       └── policies/
│
├── deployments/
│   ├── podman-compose/
│   └── kubernetes/
│
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── end_to_end/
│   ├── security/
│   ├── load/
│   └── chaos/
│
├── benchmarks/
│   ├── coding_tasks/
│   ├── gateway_load/
│   └── reports/
│
├── docs/
│   ├── architecture/
│   ├── adr/
│   ├── operations/
│   └── threat-model.md
│
├── pyproject.toml
├── uv.lock
├── compose.yaml
└── DESIGN.md
```

Use a Python workspace or monorepo package configuration. Avoid copying shared domain models between services.

---

## 7. Core Domain Models

### 7.1 Session

```python
class Session:
    id: UUID
    tenant_id: UUID
    workspace_id: UUID
    status: SessionStatus
    approval_mode: ApprovalMode
    model_route: str
    created_at: datetime
    updated_at: datetime
```

A session represents an ongoing user conversation associated with a workspace.

### 7.2 Run

```python
class Run:
    id: UUID
    session_id: UUID
    workspace_id: UUID
    status: RunStatus
    priority: int
    attempt: int
    assigned_worker_id: str | None
    lease_expires_at: datetime | None
    last_checkpoint_id: UUID | None
    cancellation_requested: bool
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
```

### 7.3 Run state machine

```text
QUEUED
   ↓
LEASED
   ↓
RUNNING
   ├── WAITING_APPROVAL
   ├── RETRY_PENDING
   ├── COMPLETED
   ├── FAILED
   ├── CANCELLED
   └── LOST
          ↓
        QUEUED
```

Every state transition must be validated centrally. Do not allow arbitrary status assignment.

### 7.4 Tool call

```python
class ToolCall:
    id: str
    run_id: UUID
    turn_number: int
    tool_name: str
    arguments: dict
    argument_hash: str
    status: ToolCallStatus
    workspace_version: str | None
    result: dict | None
    started_at: datetime | None
    completed_at: datetime | None
```

`id` must be stable across task retries.

A database uniqueness constraint must prevent two successful executions for the same logical tool-call ID.

### 7.5 Checkpoint

```python
class Checkpoint:
    id: UUID
    run_id: UUID
    session_id: UUID
    message_sequence: int
    workspace_snapshot_uri: str
    workspace_revision: str
    task_plan: dict
    context_summary: str | None
    created_at: datetime
```

### 7.6 Agent event

```python
class AgentEvent:
    run_id: UUID
    sequence: int
    event_type: str
    payload: dict
    created_at: datetime
```

The pair `(run_id, sequence)` must be unique.

Example event types:

```text
run.started
context.build_started
model.request_started
model.text_delta
model.tool_call_received
tool.approval_required
tool.started
tool.stdout
tool.stderr
tool.completed
checkpoint.created
run.retry_scheduled
run.completed
run.failed
```

### 7.7 Model call

```python
class ModelCall:
    id: str
    run_id: UUID
    request_id: str
    route_name: str
    provider: str | None
    model: str | None
    status: str
    input_tokens: int | None
    output_tokens: int | None
    cached_tokens: int | None
    estimated_cost_usd: Decimal | None
    retry_count: int
    fallback_count: int
    started_at: datetime
    first_token_at: datetime | None
    completed_at: datetime | None
```

---

## 8. Core Interfaces

Implement these interfaces before writing provider-specific adapters.

### Model gateway

```python
class ModelGateway(Protocol):
    async def stream(
        self,
        request: GatewayRequest,
    ) -> AsyncIterator[GatewayEvent]:
        ...
```

### Sandbox

```python
class Sandbox(Protocol):
    async def execute(
        self,
        command: CommandSpec,
    ) -> AsyncIterator[CommandEvent]:
        ...

    async def read_file(self, path: str) -> bytes:
        ...

    async def write_file(self, path: str, content: bytes) -> None:
        ...

    async def create_snapshot(self) -> WorkspaceSnapshot:
        ...

    async def restore_snapshot(self, snapshot: WorkspaceSnapshot) -> None:
        ...

    async def destroy(self) -> None:
        ...
```

### Run repository

```python
class RunRepository(Protocol):
    async def create(self, run: Run) -> None:
        ...

    async def get(self, run_id: UUID) -> Run:
        ...

    async def transition(
        self,
        run_id: UUID,
        expected_status: RunStatus,
        new_status: RunStatus,
    ) -> bool:
        ...
```

### Task queue

```python
class RunQueue(Protocol):
    async def enqueue(self, run_id: UUID, priority: int) -> None:
        ...

    async def claim(
        self,
        worker_id: str,
        lease_duration_seconds: int,
    ) -> ClaimedRun | None:
        ...

    async def acknowledge(self, run_id: UUID) -> None:
        ...

    async def release(self, run_id: UUID) -> None:
        ...
```

### Event store

```python
class EventStore(Protocol):
    async def append(
        self,
        run_id: UUID,
        event_type: str,
        payload: dict,
    ) -> AgentEvent:
        ...

    async def read_after(
        self,
        run_id: UUID,
        sequence: int,
    ) -> AsyncIterator[AgentEvent]:
        ...
```

### Approval broker

```python
class ApprovalBroker(Protocol):
    async def request(self, request: ApprovalRequest) -> ApprovalDecision:
        ...
```

---

# 9. Step-by-Step Implementation Plan

## Phase 0 — Project Bootstrap

### Goal

Create a reproducible monorepo with automated quality checks.

### Tasks

1. Create the repository structure.
2. Configure Python workspace packaging.
3. Add:

   * Ruff
   * mypy or Pyright
   * pytest
   * pytest-asyncio
   * coverage
4. Add pre-commit hooks.
5. Add GitHub Actions for:

   * lint
   * type checking
   * unit tests
   * migration validation
   * container build
6. Create shared configuration using Pydantic Settings.
7. Add structured JSON logging.
8. Add local Podman Compose services:

   * PostgreSQL
   * Redis
   * MinIO
   * LiteLLM Proxy
9. Add health checks for each service.

### Acceptance criteria

* `make test` or equivalent runs all checks.
* `podman-compose up` starts all dependencies through the native Podman runtime.
* No secrets are committed.
* A sample configuration file documents every required variable.
* CI passes from a clean checkout.

---

## Phase 1 — Single-Process Coding Agent

### Goal

Implement a local coding agent before adding distribution.

### Tasks

1. Define the main coding agent using OpenAI Agents SDK.

   Sequence 4 implements this by adapting the SDK `ModelProvider` and streaming `Model`
   interfaces beneath the platform `ModelGateway`. The platform `AgentLoop` remains the
   sole owner of turns, tool execution, validation, limits, redaction, and typed events;
   SDK `Runner` and SDK sessions are not used because they would introduce a second
   orchestration and state boundary.
2. Implement tools:

   * `list_files`
   * `read_file`
   * `search_files`
   * `edit_file`
   * `run_command`
   * `ask_user`
   * `update_task_plan`

   Sequence 5 implements the read-only subset with closed argument/result contracts,
   descriptor-relative no-follow filesystem access, conservative protected-path
   filtering, bounded enumeration and complete-line reads, and strict bounded ripgrep
   normalization. Sequence 6 adds the separately enabled transactional `edit_file`;
   Sequence 8 owns the separately enabled local command capability.
3. Implement streamed execution.
4. Emit typed agent events instead of calling `print()` from core code.
5. Add a CLI renderer that consumes events.
6. Implement a local in-memory session.
7. Implement maximum-turn and maximum-tool-call limits.
8. Add input and output guardrails where useful.
9. Add a fake model implementation for deterministic tests.
10. Add tests for:

    * final text response
    * one tool call
    * multiple tool calls
    * malformed tool arguments
    * tool failure
    * maximum-turn termination

The Agents SDK can manage agent turns, tools, sessions, and human-in-the-loop interruptions, but platform-specific state, sandboxing, durable execution, and distributed recovery remain responsibilities of this project.

### Acceptance criteria

* The agent can inspect and modify a sample repository.
* The agent can run tests and react to failures.
* Core tests do not require a real model API.
* The agent core contains no direct Podman, PostgreSQL, Redis, or FastAPI imports.
* Every action produces a typed event.

---

## Phase 2 — Reliable File Editing and Checkpoints

### Goal

Make repository mutation safe, reviewable, and reversible.

### Tasks

1. Create a temporary Git worktree for each run.
2. Never give the agent write access to the user's original checkout.
3. Implement file path validation:

   * reject absolute paths
   * reject `..` traversal
   * resolve symlinks
   * enforce workspace containment
4. Implement optimistic file-version checks.
5. Require exact or uniquely matched edits.
6. Record:

   * pre-edit hash
   * post-edit hash
   * patch hash
   * tool-call ID
7. Create a Git checkpoint before every mutating tool call.
8. Generate a final patch for user review.
9. Implement rewind:

   * stop active commands
   * restore repository snapshot
   * truncate conversation state
   * restore task-plan state
10. Add tests for:

    * conflicting edits
    * repeated patch application
    * symlink escape
    * rollback after failure
    * duplicate tool-call delivery

Sequence 6 implements the edit/worktree portion with one locked descriptor-relative
edit transaction, constant-size canonical patch identities, private detached worktrees,
bounded no-follow untracked-file staging, source content fingerprints, deterministic
Git execution, external-filter rejection, and bounded binary final patches. Sequence 7
implements the checkpoint and rewind portion with serialized, bounded in-memory state;
exact checkpoint and tool-call binding; fail-closed coordinator-contract validation;
cancellation-safe rollback; and removal of later checkpoints when rewinding onto an
earlier branch. The linked worktree may add objects and its own administration data to
the repository's common Git directory, but neither sequence changes a source branch,
source index, source checkout file, or source status. Durable retention and
cross-worker checkpoint ownership remain later persistence responsibilities.

### Acceptance criteria

* Duplicate delivery of an edit tool call does not mutate files twice.
* Rewind restores both repository and conversation state.
* The original repository remains unchanged until final patch application.
* Every mutating operation is associated with a checkpoint.

---

## Phase 3 — Podman Sandbox Runtime

### Goal

Move shell and filesystem execution into an isolated environment.

### Tasks

1. Implement `LocalSandbox` for development tests only.
2. Implement `PodmanSandbox`.
3. Use a non-root user.
4. Drop Linux capabilities.
5. Enable `no-new-privileges`.
6. Keep the container root filesystem read-only.
7. Mount only the temporary workspace.
8. Disable network access by default.
9. Limit:

   * CPU
   * memory
   * PIDs
   * open files
   * output size
   * command duration
10. Do not mount:

    * Podman service socket
    * SSH credentials
    * cloud credentials
    * user home directory
11. Terminate the entire process group on timeout.
12. Destroy the container if process cleanup fails.
13. Add a bounded stdout/stderr stream.
14. Add security tests:

    * read host SSH key
    * escape workspace through symlink
    * write to container root filesystem
    * fork bomb
    * memory exhaustion
    * infinite execution
    * network exfiltration
    * access Podman service socket
15. Add an optional runtime configuration for gVisor later.

Podman rootless mode, namespace isolation, and seccomp should be used where supported;
the default seccomp profile must not be disabled.

Sequence 8 implements only the provider-neutral contract and the explicitly unsafe
local development adapter. Its process boundary serializes start/cancel/close,
terminates children after timeout, output overflow, cancellation, or consumer failure,
invalidates commands queued before cancellation, bounds its environment, concurrency,
direct writes, and snapshot retention, and makes cleanup retryable. Snapshot restore
accepts only exact snapshots owned by the adapter and truncates the abandoned future
branch. These lifecycle guarantees prevent accidental local child leaks; they do not
provide host filesystem, network, privilege, PID, CPU, or memory isolation.

Sequences 9 and 10 now supply those production controls through a rootless
`PodmanSandbox` and an executable hostile-workload suite. The adapter uses one
disposable container per command, an offline network namespace, non-root keep-id
mapping, a read-only root, a bounded tmpfs, all-capability drop,
`no-new-privileges`, and explicit CPU, memory/swap, PID, open-file, duration, and
output limits. Only the private Git worktree is mounted. Tests verify host-credential
and symlink denial, root-write and network failure, unavailable Podman sockets,
resource exhaustion, and destroy-time child cleanup. Production image configuration
requires a SHA-256 digest; local runtime tests use explicit Podman-built tags.

### Acceptance criteria

* Commands cannot access host credentials.
* Network calls fail in offline mode.
* CPU, memory, PID, timeout, and output limits are verified by tests.
* Destroying a sandbox removes all remaining child processes.
* The agent receives structured error results rather than worker crashes.

---

## Phase 4 — LLM Gateway

### Goal

Route all model calls through LiteLLM Proxy.

### Tasks

1. Deploy LiteLLM Proxy through Podman Compose.
2. Configure aliases:

   * `coding-default`
   * `coding-fast`
   * `coding-strong`
   * `summarization`
   * `code-review`
3. Configure at least two providers or deployments.
4. Create a typed gateway client.
5. Normalize:

   * streaming text
   * tool calls
   * finish reasons
   * token usage
   * provider errors
6. Add a stable `request_id` to every model call.
7. Implement idempotency:

   * unseen request → execute
   * running request → attach or return conflict
   * completed request → return stored result
   * same ID with different payload → reject
8. Add timeouts.
9. Add retries with exponential backoff and jitter.
10. Add provider fallback.
11. Add circuit-breaker state.
12. Add tenant and model rate limits.
13. Record token usage and estimated cost.
14. Route context compression to the `summarization` alias.
15. Route difficult retries to `coding-strong`.
16. Ensure the agent worker contains no provider API keys.

LiteLLM Proxy provides a centralized gateway interface with provider normalization, routing, retries and fallback, spend tracking, and rate-limiting hooks.

Implementation status through Sequences 11 and 12:

* LiteLLM is deployed by the Podman Compose stack with all five aliases and two
  deterministic local deployments; the production configuration maps aliases across
  OpenAI and Anthropic.
* Provider credentials are present only on the LiteLLM service. Fake providers,
  workers, the Agents SDK adapter, and the Podman sandbox do not receive them.
* The local deployment has bounded retries and upstream timeouts plus explicit
  compatible fallbacks. Runtime tests stop the primary and prove
  `coding-default` reaches the secondary.
* The `gateway-client` package composes the Agents SDK adapter, allowlists routes,
  revalidates normalized events, enforces terminal-stream invariants, bounds cumulative
  stream events/bytes, and owns cancellation-safe, retryable cleanup that blocks reuse
  after partial cleanup.
* Every `GatewayRequest` requires tenant, session, run, turn, model-call, and stable
  request identifiers. The adapter sends this attribution as protected metadata and
  request headers.
* Items 7, 9, 11, and 12 above—durable idempotency, client exponential backoff,
  circuit-breaker state, and tenant/model rate limits—remain PR 13. LiteLLM's baseline
  deployment fallback is implemented, but the worker client does not add a second
  retry or fallback layer in PR 12.

### Acceptance criteria

* Workers only communicate with the gateway.
* Provider credentials exist only in the gateway deployment.
* Disabling the primary provider causes compatible requests to use fallback.
* Duplicate model request IDs do not create duplicate billable calls where a result is already available.
* Every model call is attributable to a tenant, session, run, and turn.

---

## Phase 5 — Persistent Sessions and HTTP API

### Goal

Expose the local agent through a durable service API.

### Tasks

1. Create PostgreSQL models and Alembic migrations.
2. Persist:

   * sessions
   * runs
   * messages
   * task plans
   * tool calls
   * approvals
   * checkpoints
   * agent events
   * model-call metadata
3. Add FastAPI endpoints:

```text
POST   /v1/sessions
GET    /v1/sessions/{session_id}
POST   /v1/sessions/{session_id}/runs
GET    /v1/runs/{run_id}
POST   /v1/runs/{run_id}/cancel
POST   /v1/runs/{run_id}/approvals/{approval_id}
POST   /v1/runs/{run_id}/rewind
GET    /v1/runs/{run_id}/events
WS     /v1/runs/{run_id}/stream
GET    /health/live
GET    /health/ready
```

4. Add authentication abstraction.
5. Require tenant identity on every request.
6. Add event sequence numbers.
7. Implement reconnect:

```text
Client sends last received sequence.
Server replays all events after that sequence.
Server then continues with live events.
```

8. Ensure WebSocket disconnect does not cancel the run.
9. Add API-level idempotency keys to run creation.

### Acceptance criteria

* Restarting the API does not lose sessions or run state.
* A disconnected client can reconnect without losing events.
* Duplicate run-creation requests return the original run.
* Tenant A cannot read Tenant B’s sessions or events.

---

## Phase 6 — Distributed Workers

### Goal

Run agent tasks on multiple independent workers.

### Tasks

1. Move agent execution out of the API process.
2. Implement a PostgreSQL-backed run queue first using row locking and `SKIP LOCKED`.
3. Start at least three worker processes locally.
4. Add worker registration:

   * worker ID
   * supported sandbox types
   * available slots
   * last heartbeat
5. Implement run leases.
6. Implement heartbeat renewal.
7. Implement scheduler detection of expired leases.
8. Transition expired runs:

```text
RUNNING → LOST → QUEUED
```

9. Restore reassigned runs from their latest checkpoint.
10. Enforce one active writer lease per workspace.
11. Add distributed cancellation.
12. Add graceful worker draining.
13. Add idempotent event and tool-result persistence.
14. Add integration test:

```text
Submit task.
Worker A claims task.
Kill Worker A.
Lease expires.
Worker B reclaims task.
Worker B restores checkpoint.
Task completes without duplicated patch.
```

### Acceptance criteria

* Killing a worker does not permanently lose its task.
* Only one worker owns a run lease at a time.
* Only one run owns a workspace write lease at a time.
* Recovered tasks do not repeat completed mutating tool calls.
* A draining worker claims no new tasks.

---

## Phase 7 — High-Concurrency Controls

### Goal

Prevent overload while supporting many simultaneous sessions.

### Tasks

1. Use asynchronous I/O for model, database, queue, and event operations.
2. Introduce separate capacity limits:

```text
worker agent-run slots
worker sandbox slots
gateway request slots
provider request limits
provider token limits
tenant active-run limits
workspace writer limit
```

3. Implement distributed semaphores or atomic Redis counters where global coordination is required.
4. Implement admission control.
5. Implement per-tenant quotas.
6. Implement priority queues:

   * interactive
   * background
   * evaluation
7. Implement backpressure:

   * stop claiming tasks when worker capacity is full
   * return a structured overload response when queue thresholds are exceeded
8. Add queue-age metrics.
9. Add optional sandbox warm pools.
10. Add load tests for:

    * 10 concurrent runs
    * 50 concurrent runs
    * 100 concurrent runs
    * gateway saturation
    * sandbox saturation
11. Record:

    * throughput
    * queue wait
    * time to first token
    * p50/p95/p99 run latency
    * resource utilization
    * error rate

### Acceptance criteria

* Concurrency remains bounded under load.
* A single tenant cannot consume all worker or gateway capacity.
* When capacity is exhausted, requests queue or fail predictably rather than causing memory exhaustion.
* Load-test results are reproducible and stored under `benchmarks/reports`.

---

## Phase 8 — Context, Memory, and Task Management

### Goal

Implement advanced agent behavior through a composable context pipeline.

### Tasks

1. Define a `ContextContributor` interface.
2. Add contributors for:

   * system instructions
   * project instructions
   * conversation history
   * referenced files
   * active task plan
   * recent tool results
   * long-term memory
   * current Git diff
3. Add token estimation.
4. Add a token budget per model route.
5. Implement context compression through the gateway.
6. Preserve:

   * recent messages
   * active files
   * unresolved task items
   * recent errors
7. Persist the complete uncompressed history.
8. Add memory extraction after completed runs.
9. Require memory provenance:

   * source session
   * source run
   * extraction time
10. Add commands or API operations for:

    * status
    * compact
    * rewind
    * new session
    * task-plan display

### Acceptance criteria

* Context stays below configured limits.
* Compression does not delete the durable original transcript.
* Task state survives process restarts.
* Memory can be disabled per tenant or session.
* Tests verify that critical active-task information survives compression.

---

## Phase 9 — Observability and Reliability

### Goal

Make every run debuggable and measurable.

### Tasks

1. Add OpenTelemetry traces.
2. Propagate:

   * trace ID
   * tenant ID
   * session ID
   * run ID
   * turn ID
   * model-call ID
   * tool-call ID
3. Create spans for:

   * queue wait
   * context construction
   * model request
   * gateway wait
   * tool execution
   * sandbox startup
   * checkpoint creation
4. Export Prometheus metrics.
5. Build Grafana dashboards for:

   * active runs
   * queue depth
   * oldest queued run
   * worker utilization
   * sandbox startup latency
   * provider success rate
   * model latency
   * token rate
   * cost by tenant
   * retries
   * fallbacks
   * circuit-breaker state
6. Add structured error categories.
7. Redact:

   * API keys
   * environment secrets
   * source-code content unless explicitly enabled
8. Integrate Agents SDK traces without making them the only source of system telemetry. The SDK records model generations, tool calls, handoffs, guardrails, and custom spans, but queueing, sandbox, lease, and distributed-worker spans must come from this platform.

### Initial service-level objectives

These are project targets, not production guarantees:

```text
API availability:                  99.5% during test deployment
Lost accepted runs:                0
Duplicate committed file patches:  0
Worker failure detection:          under 30 seconds
Event reconnect recovery:          under 5 seconds
Gateway request success:           over 99% excluding invalid requests
```

Do not publish latency or concurrency claims on the résumé until they have been measured.

---

## Phase 10 — Kubernetes Deployment

### Goal

Deploy independently scalable services.

### Workloads

Use Deployments for:

* Agent API
* Scheduler
* Agent workers
* Event gateway
* LiteLLM Proxy

Use managed or stateful services for:

* PostgreSQL
* Redis
* object storage

### Tasks

1. Add Kubernetes manifests or Helm charts.
2. Add:

   * resource requests
   * resource limits
   * readiness probes
   * liveness probes
   * PodDisruptionBudgets
   * topology spread constraints
   * anti-affinity where appropriate
3. Store secrets in Kubernetes Secrets or an external secret manager.
4. Add separate service accounts.
5. Apply least-privilege RBAC.
6. Isolate sandbox workers on dedicated nodes where possible.
7. Add NetworkPolicies:

   * workers may access gateway, database, queue, and object storage
   * sandboxes have no default external network
   * external model providers are reachable only from the gateway
8. Configure Horizontal Pod Autoscaling:

   * API based on request or CPU load
   * workers based on queue depth and oldest-task age
   * gateway based on active requests and latency
9. Add graceful termination:

   * mark worker draining
   * stop task claims
   * checkpoint active runs
   * release or finish leases
10. Add rolling-deployment tests.

Kubernetes HPA can scale workloads from CPU, memory, or custom metrics. For workers, queue depth and oldest-task wait are more meaningful than CPU alone.

### Acceptance criteria

* Increasing queue depth causes worker replicas to scale up.
* Removing a worker Pod does not lose accepted tasks.
* Deployments drain active work or recover it from checkpoints.
* Model-provider credentials are unavailable to agent-worker Pods.

---

## Phase 11 — Evaluation, Load, and Chaos Testing

### Goal

Generate defensible evidence for project quality and résumé claims.

### Coding-task evaluation

Create 30–100 deterministic repository tasks:

* Fix a failing unit test
* Add an API endpoint
* Repair a type error
* Perform a multi-file rename
* Add validation
* Refactor duplicated code
* Update dependency usage
* Fix a concurrency bug
* Add missing tests
* Diagnose an exception

Record:

```text
task success
tests passed
iterations
tool calls
input tokens
output tokens
cost
queue wait
first-token latency
total latency
retries
fallbacks
permission denials
```

### Load testing

Test:

* API run submission
* WebSocket connections
* event throughput
* distributed worker saturation
* gateway rate limiting
* provider fallback
* PostgreSQL contention
* Redis contention

### Chaos tests

Automate:

1. Kill an active worker.
2. Restart Redis.
3. Temporarily block PostgreSQL.
4. Disable the primary model provider.
5. Return gateway 429 responses.
6. Disconnect WebSocket clients.
7. Force sandbox OOM.
8. Deliver duplicate task messages.
9. Deliver duplicate tool calls.
10. Terminate a worker during checkpoint creation.

### Acceptance criteria

* Every failure scenario has a documented expected outcome.
* No accepted task silently disappears.
* Duplicate messages do not create duplicate committed changes.
* Dashboards show the failure and recovery.
* Benchmark reports include methodology and hardware configuration.

---

# 10. Permission Model

Classify tools by risk.

```text
READ_ONLY
    list files
    read files
    search code

WORKSPACE_WRITE
    edit file
    create file
    delete file
    apply patch

COMMAND
    run tests
    execute build
    run arbitrary shell command

NETWORK
    install dependencies
    access external APIs

PROHIBITED
    privileged execution
    host filesystem mounts
    Podman service socket access
    secret access
```

Approval modes:

```text
MANUAL
    all mutating and command tools require approval

AUTO_SAFE
    known-safe reads and test commands are automatically approved

AUTO_WORKSPACE
    workspace changes are approved, network and dangerous commands are not

LOCKED_DOWN
    read-only tools only
```

The permission policy decides whether an operation is allowed.

The sandbox enforces the maximum possible impact even after permission is granted.

---

# 11. Failure Semantics

## Model timeout

* Gateway retries transient provider failures.
* The agent does not create a new logical reasoning attempt until gateway retries are exhausted.

## Malformed model tool call

* Record the malformed response.
* Return a structured validation failure to the agent.
* Count against the agent’s semantic retry budget.

## Worker crash

* Lease expires.
* Run becomes `LOST`.
* Scheduler requeues it.
* A new worker loads the latest checkpoint.
* Completed idempotent tool results are reused.

## Sandbox crash

* Record command failure.
* Destroy the sandbox.
* Recreate it from the latest workspace checkpoint.
* Allow the agent to decide whether to retry.

## Database outage

* Do not report success unless durable state has been committed.
* Stop accepting new work if run creation cannot be persisted.
* Preserve active work through bounded retries.
* Do not acknowledge queue items whose completion state is not durable.

## Client disconnect

* Agent continues running.
* Events remain durable.
* Client reconnects using the last received sequence.

## Duplicate task delivery

* Claim or transition uses compare-and-set semantics.
* Only the current lease owner may continue execution.

## Duplicate tool-call delivery

* Check the stable tool-call ID.
* Return the previously persisted result when already completed.
* Reject the same ID with different arguments.

---

# 12. Security Requirements

1. Provider API keys exist only in the LLM gateway.
2. Sandboxes receive no host cloud credentials.
3. Sandboxes use temporary workspaces.
4. Network is disabled by default.
5. Host Podman service socket is never mounted.
6. Containers do not run privileged.
7. Root filesystem is read-only.
8. Workspace paths are canonicalized.
9. Symlinks cannot escape the workspace.
10. Logs redact secrets.
11. Tenant IDs are enforced in database queries.
12. Every administrative action is audited.
13. Container images are pinned by digest for releases.
14. Dependency and image scanning runs in CI.
15. Approval decisions are stored durably.
16. Tool arguments and decisions are included in the audit trail, subject to source-code privacy settings.

---

# 13. Codex Implementation Rules

Codex must follow these rules while implementing this design:

1. Implement one phase at a time.
2. Keep every pull request small enough to review.
3. Add or update tests in every pull request.
4. Do not add empty interfaces without a consumer planned in the current or next phase.
5. Do not silently catch exceptions.
6. Do not use global mutable state.
7. Do not place provider-specific logic in `agent-core`.
8. Do not let workers call model providers directly.
9. Do not execute shell commands on the host outside explicit development tests.
10. Do not claim security or scale properties that are not tested.
11. Do not introduce Kubernetes before the Podman Compose system works.
12. Do not optimize routing before baseline metrics exist.
13. Prefer deterministic routing policies over LLM-selected routing.
14. Use typed domain models and validated state transitions.
15. Treat retries as potentially duplicated execution.
16. Preserve all durable identifiers across recovery.
17. Run lint, type checks, unit tests, and integration tests before completing each phase.
18. Update `DESIGN.md` when implementation decisions diverge from this document.
19. Record meaningful architectural changes as ADRs.
20. Stop and report clearly when an acceptance criterion cannot be satisfied.

---

# 14. Suggested Pull-Request Sequence

```text
PR 01  Repository bootstrap and CI
PR 02  Core agent events and domain models
PR 03  Fake model and deterministic agent-loop tests
PR 04  OpenAI Agents SDK integration
PR 05  Read/search/file tools
PR 06  Reliable edit tool and Git worktrees
PR 07  Checkpoints and rewind
PR 08  Local sandbox abstraction
PR 09  Hardened Podman sandbox
PR 10  Sandbox security tests
PR 11  LiteLLM Proxy deployment
PR 12  Typed gateway client and normalized streaming
PR 13  Gateway retry, fallback, and idempotency
PR 14  PostgreSQL persistence and migrations
PR 15  FastAPI session and run APIs
PR 16  Durable event store and WebSocket replay
PR 17  PostgreSQL task queue
PR 18  Worker leases and heartbeats
PR 19  Failure recovery and idempotent tool replay
PR 20  Workspace writer leases
PR 21  Bounded concurrency and tenant quotas
PR 22  Backpressure and priority scheduling
PR 23  Context pipeline and compact
PR 24  Long-term memory and task tracking
PR 25  OpenTelemetry and Prometheus metrics
PR 26  Grafana dashboards
PR 27  Load-test suite
PR 28  Chaos-test suite
PR 29  Kubernetes manifests
PR 30  HPA, graceful draining, and deployment tests
PR 31  Coding-task evaluation harness
PR 32  Final benchmark report and architecture documentation
```

Each PR must include:

* Motivation
* Design summary
* Tests
* Operational impact
* Security impact
* Migration instructions, when applicable

---

# 15. Final Definition of Done

The project is complete when all of the following are true:

### Coding agent

* Can inspect, edit, and test multi-file repositories.
* Supports persistent sessions and streamed events.
* Supports approval workflows.
* Supports context compression.
* Supports task tracking.
* Supports checkpoints and rewind.

### Sandbox

* All untrusted commands run in hardened containers.
* Resource and network limits are tested.
* Host credentials are inaccessible.
* Workspace changes are isolated and reviewable.

### Distributed system

* At least three workers can process tasks concurrently.
* Runs use durable queue state.
* Workers use leases and heartbeats.
* Failed workers’ tasks recover on another worker.
* Tool execution is idempotent.
* Workspace ownership prevents concurrent writers.

### Concurrency

* Capacity is bounded.
* Backpressure is implemented.
* Per-tenant and provider quotas exist.
* Load-test results include p50, p95, and p99 metrics.
* Horizontal scaling is demonstrated.

### LLM gateway

* All model calls use LiteLLM Proxy.
* At least two provider deployments are configured.
* Routing, rate limiting, retries, fallback, and circuit breaking are tested.
* Token and cost usage is attributable per run.
* Provider credentials are isolated from agent workers.

### Reliability and observability

* Events support reconnect and replay.
* Major operations produce traces and metrics.
* Failure scenarios are covered by chaos tests.
* No accepted tasks are silently lost during tested failures.
* Benchmark and architecture reports are committed.
