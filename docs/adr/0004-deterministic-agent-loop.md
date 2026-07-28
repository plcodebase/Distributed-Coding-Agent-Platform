# ADR 0004: Deterministic agent-loop boundary

- Status: Accepted
- Date: 2026-07-28

## Context

Sequence 3 introduces the executable agent loop and deterministic model tests before
Sequence 4 integrates the OpenAI Agents SDK and before Sequence 5 supplies concrete
repository tools. The loop must therefore exercise real orchestration behavior without
depending on a provider API, SDK runtime, sandbox, database, or network service.

The design requires streamed model output, typed agent events, centrally routed model
calls, validated tool arguments, and hard turn/tool-call limits. It also requires
malformed model behavior and unexpected failures to fail closed without leaking
untrusted exception text.

## Decision

- Define `ModelGateway.stream(GatewayRequest)` as the only model boundary available to
  the core loop. Gateway requests, messages, tool definitions, tool calls, text deltas,
  completion usage, and finish reasons are immutable provider-neutral models.
- Keep provider parsing, authentication, routing, retries, and fallback outside
  `agent-core`. Sequence 4 may adapt the Agents SDK to this contract; the later typed
  gateway client may adapt LiteLLM without changing loop policy.
- Use `ScriptedModelGateway` for deterministic tests. It consumes a fixed sequence of
  turns, records every normalized request, requires no credentials or network, and
  exposes no provider-specific response shape.
- Register tools with a closed Pydantic `ToolArguments` type. `ToolRegistry.prepare`
  validates and freezes every model-generated argument object before creating an
  executable operation. Unknown tools and schema failures become sanitized structured
  errors and never invoke a handler.
- Emit existing typed Sequence 2 events for run start, context construction, every
  model request and text delta, accepted tool calls, tool execution/output/completion,
  and terminal success or failure. Sequence numbers are contiguous within one loop
  execution; durable allocation and uniqueness remain event-store responsibilities.
- Bound every loop with configurable maximum turns, maximum tool calls, semantic retry
  budget, model and tool timeouts, model output bytes, tool argument/result bytes, and
  tool output bytes. Limits are measured in UTF-8 bytes.
- Return sanitized tool failures to the conversation so the model can react on a later
  turn. Schema failures and unknown tools consume the semantic retry budget. Gateway
  retries remain gateway-owned and do not create extra semantic turns.
- Inject the clock and identifier source. Tests use stepping time and sequential IDs;
  core execution does not depend on global clock, random, or provider state.
- Buffer fake tool result chunks in Sequence 3, then emit bounded stdout/stderr events.
  Concrete command/sandbox streaming and durable tool replay remain assigned to later
  tool and sandbox sequences.

## Consequences

- Core loop tests are deterministic, run without a real model API, and cover final
  text, one and multiple tool calls, malformed arguments, tool failure, and bounded
  termination.
- Tool handlers cannot observe raw arguments that failed their declared schema.
- Unexpected gateway and tool exception messages are not copied into events or model
  feedback, reducing accidental secret disclosure.
- The current loop is single-process and does not persist messages, model calls, tool
  results, or events. Persistence, approvals, checkpoints, replay, stable duplicate
  suppression, and cross-worker recovery remain later-sequence responsibilities.
- Sequence 3 does not inspect or mutate a real checkout. Concrete workspace tools and
  their sample-repository acceptance tests begin in Sequence 5.
