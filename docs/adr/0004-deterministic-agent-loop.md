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
  completion usage, malformed-call rejections, and finish reasons are immutable
  provider-neutral models.
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
- Require tool handlers to return one ordered async stream of typed stdout/stderr chunks
  followed by exactly one structured result. The core passes explicit output/result
  ceilings, consumes output incrementally, closes the producer at the byte ceiling, and
  rejects incomplete or post-terminal streams.
- Emit existing typed Sequence 2 events for run start, context construction, every
  model request and text delta, accepted tool calls, tool execution/output/completion,
  and terminal success or failure. Sequence numbers are contiguous within one loop
  execution; durable allocation and uniqueness remain event-store responsibilities.
- Bound every loop with configurable maximum turns, maximum tool calls, semantic retry
  budget, context messages, serialized gateway-request bytes, model and tool timeouts,
  model output bytes, tool argument/result bytes, and tool output bytes. Limits are
  measured in UTF-8 bytes.
- Return sanitized tool failures to the conversation so the model can react on a later
  turn. Schema failures and unknown tools consume the semantic retry budget. Gateway
  retries remain gateway-owned and do not create extra semantic turns.
- Inject the recursive secret redactor at the loop boundary. Initial context, tool
  stdout/stderr, structured results, and external structured errors are redacted before
  gateway, event, or transcript use. Model-text deltas pass through a stateful redactor
  that retains only a bounded conservative suffix across fragments, then emits event-sized
  sanitized chunks. Both raw input and expanded redacted output remain subject to the model
  byte ceiling. Model calls containing sensitive arguments are rejected without placing
  their raw arguments in events.
- Represent provider argument parse failures as rejected normalized tool calls. The
  existing `model.tool_call_received` event records safe metadata and an error instead
  of arguments. A mixed valid/invalid turn is rejected atomically and consumes one
  semantic retry.
- Cache terminal tool outcomes by stable call ID and canonical argument hash for the
  life of one loop. An identical duplicate reuses the outcome; a conflicting hash fails
  the run before duplicate execution. Durable replay remains event-store work.
- Inject the clock and identifier source. Tests use stepping time and sequential IDs;
  core execution does not depend on global clock, random, or provider state.
- Keep model turns, tool turns, event sequence allocation, and loop coordination in
  separate internal components. Concrete command/sandbox process streaming and durable
  replay remain assigned to later tool, sandbox, and persistence sequences.

## Consequences

- Core loop tests are deterministic, run without a real model API, and cover final
  text, one and multiple tool calls, malformed arguments, tool failure, and bounded
  termination.
- Tool handlers cannot observe raw arguments that failed their declared schema.
- Tool output capture is bounded while it is produced, preserves channel ordering, and
  cannot silently continue after truncation.
- Provider JSON parse failures and sensitive arguments receive bounded model feedback
  without disclosing their raw values.
- Unexpected gateway and tool exception messages are not copied into events or model
  feedback. Known and patterned secrets in structured tool data are redacted as well.
- Secrets split at any provider-fragment boundary are redacted before events, transcript,
  or final output observe them; the redactor does not require buffering an entire turn.
- The current loop is single-process and does not persist messages, model calls, tool
  results, or events. Persistence, approvals, checkpoints, replay, stable duplicate
  suppression across attempts, and cross-worker recovery remain later-sequence
  responsibilities.
- Sequence 3 does not inspect or mutate a real checkout. Concrete workspace tools and
  their sample-repository acceptance tests begin in Sequence 5.
