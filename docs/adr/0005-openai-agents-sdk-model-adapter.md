# ADR 0005: OpenAI Agents SDK model adapter

- Status: Accepted
- Date: 2026-07-28

## Context

Sequence 4 integrates the OpenAI Agents SDK after Sequence 3 established the
provider-neutral `ModelGateway` and deterministic `AgentLoop`. The SDK also offers a
`Runner` that owns turns and tool execution. Using both loops would duplicate or bypass
the core's argument validation, event sequencing, byte and call limits, secret
redaction, and same-run duplicate suppression.

The local centralized gateway exposes an OpenAI-compatible Chat Completions endpoint.
Provider-specific parsing and credentials must remain outside `agent-core`, and tests
must not require a model API key or external service.

## Decision

- Keep `AgentLoop` as the only production orchestration and safety-policy owner. Do not
  use SDK `Runner`, sessions, tool callbacks, handoffs, or approval state.
- Add `agents-sdk-adapter` as a separate workspace package. It depends on `agent-core`
  and OpenAI Agents SDK, while foundation package tests reject `agents`, `openai`, and
  `httpx` imports.
- Implement `OpenAIAgentsGateway` against the SDK `ModelProvider` and
  `Model.stream_response` interfaces. Route names select SDK models without exposing
  provider response types to core.
- Use a per-instance `AsyncOpenAI` client and `OpenAIProvider` in Chat Completions mode.
  Normalize the configured base URL to `/v1`, disable client retries, enable strict SDK
  feature validation, and buffer fragmented provider tool calls until complete.
- Keep SDK tool definitions schema-only. Their callbacks always fail if invoked.
  Preserve the core JSON schema exactly instead of enabling SDK strict-schema rewriting,
  which would make optional Pydantic fields required. Core validation remains
  authoritative.
- Convert only leading system messages to SDK system instructions. Preserve user,
  assistant, function-call, and function-output ordering as Responses-format input
  items. Reject a system message after conversation content.
- Emit text only from `ResponseTextDeltaEvent` and tool calls only from completed
  `ResponseFunctionToolCall` items. Parse arguments with finite JSON semantics and emit
  sanitized invalid-call metadata without retaining rejected raw arguments.
- Map completed, incomplete, refusal, failed, and error events into the existing gateway
  event union. Do not fabricate an upstream provider name; use the reported model only
  when it satisfies the core identifier contract.
- Treat unknown output items, mixed refusal/tool output, invalid call metadata, stream
  data after completion, and streams without a terminal event as structured failures.
- Disable SDK tracing per model call. Platform telemetry remains authoritative until the
  later observability phase supplies a secret-safe SDK trace processor.
- Close normalized streams on consumer cancellation. The adapter closes its provider and
  any `AsyncOpenAI` client created by the production factory; injected HTTP clients
  remain caller-owned.

## Consequences

- The production model path now uses the OpenAI Agents SDK without changing the
  deterministic loop, tool, event, or gateway contracts established in Sequence 3.
- Unit tests inject fake SDK models, while an integration test sends fragmented SSE
  through the real SDK Chat Completions model and an in-process OpenAI-compatible server.
- The SDK's Chat Completions converter currently normalizes provider
  `finish_reason="length"` into `response.completed`. The adapter correctly maps
  `ResponseIncompleteEvent` reasons when an SDK model provides them, but exact length
  detection for this Chat Completions implementation remains an SDK limitation.
- Concrete repository tools, SDK `Runner`, durable sessions, retries, fallback, and
  gateway transport normalization remain assigned to later sequences.
