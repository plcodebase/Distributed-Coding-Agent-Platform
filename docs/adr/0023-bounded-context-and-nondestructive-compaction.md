# ADR 0023: Bounded context and non-destructive compaction

- Status: Accepted
- Date: 2026-07-31
- Sequence: 23

## Context

Durable transcripts grow beyond model route limits, but deleting or rewriting messages
would make recovery, audit, and rewind unreliable.

## Decision

- Assemble context through independent typed contributors for every Phase 8 source.
- Reserve output capacity and use conservative serialized UTF-8 byte accounting as the
  provider-neutral token upper bound.
- Mark recent messages, active files, unresolved/unknown task items, and recent errors
  critical. Completed/cancelled tasks and plan metadata are compressible; bounded
  legacy plans are chunked before entering a fragment.
- Perform compression through the existing gateway on the `summarization` route, with
  tool calls rejected and all output bounded, redacted, and validated.
- Persist explicit compaction requests with idempotency keys and source-message
  watermarks. Serialize creation under the session lock and enforce one pending request
  with a partial unique index. Record the actual `summarization` route, store summaries
  and usage as new records, and never mutate source messages.

## Consequences

The platform can prove every gateway request fits its configured route budget and can
rebuild from the uncompressed transcript. Conservative estimation can compact earlier
than a provider tokenizer would require.

## Migration

Revision `0005` creates append-only context-compaction metadata.
