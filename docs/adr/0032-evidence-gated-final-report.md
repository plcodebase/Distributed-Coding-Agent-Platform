# ADR 0032: Evidence-gated final report

- Status: Accepted
- Date: 2026-08-11
- Sequence: 32

## Context

Combining simulated and live benchmark output without provenance can turn test fixtures into false
performance claims.

## Decision

- Compile only the five versioned deployment, coding, load, chaos, and quality-gate schemas.
- Reject duplicate JSON keys, unsupported versions, mixed revisions, duplicate artifact identities,
  inconsistent counts, and optional trusted-index digest mismatches.
- Record methodology, environment/hardware, source revision, timestamps, artifact digests,
  acceptance results, and limitations.
- Permit performance metrics only from live artifacts. Simulation may prove harness contracts but
  cannot populate measured metrics.
- Require both deployment-removal modes, all ten coding categories and at least thirty tasks, every
  load scenario, every chaos scenario, and every fixed quality gate. One artifact of a broad kind is
  not campaign completeness.
- Evaluate intentional load failures with scenario-specific expectations; a measured rate-limit
  campaign is not failed merely because it observed the configured 429 responses.
- Derive the default report timestamp from the latest validated source artifact so unchanged inputs
  compile byte-for-byte. Source generation and final compilation are separate operations.
- Emit `complete` only when every required acceptance item has complete live verified evidence from
  a clean revision; emit
  `incomplete` for missing/live-pending evidence and `failed` for a failed supplied artifact.

## Consequences

The committed baseline can be honest about external dependencies. Production and résumé claims must
come from a later live evidence set tied to a committed revision.
