# ADR 0021: Bounded capacity and tenant quotas

- Status: Accepted
- Date: 2026-07-31
- Sequence: 21

## Context

Worker slots alone do not prevent one tenant or a saturated model route from consuming
all shared capacity. Provider calls also reserve output tokens before actual usage is
known.

## Decision

- Enforce separate worker run and sandbox semaphores.
- Persist tenant active-run, queued-run, and gateway-request ceilings in PostgreSQL.
- Acquire expiring gateway capacity leases atomically across tenant request slots,
  route request slots, and a fixed token window.
- Renew exact, unexpired gateway leases while provider work remains live. Renewal loss
  cancels the request and fails closed; release always uses the latest lease identity.
- Reserve output tokens before provider contact and reconcile terminal usage.
- Keep expired reservations charged until the token window resets; this fails
  conservatively after an interrupted call.
- Return typed retryable capacity failures without provider contact.
- Reconcile platform-owned route limits from validated deployment configuration after
  locking the durable route row. Tenant quota rows remain explicit durable overrides.

## Consequences

Capacity remains bounded across processes and a tenant cannot consume every run or
gateway slot. Conservative token accounting may temporarily underutilize a route after
a crashed request.

## Verification

Unit tests cover local and PostgreSQL claims, renewal, lease loss, expiry,
reconciliation, cancellation, tenant queue admission, active-run claim skipping, and
independent sandbox saturation.

## Migration

Revision `0003` creates tenant quota, provider capacity, and expiring gateway lease
tables.
