# ADR 0528: Finite immutable grouped shuffle result retry

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB distributed-query and networking maintainers
- **Extends:** [ADR 0527](0527-bounded-grouped-shuffle-result-mutual-tls.md)

## Context

One result TLS attempt consumes its stream cursor. Reconnecting must neither resume an ambiguous
partial offset nor rebuild a different partition after authority drift. A receipt lost after
coordinator acceptance also requires the reducer to resend the exact same bytes until duplicate
collection becomes idempotent.

## Decision

Add a move-only, single-thread-affine retry owner for one authority partition. It borrows the exact
shuffle authority and raw result schema, owns the source/coordinator route and canonical encoded
result batches, and prevalidates a complete stream before publication. Every `begin_attempt`
constructs a fresh byte-identical stream with a monotonic attempt number and unchanged coordinator
target. Construction failure consumes no attempt budget and leaves policy state unchanged.

Only the correlated result receipt reports success. `UNAVAILABLE`, `IO_ERROR`, and
`RESOURCE_EXHAUSTED` retry after positive capped exponential backoff. Other failures are terminal.
Attempt count is capped at 1,024 and time addition saturates. The owner has no socket or clock and
performs no address rotation, duplicate installation, or lifecycle scheduling.

## Consequences

TCP reconnect can now consume fresh immutable result attempts without depending on prior write
progress. Lost receipts may still yield duplicate complete streams; the coordinator collector must
deduplicate exact partition attempts before global finalization.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): query, schema, partition, nodes, and bytes remain
  immutable across attempts.
- [Invariant 10](../architecture/invariants.md): every attempt is reconstructed through the exact
  result frame and stream codecs.
- [Invariant 11](../architecture/invariants.md): retained batches, active cursor, and borrowed proof
  lifetimes are explicit.
- [Invariant 15](../architecture/invariants.md): attempts, backoff, frames, and bytes are finite.
- [Invariant 18](../architecture/invariants.md): retry cannot change result partition semantics or
  make write completion equivalent to acceptance.

## Validation plan

Compare the exact bytes of three reconstructed multi-frame attempts, exercise active-attempt
exclusion, exact and capped backoff boundaries, finite exhaustion, receipt-only success, and
permanent authentication failure. Sweep policy prevalidation and attempt reconstruction
allocations while proving the budget remains untouched. Run cluster, allocation-failure,
sanitizer, formatting, static-analysis, and diff gates.

## Migration or rollback considerations

No durable or wire bytes change. Rollback removes the policy owner; result return must then fail
after one connected attempt rather than retry from partial state.

## Unresolved questions

- Add deadline-bound result TCP connect and bounded listener admission.
- Deduplicate exact completed partition attempts at the coordinator.
- Schedule all required remote partition results under one query deadline.

## References

- [Result TLS decision](0527-bounded-grouped-shuffle-result-mutual-tls.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
