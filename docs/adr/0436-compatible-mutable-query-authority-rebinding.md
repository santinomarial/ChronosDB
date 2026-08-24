# ADR 0436: Compatible mutable query authority rebinding

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB cluster, query, Raft, and networking maintainers
- **Extends:** [ADR 0434](0434-proof-bound-mutable-vector-query-execution.md),
  [ADR 0435](0435-bounded-mutable-vector-query-tcp-scheduling.md)

## Context

The mutable TCP scheduler correctly kept fragment authority immutable and surfaced hints, but a
retryable terminal failure still required constructing a new scheduler. An embedding needed a
bounded way to install freshly acquired fragment authority without changing the logical query,
losing cumulative metrics, resetting the whole-operation deadline, or accidentally reusing partial
results.

## Decision

`DistributedMutableVectorQueryExecution` retains an explicit logical identity. It contains the
query, database, table, destination schema, read policy, projection, event-time predicate, vector
plan, result schema, and plan-ordered tablet/Raft-group pairs. Creation now also requires projection
and event-time predicate equality across all fragments. Serving node, exact applied and observed
positions, placement epoch, and linearizable barrier are excluded because those are the authority
fields a fresh bind must replace.

`DistributedMutableVectorQueryTcpExecution::rebind` accepts a complete newly constructed portable
execution and TCP configuration only after the current execution has terminally failed with
`UNAVAILABLE`, `RESOURCE_EXHAUSTED`, or `IO_ERROR`. It requires exact logical-identity equality,
the same whole-operation deadline and rebind budget, and remaining bounded rebind capacity. Normal
scheduler creation then independently revalidates every replacement route before ownership swap.

A successful replacement discards all prior failed sender/coordinator/client state, increments the
rebind count, and carries cumulative attempt/retry/completion/failure metrics forward. No previous
result prefix exists to merge. A failed compatibility or construction check leaves the prior failed
owner intact.

## Consequences

Leader, position, placement, and barrier changes can be installed only through a complete fresh
fragment set. Query semantics, tablet membership/order, and tablet-to-group identity cannot drift.
The embedding remains responsible for acquiring correlated current authority and routes before
calling `rebind`; the poll owner performs no blocking Raft or DNS acquisition itself.

Compatibility comparison is linear in bounded query metadata. One thread serializes failure and
replacement, so no inter-thread memory-ordering argument applies. No durable or network format
changes.

## Affected invariants

- [Invariant 4](../architecture/invariants.md): replacement positions come only from a newly bound
  fragment set.
- [Invariant 5](../architecture/invariants.md): stale bytes are never retargeted to a new leader.
- [Invariant 6](../architecture/invariants.md): logical query and tablet/group identity remain
  exact across authority changes.
- [Invariant 15](../architecture/invariants.md): rebind count and original whole-operation deadline
  remain bounded.
- [Invariant 18](../architecture/invariants.md): replacement cannot weaken logical or proof
  guarantees.

## Validation

A real loopback test exhausts a one-attempt stale route, observes terminal `IO_ERROR`, rejects a
fresh execution with a different query identity, then installs higher position/placement authority
for a different serving leader. The replacement completes over mutual TLS and preserves two total
attempts, one failed and one completed transport, one rebind, and zero live clients. A late rebind
after completion is rejected.

## Migration and rollback

This is additive and is not yet installed in native query request handling. Rollback removes
logical-identity introspection and explicit scheduler replacement without changing any bytes.

## References

- [Correlated replicated read authority](0299-correlated-replicated-read-authority.md)
- [Bounded mutable vector query TCP scheduling](0435-bounded-mutable-vector-query-tcp-scheduling.md)
- [Authoritative co-located native query redirect](0428-authoritative-co-located-native-query-redirect.md)
