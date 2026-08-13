# ADR 0339: Finite grouped-query sender

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB cluster, query, and networking maintainers
- **Extends:** [ADR 0169](0169-bounded-distributed-query-carrier-lifecycle.md),
  [ADR 0330](0330-distinct-grouped-float64-query-transport.md),
  [ADR 0336](0336-deadline-bound-grouped-query-tcp-client.md)

## Context

The grouped TCP client owned one connection attempt and withheld incomplete response prefixes, but
no portable policy owner could construct attempts, exact-correlate the complete response vector,
apply finite retry/backoff, or expose advisory leader hints without mutating the proof-bound target.
Letting socket code own retries would duplicate policy and risk carrying partial grouped state into
a later attempt.

## Decision

`DistributedGroupedQuerySender` is a move-only, single-threaded deterministic state machine for one
immutable group-scoped dispatch. It validates the source, target, encodability, attempt limit, and
positive capped exponential-backoff range at construction. `begin_attempt` permits only one
outstanding attempt and independently encodes one value-owned canonical request.

`accept_responses` accepts only a nonempty terminally closed response vector. Before state mutation,
it exact-matches every reverse route, query, tablet, payload query/tablet, one-based sequence, and
terminal position. Terminal-only is valid only as the sole sequence-one payload. A failure must be
the sole response and carry no payload. Advisory hints require nonzero node and placement epoch.

Only after the complete successful vector validates and copies does the sender publish its
value-owned payload result and enter `Succeeded`. Correlation, sequence, hint, or allocation failure
publishes no prefix and leaves the outstanding attempt intact. Retryable `UNAVAILABLE`,
`RESOURCE_EXHAUSTED`, and `IO_ERROR` responses or transport failures schedule one whole new attempt
under the finite capped backoff; other statuses and attempt exhaustion are terminal.

Leader hints are exposed but never change the immutable target or dispatch. Following one requires
fresh external authority acquisition and a new execution owner. The sender owns no socket, clock,
coordinator, or durable state and changes no network bytes.

## Consequences and validation

Retained memory is one bounded dispatch, one optional bounded successful response vector, and
constant retry metadata. Each attempt allocates one bounded request. Work is linear in the complete
response vector. One owner thread serializes calls, so no synchronization or memory-ordering
argument is required.

Two focused cases prove exact attempt bytes, two-part success, terminal-only success, payload-level
correlation rejection without state mutation, terminal result retention, advisory leader capture,
exact capped backoff, transport failure, terminal status, attempt exhaustion, and absence of a
partial result across retries. The installed-consumer gate references sender construction.

Coordinator delivery, multi-tablet TCP scheduling, whole-query cancellation/deadlines, packaged
grouped execution, process failover, and broad fault/measurement evidence remain incomplete. No
Phase 16 exit gate is claimed.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Bounded distributed query carrier lifecycle](0169-bounded-distributed-query-carrier-lifecycle.md)
- [Distinct grouped FLOAT64 query transport](0330-distinct-grouped-float64-query-transport.md)
- [Deadline-bound grouped-query TCP client](0336-deadline-bound-grouped-query-tcp-client.md)
- [Bounded grouped FLOAT64 coordinator](0324-bounded-grouped-float64-coordinator.md)
