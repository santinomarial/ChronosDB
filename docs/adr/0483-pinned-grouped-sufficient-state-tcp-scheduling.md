# ADR 0483: Pinned grouped sufficient-state TCP scheduling

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0342](0342-pinned-grouped-query-tcp-scheduling.md),
  [ADR 0476](0476-portable-pinned-grouped-sufficient-state-execution-owner.md), and
  [ADR 0480](0480-deadline-bound-grouped-sufficient-state-tcp-client.md)

## Context

The portable grouped sufficient-state execution owner pinned one compatible Manifest snapshot,
validated cross-tablet key/state authority, accepted complete canonical worker batches, and merged
them in plan order. The finite sender and nonblocking TCP client separately owned retry policy,
response reconstruction, authentication, and one connection attempt. No owner joined those pieces
for every tablet, so production callers still had to invent route validation, retry/address
rotation, polling, cancellation, and atomic delivery policy.

## Decision

`DistributedVectorGroupedAggregateQueryTcpExecutionV2` owns the portable execution and one slot per
plan-ordered tablet. Each slot owns one finite grouped sender and at most one active grouped TCP
client. Construction validates the source node, authentication dependencies, all carrier limits,
the grouped authority width, and a complete unique route table before opening any descriptor. Route
addresses are nonzero and unique within their target, and every planned serving node must resolve.

The scheduler creates each sender from the pinned dispatch, exact result schema, complete grouped
authority, and the portable execution's shared query resource context. Sender attempt number
selects an address modulo the target node's finite ordered endpoint list. Advisory leader hints are
observable but never rewrite route or snapshot authority.

One thread calls `poll_once`. It starts only ready or due-backoff attempts, maintains preallocated
descriptor and slot-index tables, caps the caller's wait by the earliest retry and whole-query
deadline, and drives each nonblocking client once per readiness result. Retryable connection and
carrier failures discard the complete attempt and return policy to the sender. Local resource
exhaustion and terminal sender failures poison the whole execution.

A successful sender's canonical nested frame vector is delivered to the portable execution exactly
once. No row is exposed until every sender succeeds and the portable coordinator closes globally.
Only then does `next` delegate to the merged grouped physical output. Explicit cancellation,
deadline expiry, or terminal failure destroys every active client and resets the active-attempt
metric before returning. The scheduler does not perform Native protocol finalization, computed
pre-group plan splitting, or shuffle routing.

## Alternatives considered

- **Let callers loop over one-attempt clients:** rejected because atomic publication, deadline
  arbitration, and whole-query cancellation would remain convention rather than an invariant.
- **Rewrite targets from leader hints:** rejected because hints are not authority and may be stale.
- **Feed response prefixes directly to the coordinator:** rejected because retries replace whole
  attempts and the grouped stream is publishable only after canonical terminal reconstruction.
- **Allocate poll tables on every call:** rejected because tablet cardinality is known at creation.

## Consequences

The outbound grouped sufficient-state path now has a finite all-tablet owner with complete route
preflight, deterministic address rotation, retry/backoff, earliest-deadline polling, cancellation,
metrics, and all-or-nothing row publication. The Manifest pin, exact grouped authority, immutable
dispatches, and shared query-memory authority survive every attempt.

Memory is bounded by the finite tablet/route configuration, sender and carrier frame/byte limits,
the portable coordinator, and the shared query-memory ceiling. One thread serializes all calls, so
no synchronization or inter-thread memory-ordering argument applies.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): scheduling changes no durable or wire bytes.
- [Invariant 6](../architecture/invariants.md): routes, attempts, waits, frames, bytes, coordinator
  retention, and query memory are finite.
- [Invariant 10](../architecture/invariants.md): every attempt retains exact cross-tablet grouped
  key/state authority from the pinned compatible snapshot.
- [Invariant 11](../architecture/invariants.md): no retry changes the pinned Manifest generation,
  dispatch, serving node, or placement proof.
- [Invariant 14](../architecture/invariants.md): request, response, tablet, query, and route
  correlation remain exact through sender and carrier validation.
- [Invariant 15](../architecture/invariants.md): mutual authentication and exact target authorization
  precede application request write.
- [Invariant 18](../architecture/invariants.md): client ownership, borrowed TLS/authentication
  lifetimes, thread affinity, and cancellation destruction order are explicit.

## Validation

The end-to-end loopback case starts two bounded grouped servers, injects a refused first endpoint,
rotates the affected tablet to its second address, authenticates both connections, binds and
executes each worker once, and publishes one globally merged key with count three only after both
terminal streams close. Metrics prove three attempts, one retry, two completed transports, one
failed transport, and no remaining active client. Separate cases reject incomplete routes and
invalid grouped bounds before I/O, expire before starting an attempt, and cancel active attempts.

The focused two-test suite passes normally and under ASan/UBSan. The complete cluster suite passes
244 of 244 and its allocation-failure suite passes 31 of 31. Header self-containment, formatting,
and whitespace checks pass. LLVM 18 static analysis remains blocked by the installed macOS 26
libc++ headers (`__builtin_clzg`, `__builtin_ctzg`, and related substitutions); the new scheduler
source reports no project-local finding before that compiler failure, and the one new test finding
was corrected.

## Migration and rollback

No durable or wire migration exists. Callers that need grouped sufficient-state rows can replace
manual per-tablet client orchestration with this owner. Rollback must disable this coordinator path
or preserve equivalent complete-route preflight, whole-attempt retry, shared authority, and atomic
publication; it must not publish partial grouped rows.

## Unresolved questions

- Final grouped Native SQL projection, ordering, limit, and protocol result integration.
- Computed pre-group physical-plan splitting.
- Partitioned/shuffle grouped execution for non-colocated grouping keys.

## References

- [Portable pinned grouped sufficient-state execution owner](0476-portable-pinned-grouped-sufficient-state-execution-owner.md)
- [Deadline-bound grouped sufficient-state TCP client](0480-deadline-bound-grouped-sufficient-state-tcp-client.md)
- [Distributed Vector Grouped Aggregate Query Transport v2](../formats/distributed-vector-grouped-aggregate-query-transport-v2.md)
