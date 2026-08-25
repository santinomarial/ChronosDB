# ADR 0513: Bounded grouped shuffle remote-edge scheduling

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB distributed-query and networking maintainers
- **Extends:** [ADR 0507](0507-finite-immutable-route-grouped-shuffle-retry.md),
  [ADR 0508](0508-deadline-bound-grouped-shuffle-tcp-client.md),
  [ADR 0512](0512-atomic-grouped-shuffle-source-fanout.md)

## Context

Atomic source fan-out produces one finite retry owner per remote partition edge, while the TCP
client drives only one attempt. A packaged source still needed exact authority matching, complete
route preflight, bounded parallel poll storage, address rotation, retry/deadline arbitration,
cancellation, terminal metrics, and an all-remote-edges success condition.

## Decision

Add one move-only, single-thread-affine TCP execution owner for at most 4,096 remote edges. Each
slot owns one prepared retry and at most one active TCP/mTLS client. Construction requires every
retry to borrow the exact same authority object as the scheduler, rejects local or duplicate
tablet/partition edges, revalidates each edge, and requires a finite route for every destination.
Routes have nonzero unique node IDs, one to 1,024 nonzero unique IPv4 endpoints, and a borrowed TLS
client context. Authentication dependencies and all carrier/connect limits pass before any socket
opens.

`poll_once` starts only ready or due-backoff retries. Attempt number selects
`(attempt - 1) % endpoint_count`, preserving finite ordered address rotation without mutating node
authority. One active attempt exists per edge. The scheduler preallocates descriptor and slot-index
arrays, derives read/write interest from each client, and caps the caller wait by the query
deadline, earliest retry, and every active client's exact connect/handshake/exchange deadline.
The grouped TLS and TCP clients now expose their current deadline for this purpose. The bounded
listener applies the same deadline cap to active server sessions.

Connection, TLS, I/O, and resource failures return to the retry's declared policy. A retry whose
budget or permanent failure becomes terminal fails the whole owner. A client counts as successful
only after its exact reverse receipt has been authenticated and the retry records acknowledgment.
The owner completes only when every edge succeeds. Explicit cancellation or whole-execution
deadline destroys all clients and reports zero active attempts. Metrics expose attempts, retries,
completed/failed transports, active attempts, succeeded edges, and total edges.

An allocation-injection finding removed an incorrect `noexcept` from the internal scheduler
constructor: its diagnostic `Status` member can allocate. Construction allocation now unwinds into
`RESOURCE_EXHAUSTED` instead of terminating. No shared-memory concurrency is introduced; one
event-loop thread serializes the owner.

## Detailed rationale

The retry already owns immutable bytes and backoff policy, while the TCP client already owns one
descriptor and authenticated receipt. The scheduler should compose those contracts, not duplicate
them. Exact authority object identity prevents a retry encoded under a different query/shape owner
from being paired with an edge-compatible but semantically different authority. Deadline-aware
polling makes the carrier's exact timeout meaningful even when a caller supplies a long maximum
wait.

## Alternatives considered

- **One blocking thread per edge.** Rejected because descriptors, scheduling, cancellation, and
  completion accounting would be harder to bound and test.
- **Re-resolve addresses after each failure.** Rejected because blocking resolution and authority
  rebinding need a separate explicit policy; this owner rotates a frozen finite route.
- **Treat TCP connect as success.** Rejected because only the destination's authenticated receipt
  proves exact stream acceptance.
- **Allow value-equivalent authority objects.** Rejected because the retry's encoded bytes and
  resource ownership are bound to its original immutable authority.

## Consequences

Every remote edge from one or more prepared source plans can now be driven through finite address
rotation to authenticated receipt under one all-or-fail owner. Local self-routes remain outside the
network scheduler. Destination servers still retain completed streams separately; a packaged node
owner must drain them into reducers and gather partition outputs.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): retry, edge, node route, and authenticated receipt
  remain bound to one exact immutable shuffle authority.
- [Invariant 10](../architecture/invariants.md): no application success precedes mutual TLS,
  principal/node authorization, frame validation, and exact receipt validation.
- [Invariant 11](../architecture/invariants.md): retry, client, descriptor, TLS context borrowing,
  and teardown order are explicit; the false nonthrowing constructor contract is removed.
- [Invariant 15](../architecture/invariants.md): remote edges, routes, endpoints, active attempts,
  poll arrays, retry budgets, backoff, and every deadline are finite.
- [Invariant 18](../architecture/invariants.md): scheduling changes neither canonical routing nor
  reducer merge semantics.

## Validation plan

A real loopback mTLS case first selects a refused endpoint, records one failed transport, waits the
finite backoff, rotates to a live bounded server, authenticates both principals, validates the
receipt, completes the scheduler, extracts the retained stream, and finishes its reducer. Metrics
prove two attempts, one retry, one failure, one completion, zero active attempts, and one succeeded
edge. Negative coverage rejects empty execution and proves pre-attempt cancellation. Allocation
injection sweeps test setup plus route/index/slot/poll/PImpl construction and specifically proves
the diagnostic allocation unwinds rather than terminating. Header self-containment, full
warning-as-error ASan/UBSan suites, changed-file formatting, changed-source clang-tidy, and final
diff review are required. The warning-as-error ASan/UBSan build, all 291 cluster tests, and all 51
cluster allocation-failure tests pass. Changed C++ files pass LLVM 18 formatting. The
repository-wide format check reaches one unchanged pre-existing grouped-query TLS header
violation. Changed-source clang-tidy reaches only the known LLVM 18/macOS 26 libc++ builtin
incompatibility without another ChronosDB-source finding. Whitespace and scope review pass.

## Migration or rollback considerations

No durable or wire bytes change. Rollback removes the all-edge scheduler and deadline accessors;
callers must not drive retries ad hoc or use poll waits that can overrun active carrier deadlines.

## Unresolved questions

- Atomically drain acknowledged listener streams into their local partition reducers.
- Own all local reducers and return disjoint partition outputs to the Native coordinator.
- Add node-loss, receipt-loss, cancellation, skew, and multi-process differential qualification.

## References

- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
