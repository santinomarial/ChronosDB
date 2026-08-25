# ADR 0481: Bounded grouped sufficient-state TCP server

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB cluster and networking maintainers
- **Extends:** [ADR 0393](0393-bounded-definition-bound-vector-aggregate-query-v2-tcp-server.md),
  [ADR 0479](0479-bounded-grouped-sufficient-state-mutual-tls.md)

## Context

The inbound grouped sufficient-state TLS carrier required embeddings to join a listener, long-lived
server TLS context, stable accepted-descriptor/carrier lifetime, readiness and deadline polling,
admission bounds, metrics, and deterministic shutdown. An unbounded accept drain or connection
table would let hostile peers control retained memory and per-poll work. Moving an inline record
could also destroy its descriptor before the TLS carrier that borrows it.

## Decision

`DistributedVectorGroupedAggregateQueryTcpServerV2` is a move-only, single-thread-affine POSIX
`poll` owner for one grouped Fragment-v2 request and one complete empty-or-contiguous sufficient-
state stream per connection. Startup validates TLS credentials, every outer and nested carrier
limit, positive handshake and exchange deadlines, connection capacity, and the per-poll accept
budget before binding. It owns the listener, TLS server context, a connection vector reserved to
the configured maximum, and fixed poll storage sized to that maximum plus the listener.

Each poll performs one kernel wait, progresses at most one TLS operation per existing connection,
applies carrier deadlines even without readiness, and attempts no more than the configured accept
budget. When the table is full, newly accepted sockets are destroyed immediately and counted as
rejected within that same budget.

Every admitted session has a separately allocated stable record behind `unique_ptr`; table
compaction moves only handles. The record declares `TcpSocket` before the grouped TLS server, so
reverse destruction always removes TLS before closing its borrowed descriptor. Shutdown clears all
sessions before closing the listener and is idempotent. The receiver and authenticator are borrowed
and outlive the server.

Accepted, rejected, accept-error, completed, failed, and active connection metrics are exposed.
Lifetime counters saturate rather than wrap. This owner adds no format, worker construction, retry,
query scheduling, or process lifecycle.

## Alternatives considered

- **Reuse the ungrouped aggregate TCP server:** rejected because its carrier cannot bind group-key
  authority or send a data-dependent grouped response stream.
- **Store connection records inline:** rejected because vector compaction could violate stable
  descriptor borrowing and teardown order.
- **Drain all pending accepts:** rejected because a busy listener could starve established query
  progress.
- **Allocate maximum response bytes at startup:** rejected because each admitted reader allocates
  only exact independently bounded frames.

## Consequences

Retained memory is `O(maximum_connections)` plus independently bounded admitted-session state.
Poll work is `O(active_connections + maximum_accepts_per_poll)`. One owner thread serializes calls,
so no synchronization or inter-thread memory-ordering argument applies.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): listener ownership does not reinterpret Fragment-v2
  or `CHDVGRP2` bytes.
- [Invariant 6](../architecture/invariants.md): startup and carrier limits bound connections,
  accepts, groups, frames, nested state, bytes, deadlines, and retained state.
- [Invariant 10](../architecture/invariants.md): admitted sessions obtain complete grouped authority
  from the authenticated receiver boundary.
- [Invariant 14](../architecture/invariants.md): the exact accepted peer address enters the
  authenticated route boundary.
- [Invariant 15](../architecture/invariants.md): every admitted session completes mutual TLS before
  application bytes.
- [Invariant 18](../architecture/invariants.md): stable record lifetime, reverse-safe teardown,
  borrowed dependencies, and single-thread affinity are explicit.

## Validation

A real loopback test starts on a kernel-selected port, completes nonblocking TCP and mutual TLS
through the grouped TCP client, authenticates both certificate fingerprints, binds fresh complete
grouped authority, executes once, publishes two terminally closed groups, and observes one accepted
and completed session with no retained connection. A second case configures one slot, opens two
sockets, and proves one admission, one explicit rejection, invalid configuration and poll rejection,
bounded active state, and idempotent ordered shutdown. Both cases pass normally and under ASan/
UBSan. The complete cluster suite passes 242 of 242 and the allocation-failure suite passes 31 of
31. Header self-containment, formatting, and whitespace checks pass. LLVM 18 static analysis remains
blocked by the installed macOS 26 libc++ headers and reports no project-local finding before those
compiler errors.

## Migration and rollback

No durable or wire bytes change. Embeddings can replace ad hoc grouped listener loops with this
owner. Rollback restores caller-side admission and polling, which must preserve finite limits,
authentication order, exact peer address, stable descriptor/carrier lifetime, deadline progression,
metrics, and shutdown ordering.

## Unresolved questions

- Production receiver/worker/server composition.
- Multi-address retries, whole-query cancellation, and all-tablet scheduling.
- Native SQL and multi-process compatibility qualification.

## References

- [Bounded grouped sufficient-state mutual TLS](0479-bounded-grouped-sufficient-state-mutual-tls.md)
- [Deadline-bound grouped sufficient-state TCP client](0480-deadline-bound-grouped-sufficient-state-tcp-client.md)
- [Distributed Vector Grouped Aggregate Query Transport v2](../formats/distributed-vector-grouped-aggregate-query-transport-v2.md)
