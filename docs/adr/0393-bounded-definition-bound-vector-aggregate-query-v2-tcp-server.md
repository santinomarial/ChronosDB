# ADR 0393: Bounded definition-bound vector aggregate query v2 TCP server

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB cluster and networking maintainers
- **Extends:** [ADR 0372](0372-bounded-schema-bound-vector-query-v2-tcp-server.md),
  [ADR 0391](0391-bounded-definition-bound-vector-aggregate-query-v2-mutual-tls.md)

## Context

The inbound aggregate TLS carrier required embeddings to join a listener, long-lived server TLS
context, stable per-connection descriptor/carrier lifetime, readiness and deadline polling,
admission limits, metrics, and deterministic shutdown. Unbounded accept drains or connection tables
would let hostile peers control memory and per-poll work. Moving an inline record could also destroy
the socket descriptor before the TLS carrier that borrows it.

## Decision

`DistributedVectorAggregateQueryTcpServerV2` is a move-only, single-threaded POSIX `poll` owner for
one aggregate Fragment-v2 request and one complete definition-bound state vector per connection.
Startup validates TLS credentials, all outer/nested carrier limits and positive deadlines,
connection capacity, and per-poll accept budget before binding. It owns the listener, TLS server
context, a connection vector reserved to the configured maximum, and fixed poll storage sized to
that maximum plus the listener.

Each poll performs one kernel wait, progresses at most one TLS operation per existing connection,
applies carrier deadlines without readiness, and attempts no more than the configured accept budget.
When the table is full, newly accepted sockets are destroyed immediately and counted as rejected
within that budget.

Every admitted session has a separately allocated stable record behind `unique_ptr`; table
compaction moves only handles. The record declares `TcpSocket` before the aggregate TLS server so
reverse destruction always removes TLS before closing its borrowed descriptor. Shutdown clears all
sessions before closing the listener. The receiver and authenticator are borrowed and outlive the
server.

Accepted, rejected, accept-error, completed, failed, and active connection metrics are exposed.
Lifetime counters saturate rather than wrap. This owner adds no format, worker construction, retry,
query scheduling, or process lifecycle.

## Alternatives considered

- **Reuse the row TCP server:** rejected because its TLS carrier cannot bind aggregate definitions
  or merge-state response bytes.
- **Store connection records inline:** rejected because vector compaction could violate stable
  descriptor borrowing and teardown order.
- **Drain all pending accepts:** rejected because a busy listener could starve established query
  progress.
- **Allocate response maxima at startup:** rejected because per-session readers allocate only exact
  independently bounded frames.

## Consequences

Retained memory is `O(maximum_connections)` plus independently bounded admitted-session state. Poll
work is `O(active_connections + maximum_accepts_per_poll)`. One owner thread serializes calls, so no
synchronization or memory-ordering argument is required.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): listener ownership does not reinterpret Fragment-v2
  or aggregate response bytes.
- [Invariant 6](../architecture/invariants.md): startup and carrier limits bound connections,
  accepts, frames, nested state, bytes, deadlines, and retained state.
- [Invariant 10](../architecture/invariants.md): admitted sessions still obtain definitions from the
  authenticated receiver authority.
- [Invariant 14](../architecture/invariants.md): the exact accepted peer address enters the
  authenticated route boundary.
- [Invariant 15](../architecture/invariants.md): every admitted session completes mutual TLS before
  application bytes.
- [Invariant 18](../architecture/invariants.md): stable record lifetime, reverse-safe teardown,
  borrowed dependencies, and single-thread affinity are explicit.

## Validation plan

A real loopback test starts on a kernel-selected port, completes nonblocking TCP and mutual TLS
through the aggregate TCP client, authenticates both certificate fingerprints, binds definitions,
executes once, publishes two states, and observes one accepted/completed session with no retained
connection. A second test configures one slot, opens two sockets, and proves one admission, one
explicit rejection, invalid configuration/poll rejection, bounded active state, and idempotent
ordered shutdown. Run header self-containment, installed consumption, formatting, static analysis,
ASan/UBSan, and the full serialized suite.

## Migration or rollback considerations

No durable or wire bytes change. Embeddings can replace ad hoc aggregate listener loops with this
owner. Rollback restores caller-side admission/polling, which must preserve finite limits,
authentication order, exact peer address, stable descriptor/carrier lifetime, deadline progression,
metrics, and shutdown ordering.

## References

- [Bounded schema-bound vector query v2 TCP server](0372-bounded-schema-bound-vector-query-v2-tcp-server.md)
- [Bounded definition-bound vector aggregate query v2 mutual TLS](0391-bounded-definition-bound-vector-aggregate-query-v2-mutual-tls.md)
- [Distributed Vector Aggregate Query Transport v2](../formats/distributed-vector-aggregate-query-transport-v2.md)
