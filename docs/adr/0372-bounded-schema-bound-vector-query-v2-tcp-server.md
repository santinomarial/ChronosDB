# ADR 0372: Bounded schema-bound vector query v2 TCP server

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB cluster and networking maintainers
- **Extends:** [ADR 0175](0175-nonblocking-ipv4-tcp-descriptor-ownership.md),
  [ADR 0370](0370-bounded-schema-bound-vector-query-v2-mutual-tls.md)

## Context

The inbound v2 TLS carrier still required embeddings to join a listener, long-lived TLS context,
stable per-connection descriptor/carrier lifetime, readiness polling, deadline driving, admission
limits, metrics, and deterministic shutdown. An unbounded accept drain or connection table would
let hostile peers control memory or per-poll work. Moving inline connection records could also
close a descriptor before destroying the TLS object that borrows it.

## Decision

`DistributedVectorQueryTcpServerV2` is a move-only, single-threaded POSIX `poll` owner dedicated to
one schema-bound vector request and its bounded response stream per connection. Startup validates
TLS credentials, carrier time/frame/byte limits, connection capacity, and per-poll admission before
binding. It owns the listener, TLS server context, a connection vector reserved to the configured
maximum, and a fixed poll vector sized to that maximum plus the listener.

Each poll performs one kernel wait, drives at most one TLS operation per existing connection,
applies carrier deadlines even without readiness, and attempts no more than the configured finite
accept budget. When the table is full, newly accepted descriptors are immediately destroyed and
counted as rejections within that same budget.

Each admitted connection is separately allocated behind `unique_ptr`; compacting the table moves
only stable handles. Inside each record, `TcpSocket` is declared before the v2 TLS carrier, so
reverse destruction always removes TLS before closing its borrowed descriptor. Shutdown clears
every connection before closing the listener. The receiver and authenticator remain borrowed and
must outlive the server.

Accepted, rejected, accept-error, completed, failed, and active connection metrics are exposed.
Lifetime counters saturate instead of wrapping. The server adds no durable or network format and
does not own worker construction, retry, or query scheduling.

## Alternatives considered

- **Route through the native protocol reactor:** rejected because cluster query traffic has a
  distinct framing, trust, and response-retention boundary.
- **Store connection objects inline:** rejected because compacting moves can violate
  carrier-before-descriptor teardown.
- **Drain every pending accept:** rejected because a busy listener could starve established query
  progress.
- **Allocate maximum response storage at startup:** rejected because the TLS carrier grows only
  under independently checked exact frame and byte bounds.

## Consequences

Retained memory is `O(maximum_connections)` plus the bounded state of admitted connections. Poll
work is `O(active_connections + maximum_accepts_per_poll)`. One owner thread serializes calls, so no
synchronization or memory-ordering argument is required.

ADRs 0373–0376 subsequently supply finite retry, schema-bound coordination, the production row
worker, and stable inbound service composition. Outbound multi-tablet execution, global result
semantics, aggregate state, and process integration remain incomplete.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): listener ownership does not reinterpret v2 bytes.
- [Invariant 6](../architecture/invariants.md): startup and carrier limits bound connections,
  accepts, frames, bytes, deadlines, and retained state.
- [Invariant 10](../architecture/invariants.md): the admitted Fragment-v2 schema remains the sole
  response descriptor authority.
- [Invariant 14](../architecture/invariants.md): the accepted peer address enters the authenticated
  route boundary without substitution.
- [Invariant 15](../architecture/invariants.md): every admitted connection completes mutual TLS
  before application bytes.
- [Invariant 18](../architecture/invariants.md): stable record lifetime, reverse-safe destruction,
  borrowed dependencies, and single-thread affinity are explicit.

## Validation plan

A real loopback test starts the server on a kernel-selected port, completes nonblocking TCP and
mutual TLS through the v2 TCP client, authenticates both certificate fingerprints, invokes one
worker, returns two schema-bound responses, and observes one accepted/completed connection with no
retained session. A second test configures one slot, opens two connections, and proves one admission,
one explicit rejection, invalid-config/poll rejection, bounded active state, and idempotent ordered
shutdown. Header self-containment, installed consumption, ASan/UBSan, relevant static analysis,
formatting, and the full serialized suite are required before completion.

## Migration or rollback considerations

No durable or wire migration exists. Embeddings can replace ad hoc v2 listener loops with this
owner. Rollback restores caller-side admission and polling, which must preserve the same finite
limits, authentication order, exact peer address, stable carrier/descriptor lifetime, deadlines,
metrics, and shutdown order.

## References

- [Bounded schema-bound vector query v2 mutual TLS](0370-bounded-schema-bound-vector-query-v2-mutual-tls.md)
- [Bounded grouped-query TCP server](0337-bounded-grouped-query-tcp-server.md)
- [Bounded distributed-query TCP server](0176-bounded-distributed-query-tcp-server.md)
- [Distributed Vector Query Transport v2](../formats/distributed-vector-query-transport-v2.md)
