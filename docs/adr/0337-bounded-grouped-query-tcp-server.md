# ADR 0337: Bounded grouped-query TCP server

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB cluster and networking maintainers
- **Extends:** [ADR 0175](0175-nonblocking-ipv4-tcp-descriptor-ownership.md),
  [ADR 0335](0335-bounded-grouped-query-mutual-tls.md)

## Context

The inbound grouped TLS carrier still required embeddings to join a listener, long-lived TLS
context, stable per-connection descriptor/carrier lifetime, readiness polling, deadline driving,
admission limits, metrics, and deterministic shutdown. Unbounded accept draining or connection
storage would let a hostile peer control memory or per-poll work, while moving an inline record
could close a descriptor before destroying the TLS object that borrows it.

## Decision

`DistributedGroupedQueryTcpServer` is a move-only, single-threaded POSIX `poll` owner dedicated to
one grouped request and its bounded response stream per connection. Startup validates TLS
credentials, carrier time/frame limits, connection capacity, and per-poll admission before binding.
It owns the listener, TLS server context, a connection vector reserved to the configured maximum,
and a fixed poll vector sized to that maximum plus the listener.

Each poll performs one kernel wait, drives at most one TLS operation per existing connection,
applies carrier deadlines even without readiness, and attempts no more than the configured finite
accept budget. When the table is full, newly accepted descriptors are immediately destroyed and
counted as rejections within that same budget.

Each admitted connection is separately allocated behind `unique_ptr`; compacting the table moves
only stable handles. Within the connection owner, `TcpSocket` is declared before the grouped TLS
carrier, so reverse destruction always removes TLS before closing its borrowed descriptor. Shutdown
clears every connection before closing the listener. The receiver and authenticator remain borrowed
and must outlive the server.

The server exposes accepted, rejected, accept-error, completed, failed, and active connection
metrics. It adds no durable or network format and does not own worker scheduling or query retry.

## Consequences and validation

Retained memory is `O(maximum_connections)` plus the bounded state of admitted connections. Poll
work is `O(active_connections + maximum_accepts_per_poll)`. One owner thread serializes calls, so no
synchronization or memory-ordering argument is required.

A focused real loopback test starts the server on a kernel-selected port, completes nonblocking TCP
and mutual TLS through the grouped TCP client, authenticates both certificate fingerprints,
invokes one worker, returns two exact ordered partials, and observes one accepted/completed
connection with no retained session. A second case configures one slot, opens two connections, and
proves one admission, one explicit rejection, bounded active state, invalid-config rejection, and
deterministic shutdown. The installed external-consumer gate references `start`.

ADR 0338 subsequently supplies production real-CSEG service composition. Sender/coordinator
integration, packaged multi-tablet execution, multi-process failover, and broad fault/measurement
evidence remain incomplete. No Phase 16 exit gate is claimed.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Bounded grouped-query mutual TLS](0335-bounded-grouped-query-mutual-tls.md)
- [Bounded distributed-query TCP server](0176-bounded-distributed-query-tcp-server.md)
- [Nonblocking IPv4 TCP descriptor ownership](0175-nonblocking-ipv4-tcp-descriptor-ownership.md)
- [Distributed Grouped FLOAT64 Query Transport v1](../formats/distributed-grouped-float64-query-transport-v1.md)
