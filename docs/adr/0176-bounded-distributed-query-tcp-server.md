# ADR 0176: Bounded distributed-query TCP server

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** cluster and network subsystems
- **Extends:** [ADR 0174](0174-bounded-inbound-distributed-query-tls-carrier.md), [ADR 0175](0175-nonblocking-ipv4-tcp-descriptor-ownership.md)

## Context

The inbound carrier and TCP listener could serve one connection when an embedding joined them, but
there was no owner for a listener, long-lived TLS context, multiple connection records, bounded
admission, readiness polling, deadline checks, or deterministic shutdown. Reusing the native client
reactor would mix protocol/trust domains, while an unbounded connection table or accept drain would
let hostile peers control memory and reactor work.

## Decision

`DistributedQueryTcpServer` is a move-only, single-threaded POSIX `poll` owner dedicated to the
one-request-per-connection distributed-query protocol. Startup validates all limits and TLS
credentials before binding. It owns one `TcpListener`, one `TlsServerContext`, a connection table
whose capacity is reserved to the configured maximum, and a fixed poll-descriptor vector sized to
that maximum plus the listener.

Each poll performs one kernel wait, drives at most one TLS operation for every existing carrier,
applies carrier deadlines even without readiness, and admits at most
`maximum_accepts_per_poll`. Connections beyond `maximum_connections` are accepted and immediately
closed within that same finite admission budget, preventing an indefinitely full kernel backlog
from masquerading as application admission. Listener errors, rejected admissions, carrier failures,
successful exchanges, and active connections have distinct metrics.

An accepted `TcpSocket` is configured before exposure, its peer address is passed to the TLS
carrier authenticator, and its descriptor is borrowed by a new `TlsSocket`. Each table entry is a
stable separately allocated owner record held behind `unique_ptr`. Inside that record the TCP socket
is declared before the TLS carrier, so destruction occurs in reverse order: carrier first,
descriptor second. Erasing or compacting the table moves only `unique_ptr` handles and can never
memberwise-move a socket over a carrier that still borrows it.

The connection-table and poll-vector allocations happen at startup. One bounded allocation remains
per admitted connection for the stable record; allocation failure rejects that connection and
destroys its carrier before its descriptor. Shutdown clears every connection before closing the
listener; the TLS context remains alive until the server owner is destroyed.

The receiver and authenticator are borrowed and must outlive the server. The worker call remains the
synchronous boundary already defined by `DistributedQueryReceiver`; asynchronous worker completion,
cross-thread wakeups, and pooling require separate designs.

## Consequences and validation

Retained memory is `O(maximum_connections)` plus the already bounded per-connection TLS/request/
response state. Work per poll is `O(active_connections + maximum_accepts_per_poll)`. The design is
portable across the supported POSIX development/production targets and exposes no `pollfd` publicly.

A real loopback test starts the server on a kernel-selected port, completes nonblocking TCP and
maintained mutual TLS, authenticates both certificate principals, executes one proof-bound query,
returns the correlated aggregate, and observes one accepted/completed connection with no retained
session. A second real test opens two connections against a maximum of one and proves exactly one
admission and one explicit rejection before clean shutdown. Focused sanitizer and installed-consumer
tests cover lifetime and public ABI.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## Alternatives considered

- **Route through the native-protocol epoll reactor:** rejected because cluster control traffic has
  distinct framing, authentication, and worker semantics.
- **Store connection records directly in a compacting vector:** rejected because memberwise move
  assignment can close the destination descriptor before destroying its borrowing TLS carrier.
- **Allocate the entire carrier objects inline at maximum capacity:** deferred because their large
  fixed request buffers would commit substantial memory for inactive slots.
- **Drain accepts until would-block:** rejected because a busy listener could starve established
  query progress.
- **Thread per connection:** rejected for the same unbounded stack/scheduler ownership reasons as
  ADR 0009.

## Migration and rollback

This adds no durable or wire format. Embeddings can start this dedicated server instead of manually
joining listener, TLS context, and inbound carriers. Equivalent reactors must preserve the same
limits, authentication order, stable carrier/descriptor lifetime, deadline checks, and shutdown
order. Removing it returns multi-connection ownership to the embedding without changing individual
carriers.

## References

- [Network reactor strategy](0009-network-reactor-strategy.md)
- [Bounded inbound distributed-query TLS carrier](0174-bounded-inbound-distributed-query-tls-carrier.md)
- [Nonblocking IPv4 TCP descriptor ownership](0175-nonblocking-ipv4-tcp-descriptor-ownership.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Architecture invariants](../architecture/invariants.md)
