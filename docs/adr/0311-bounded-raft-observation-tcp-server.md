# ADR 0311: Bounded Raft observation TCP server

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB cluster and networking maintainers
- **Extends:** [ADR 0310](0310-bounded-inbound-raft-observation-mtls-session.md),
  [ADR 0175](0175-nonblocking-ipv4-tcp-descriptor-ownership.md)

## Context

The accepted-socket session still required an embedding to own a listener, long-lived TLS context,
bounded connection table, readiness polling, admission rejection, metrics, and shutdown ordering.
An unbounded table or accept drain would let hostile peers control memory or per-poll work.

## Decision

`RaftObservationTcpServer` is a move-only single-threaded POSIX `poll` owner for the dedicated
one-request-per-connection observation protocol. Startup validates limits and credentials, owns one
`TcpListener` and `TlsServerContext`, reserves a connection table to the configured maximum, and
allocates a fixed poll vector.

Each poll drives at most one TLS operation per active session and admits at most
`maximum_accepts_per_poll`. Connections beyond the cap are accepted and immediately closed within
that finite budget and counted separately from accept errors. Every stable heap-owned connection
record declares its `TcpSocket` before its borrowing TLS session so erase and shutdown destroy TLS
first. Completion, failure, rejection, error, and active counts remain distinct.

Shutdown clears all sessions before closing the listener. The TLS context remains alive through
connection destruction. The authenticator and receiver are borrowed and must outlive the server.

## Consequences and validation

Retained memory is `O(maximum_connections)` plus bounded per-session state, and work per poll is
`O(active_connections + maximum_accepts_per_poll)`. A real loopback test completes nonblocking TCP,
mutual TLS, one observation request, and correlated result while checking accepted/completed/active
metrics. A second test proves an admission cap of one accepts exactly one of two peers, explicitly
rejects the other, and deterministically clears the active session on shutdown. These tests require
approved host execution where sandbox policy forbids loopback bind. The installed consumer covers
the public server interface.

Remote multi-address retry, leader/follower fan-out, pair selection, and packaged query construction
remain incomplete.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Bounded inbound Raft observation mTLS session](0310-bounded-inbound-raft-observation-mtls-session.md)
- [Nonblocking IPv4 TCP descriptor ownership](0175-nonblocking-ipv4-tcp-descriptor-ownership.md)
- [Bounded distributed-query TCP server](0176-bounded-distributed-query-tcp-server.md)
