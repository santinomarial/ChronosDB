# ADR 0463: Deadline-bound Raft read-authority TCP endpoints

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB cluster transport, networking, query, and Raft maintainers
- **Extends:** [ADR 0462](0462-bounded-raft-read-authority-partial-io-and-mtls.md) and
  [ADR 0175](0175-nonblocking-ipv4-tcp-descriptor-ownership.md)

## Context

The read-authority mutual-TLS owners accepted already-connected sockets. Process integration still
needed explicit nonblocking connect ownership, listener and descriptor lifetime, bounded admission,
poll scheduling, metrics, and shutdown ordering. Ad hoc embedding would risk closing a descriptor
before its TLS borrower, accepting unbounded peers, or authenticating an address other than the one
actually connected.

## Decision

`RaftReadAuthorityTcpClient` owns one exact request, one nonblocking `TcpSocket`, and its later TLS
carrier. Construction validates the request, transport bounds, positive timeouts, TLS dependencies,
and equality between the authentication IPv4 address and the actual remote endpoint before opening
a descriptor. Connect completion requires `SO_ERROR` proof through `finish_connect`; a separate
deadline expires and closes the descriptor exactly. Once connected, the existing TLS owner handles
authentication, authorization, framing, correlation, and exchange deadlines. Failure destroys TLS
before closing the socket and remains sticky.

`RaftReadAuthorityTcpServer` is a move-only single-threaded POSIX `poll` owner. Startup validates
credentials and bounds, owns one listener and long-lived TLS context, reserves exactly the configured
connection capacity, and allocates one fixed poll vector. A poll admits at most
`maximum_accepts_per_poll`; peers above `maximum_connections` are accepted, immediately closed, and
counted separately from accept errors. Each stable heap-owned connection declares its socket before
the borrowing TLS session so erase and shutdown destroy TLS first. Completion, failure, rejection,
error, and active counts are distinct.

The endpoints own no DNS, address rotation, retry, leader discovery, all-group fan-out, daemon
service implementation, or query-attempt policy. One event-loop thread serializes every call.

## Consequences

Each outbound acquisition has one descriptor owner across connect and exchange. Server memory is
`O(maximum_connections)` with one bounded TLS session per admitted peer; poll work is
`O(active_connections + maximum_accepts_per_poll)`. Shutdown clears sessions before closing the
listener while retaining the TLS context until session destruction completes.

The dedicated endpoint is implemented but not yet started by `chronosd`. Its port and resource
budget therefore remain an embedding choice. No durable, consensus, or wire-format bytes change.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): authentication address and actual route are exact,
  and the existing group/correlation proof remains unchanged.
- [Invariant 10](../architecture/invariants.md) and
  [Invariant 11](../architecture/invariants.md): connect time, admission, accepts per poll, retained
  sessions, and poll storage are bounded with explicit owners.
- [Invariant 15](../architecture/invariants.md): TCP does not bypass the mutual-TLS principal-to-node
  checks and descriptor destruction preserves the TLS borrowing lifetime.
- [Invariant 18](../architecture/invariants.md): one endpoint request remains one per-group authority,
  not a global cross-group snapshot.

## Validation

A real loopback test starts the bounded server, drives nonblocking client connect and mutual TLS,
returns the exact authority, invokes the service once, and verifies accepted, completed, and active
metrics. A second case rejects route mismatch before connect, proves exact sticky connect expiry and
descriptor closure, admits one of two peers under a capacity of one, counts the other rejection, and
clears the live session on shutdown. Broader suite, sanitizer, format, and static-analysis evidence
is recorded with the implementing commit. Before commit, all 216 normal cluster tests and all 28
cluster allocation-failure tests passed with loopback permission. All ten focused authority tests
passed under ASan/UBSan with leak detection disabled because Apple's sanitizer runtime does not
support LeakSanitizer. Both new production sources passed repository-pinned clang-tidy 18; all
changed C++ files passed clang-format 18; and the diff passed whitespace review.

## Migration or rollback considerations

No format migration. A future daemon must publish a distinct configured endpoint only among peers
supporting exact v1. Rolling back this currently unintegrated endpoint removes no stored state;
after daemon integration, roll back its complete configuration and process stack together.

## Unresolved questions

- Define finite multi-address retry and current-leader route refresh without silently rebinding one
  attempt to incompatible authority.
- Decide dedicated versus multiplexed daemon listener after measuring resource isolation and
  head-of-line behavior.

## References

- [Raft Read Authority Transport v1](../formats/raft-read-authority-transport-v1.md)
- [Nonblocking IPv4 TCP descriptor ownership](0175-nonblocking-ipv4-tcp-descriptor-ownership.md)
- [Bounded Raft read-authority partial I/O and mutual TLS](0462-bounded-raft-read-authority-partial-io-and-mtls.md)
