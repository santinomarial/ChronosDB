# ADR 0177: Deadline-bound distributed-query TCP client

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** cluster and network subsystems
- **Extends:** [ADR 0173](0173-bounded-outbound-distributed-query-tls-carrier.md), [ADR 0175](0175-nonblocking-ipv4-tcp-descriptor-ownership.md)

## Context

The outbound TLS carrier and nonblocking TCP socket owner still required embedding glue to retain an
attempt during connect, prove connect completion, create TLS at the right boundary, translate
readiness, expire a stalled connect, preserve exact peer identity, and destroy TLS before closing
its borrowed descriptor. A false `SO_ERROR` transition or inconsistent configured peer address
could weaken server-node authorization.

## Decision

`DistributedQueryTcpClient` is a move-only, single-owner composite for one immutable
`DistributedQueryAttempt`. Before opening a descriptor it validates retry/target identity, exact
decodes the canonical request, checks its embedded target, validates every timeout, and requires the
authenticator peer IPv4 address to equal the actual remote TCP route.

`begin` starts one nonblocking `TcpSocket` and retains the attempt. While connecting, the client
requests write readiness and applies a separate positive connect deadline. It creates no TLS state
until `finish_connect` observes successful `SO_ERROR`. Only then does it create a maintained client
`TlsSocket` and the existing outbound carrier, which applies handshake/exchange deadlines,
certificate identity verification, principal-to-target authorization, request short writes, and
bounded response framing.

The composite delegates readiness to the carrier after connection and retains its exact completed
response bytes for the existing sender. A connect or carrier failure is sticky: it first destroys
the TLS carrier, then closes the TCP descriptor, and exposes no readiness. Field declaration order
also guarantees carrier-before-socket destruction on normal completion and owner destruction. The
TLS context, authenticator, and node authorizer are borrowed and must outlive the client.

The client owns no retry or backoff. A caller reports failure or exact response bytes to
`DistributedQuerySender`/`DistributedQueryExecution`, which remains the sole attempt-policy owner.
Address resolution, multiple candidate addresses, route pooling, and multi-attempt scheduling remain
above this one-attempt composite.

ADR 0178 now supplies that multi-tablet and multi-attempt scheduling layer while retaining this
client as the sole owner of one immutable connection attempt.

## Consequences and validation

Memory is the sum of one bounded attempt, TCP/TLS state, and the outbound carrier's fixed response
storage. Work is linear in request/response bytes plus constant connect state. No socket is opened
for an invalid attempt or inconsistent route identity.

The real TCP server test now uses this composite directly, proving nonblocking connect, mutual TLS,
both principal mappings, worker execution, response retention, and sender acceptance. A separate
real listener test proves invalid configuration rejects before connection, a pre-deadline call does
not expire, the exact connect deadline fails with `UNAVAILABLE`, the descriptor is already closed,
and later calls return the identical sticky failure.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## Alternatives considered

- **Create TLS while connect is pending:** rejected because record I/O cannot begin until the socket
  connection result is authoritative.
- **Let callers provide an unrelated authentication peer address:** rejected because authorization
  metadata must describe the actual route being established.
- **Move retry/backoff into the composite:** rejected because proof rebinding and retry budget belong
  to the existing deterministic sender.
- **Keep a failed descriptor for diagnostics:** rejected because a terminal attempt must release its
  finite kernel resource immediately; the returned status retains the diagnostic.

## Migration and rollback

This adds no durable or wire format. Outbound embeddings may replace manual TCP/TLS/carrier joining
with this composite and feed its terminal outcome to the sender. Equivalent implementations must
validate before connect, bind peer identity to route, wait for `SO_ERROR`, enforce all three
deadlines, and destroy carrier before descriptor. Removing it returns this orchestration to the
embedding.

## References

- [Bounded outbound distributed-query TLS carrier](0173-bounded-outbound-distributed-query-tls-carrier.md)
- [Nonblocking IPv4 TCP descriptor ownership](0175-nonblocking-ipv4-tcp-descriptor-ownership.md)
- [Bounded distributed-query TCP server](0176-bounded-distributed-query-tcp-server.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Architecture invariants](../architecture/invariants.md)
- [Pinned multi-tablet TCP query scheduling](0178-pinned-multi-tablet-tcp-query-scheduling.md)
