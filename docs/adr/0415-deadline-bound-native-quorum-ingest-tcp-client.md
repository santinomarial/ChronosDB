# ADR 0415: Deadline-bound native QUORUM_SYNC TCP client

- **Status:** accepted
- **Date:** 2026-08-23
- **Owners:** ChronosDB networking, security, native-client, and ingest maintainers
- **Extends:** [ADR 0172](0172-maintained-mutual-tls-client-socket.md),
  [ADR 0175](0175-nonblocking-ipv4-tcp-descriptor-ownership.md),
  [ADR 0414](0414-exact-native-quorum-ingest-redirect-replay.md)

## Context

The portable QUORUM_SYNC replay owner retained exact request bytes and selected every redirected
route, but embeddings still had to join nonblocking connect completion, mutual TLS, certificate
principal authorization, partial record I/O, three deadline phases, descriptor replacement, and
terminal result publication. Duplicated glue could send protocol bytes before authenticating the
selected node, keep TLS alive after closing its borrowed descriptor, or replay an ambiguous request
after transport loss.

## Decision

`NativeQuorumIngestTcpClient` is a move-only, single-threaded composite for one exact
`NativeQuorumIngestRetry`. Before acquisition it validates positive connect, handshake, and
exchange timeouts plus a finite positive I/O chunk bound. It creates one nonblocking `TcpSocket`
for the replay owner's current explicit IPv4 route and exposes the descriptor, current poll
interest, and active monotonic deadline to an embedding-owned event loop.

After `SO_ERROR` proves connect completion, the client creates a maintained `TlsSocket` from that
route's borrowed `TlsClientContext`. No native-protocol byte is written until mutual TLS verifies
the server certificate, `ConnectionAuthenticator` maps its SHA-256 fingerprint to a nonzero stable
principal, and the borrowed `NativeNodePrincipalAuthorizer` permits that principal to claim the
exact selected node ID. Connect, TLS handshake, and protocol exchange each receive a separate
positive deadline. Deadline equality is expired.

Each readiness call advances at most one connect, handshake, bounded write, or bounded read
operation. The configured chunk bound applies to both directions; the portable replay owner retains
all framing and request lifecycle state. A validated terminal redirect destroys TLS before closing
the old descriptor, opens the newly selected route, resets the connect deadline, and repeats full
TLS and node authorization before the fresh Protocol 2 handshake. Only the replay owner's validated
QUORUM_SYNC receipt completes the composite; completion destroys TLS, closes TCP, and publishes the
value-owned receipt.

After construction, connect, TLS, authorization, protocol, deadline, allocation, and server-error
failures are sticky. Construction failure returns no owner and RAII releases any partially acquired
descriptor. An EOF, reset, or other generic transport failure is not evidence that the ingest was
unapplied and therefore never starts replay. Only the existing authenticated Protocol 2 redirect
does so.

## Consequences

The native client library now owns the full finite TCP/mutual-TLS redirect path for one QUORUM_SYNC
operation. Memory is one exact append, the bounded route map and client buffers, one configured I/O
chunk, one TCP/TLS attempt, and one terminal receipt. Work is linear in transferred bytes plus the
router's `O(log routes)` accepted-redirect lookup. The redirect bound and positive per-phase
deadlines make the carrier finite when its event-loop owner services the exposed deadline. One
event-loop thread serializes methods, so no inter-thread memory-ordering argument applies.

The composite owns no event loop, DNS, deployment-text route parser, credential reload, backoff,
generic transport retry, connection pool, or `chronosd` client workflow. ADR 0416 subsequently
supplies a bounded single-operation `poll` owner with whole-operation cancellation and deadline.
Strict route configuration and command/process integration remain separate concerns. No durable or
network bytes change.

## Affected invariants

- [Invariant 1](../architecture/invariants.md): terminal success remains the existing exact
  QUORUM_SYNC receipt and cannot cross an unproved transport outcome.
- [Invariant 9](../architecture/invariants.md): only the exact retained append is replayed, and only
  after explicit leader authority selects a fresh authenticated attempt.
- [Invariant 11](../architecture/invariants.md): TCP owns the descriptor, TLS borrows it, reverse
  destruction is fixed, and configuration authorities have explicit borrowed lifetimes.
- [Invariant 14](../architecture/invariants.md): the carrier changes no Protocol 2 bytes or
  connection-local request identity rules.
- [Invariant 18](../architecture/invariants.md): authentication, deadlines, bounded I/O, and sticky
  terminal failures cannot be bypassed by a faster path.

## Validation

A focused real-loopback test uses two mutual-TLS servers, authenticates the certificate principal
against nodes one and two, limits application I/O to one byte per operation, receives a canonical
redirect, reconnects, proves request ID one and byte-identical append bytes on both sessions, and
publishes the exact second-node receipt. Negative tests prove that an abrupt post-request transport
close does not replay, wrong node authorization sends no protocol bytes, invalid bounds fail before
acquisition, and connect, handshake, and authenticated-exchange deadlines expire exactly and
sticky. Header self-containment, installed consumption, warnings-as-errors, clang-tidy,
ASan/UBSan, formatting, and the full serialized suite are required before completion.

## Migration and rollback

Native event loops may replace hand-written connect/TLS/reconnect glue with this composite while
retaining poll scheduling and deployment configuration. Rollback returns that lifecycle to each
embedding and changes no server, durable, or wire state.

## References

- [Authentication and TLS integration boundary](0066-authentication-and-tls-integration-boundary.md)
- [Maintained mutual-TLS client socket](0172-maintained-mutual-tls-client-socket.md)
- [Nonblocking IPv4 TCP descriptor ownership](0175-nonblocking-ipv4-tcp-descriptor-ownership.md)
- [Exact native QUORUM_SYNC redirect replay](0414-exact-native-quorum-ingest-redirect-replay.md)
- [Bounded native QUORUM_SYNC TCP execution](0416-bounded-native-quorum-ingest-tcp-execution.md)
- [Native Protocol v2](../protocol/native-v2.md)
