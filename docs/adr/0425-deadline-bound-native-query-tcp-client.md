# ADR 0425: Deadline-bound native finite-query TCP client

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB networking, security, native-client, and query maintainers
- **Extends:** [ADR 0172](0172-maintained-mutual-tls-client-socket.md),
  [ADR 0175](0175-nonblocking-ipv4-tcp-descriptor-ownership.md),
  [ADR 0424](0424-exact-native-query-redirect-replay.md)

## Context

The finite-query replay owner retained exact SQL and a complete result stream, but an embedding
still had to compose nonblocking connect completion, mutual TLS, certificate-principal-to-node
authorization, partial I/O, phase deadlines, and descriptor replacement. Incorrect glue could send
SQL before authenticating the selected node, close a descriptor while TLS still borrowed it, retry
an ambiguous transport failure, or expose rows before terminal validation.

## Decision

`NativeQueryTcpClient` is a move-only, single-threaded carrier for one `NativeQueryRetry`. Creation
requires positive connect, handshake, and exchange timeouts; a positive I/O chunk bounded by the
retry input buffer; a certificate authenticator; and a node-principal authorizer. The shared
`NativeNodePrincipalAuthorizer` interface moves to its own neutral header because both query and
QUORUM_SYNC carriers now consume the same authority without changing its ABI or semantics.

The client opens the retry owner's exact IPv4 route nonblocking. After `SO_ERROR` proves connection,
it creates TLS from that route's borrowed context. It writes no native bytes until mutual TLS
verifies the peer certificate, the authenticator maps its SHA-256 fingerprint to a nonzero
principal, and the node authorizer permits that principal to claim the selected node. Connect,
handshake, and exchange phases each receive a separate monotonic deadline; equality is expired.

Each readiness call advances at most one connect, TLS handshake, bounded write, or bounded read.
Only a validated Protocol 2 redirect destroys TLS, closes the old descriptor, opens the new route,
and repeats authentication before a fresh session. Generic EOF, reset, timeout, authorization,
allocation, server, and protocol failures are sticky and do not replay. Completion destroys TLS,
closes TCP, and exposes only the retry owner's complete terminal result.

## Consequences

The native library now owns the full TCP/mutual-TLS redirect carrier for one exact finite query.
Memory remains bounded by the retry configuration plus one I/O chunk and one live TCP/TLS attempt.
Work is linear in transferred bytes plus logarithmic route selection. One event-loop thread
serializes methods, so no inter-thread memory-ordering argument applies. No durable or wire format
changes.

The carrier intentionally has no poll loop, whole-operation deadline, cancellation owner, route
file parser, credential reload, connection pool, CLI, or authority for collapsing a multi-group
query onto one leader. A subsequent execution owner and packaged command will compose those
existing deployment facilities after authoritative single-group server redirects exist.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): every fresh connection authenticates the exact node
  selected by validated group authority.
- [Invariant 6](../architecture/invariants.md): the carrier delegates exact SQL replay and complete
  result ownership to one retry owner.
- [Invariant 11](../architecture/invariants.md): TCP owns its descriptor, TLS borrows it, reverse
  destruction is fixed, and route/authentication authorities have explicit borrowed lifetimes.
- [Invariant 14](../architecture/invariants.md): the carrier changes no Protocol 2 bytes or
  connection-local request identity rules.
- [Invariant 18](../architecture/invariants.md): bounded I/O, phase deadlines, authentication, and
  sticky terminal failure cannot be bypassed.

## Validation

A real-loopback test runs two mutual-TLS servers with one-byte application I/O, authenticates nodes
one and two, receives a canonical redirect, proves request ID one and byte-exact SQL on both fresh
sessions, validates `QUERY_RESULT` plus `QUERY_END`, and publishes only the complete encoded result.
Negative tests prove an abrupt post-request close does not replay and invalid I/O bounds plus exact
connect-deadline expiry fail closed. Existing ingest-carrier tests prove the extracted authorization
header preserves behavior. Header self-containment, installed consumption, formatting,
warnings-as-errors, static analysis, sanitizers, and the serialized suite remain required gates.

## Migration and rollback

This is additive except for the source-header relocation of an unchanged public interface. Existing
includes remain source-compatible because the QUORUM_SYNC carrier header includes the neutral
header. Rollback removes the query carrier and restores the declaration without changing ABI,
protocol bytes, or durable state.

## References

- [Exact native finite-query redirect replay](0424-exact-native-query-redirect-replay.md)
- [Deadline-bound native QUORUM_SYNC TCP client](0415-deadline-bound-native-quorum-ingest-tcp-client.md)
- [Authentication and TLS integration boundary](0066-authentication-and-tls-integration-boundary.md)
- [Native Protocol v2](../protocol/native-v2.md)
