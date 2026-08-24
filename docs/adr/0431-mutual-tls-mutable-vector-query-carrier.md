# ADR 0431: Mutual-TLS mutable vector query carrier

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB cluster, networking, query, and security maintainers
- **Extends:** [ADR 0430](0430-distinct-mutable-vector-query-transport.md),
  [ADR 0369](0369-mutual-tls-schema-bound-vector-query-v2-carrier.md)

## Context

Mutable Query Transport v1 required a previously authenticated peer result but did not own the
handshake that produced it. Passing plaintext request bytes into that receiver would leave the
source identity assertion outside the implemented boundary. Split-leader execution needs the same
certificate and deadline guarantees already used by the durable vector-v2 carrier without
reinterpreting its request protocol.

## Decision

`DistributedMutableVectorQueryTlsClient` and `DistributedMutableVectorQueryTlsServer` are move-only,
single-threaded nonblocking state machines over `network::TlsSocket`. They reuse the established
vector-v2 timeout, response-count, response-byte, state, and readiness types while retaining the
distinct mutable request reader and writer.

The client completes mutual TLS, derives the server certificate fingerprint, authenticates the
principal, and authorizes that principal for the immutable target node before writing any request
byte. It retains the exact expected result schema, validates every correlated response frame,
enforces contiguous terminal sequencing and total bounds, and clears incomplete results on any
failure.

The server completes mutual TLS and authenticates the client certificate before reading request
bytes. It passes the resulting authenticated peer to the mutable receiver, exact-decodes the
receiver's complete bounded response vector against the request schema, and then writes the frames
in order. Handshake and exchange deadlines are exact and sticky. TLS contexts, authenticators,
authorizers, receivers, and their dependencies outlive the carrier.

## Consequences

No mutable fragment crosses the carrier before both transport and node-principal authority are
established. TLS supplies confidentiality and peer authentication; the request and nested CRCs
retain framing/integrity diagnostics. The carrier owns bounded request, scratch, writer, and
response storage and publishes no prefix.

All methods are serialized by one event-loop thread, so no inter-thread memory-ordering algorithm
is introduced. Actual TCP connect/listen ownership, route scheduling, packaged worker integration,
and native split-leader composition remain subsequent boundaries.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): only the proof-bound committed fragment is sent.
- [Invariant 6](../architecture/invariants.md): schema and mutable publication authority remain one
  immutable request.
- [Invariant 10](../architecture/invariants.md): TLS wraps independently checksummed frames.
- [Invariant 14](../architecture/invariants.md): the distinct mutable request version is preserved.
- [Invariant 15](../architecture/invariants.md): deadlines, frame counts, and bytes are bounded.
- [Invariant 18](../architecture/invariants.md): authentication, route, correlation, and terminal
  mismatch fail closed.

## Validation

A real socket-pair/mutual-TLS test verifies both certificate fingerprints, target-node
authorization, one exact worker call, and one complete schema-bound response. Negative tests prove
an unauthorized server principal fails before request writing, mismatched attempts reject before
I/O, and deadlines expire exactly and stay sticky. Allocation sweeps cover client and server owner
construction and caught a false `noexcept` server constructor; exhaustion now returns
`RESOURCE_EXHAUSTED` instead of terminating.

## Migration and rollback

This is additive and not yet installed behind a packaged TCP listener. Rollback removes the mutable
TLS carrier without changing request, response, or durable bytes.

## References

- [Distributed Mutable Vector Query Transport v1](../formats/distributed-mutable-vector-query-transport-v1.md)
- [Mutual-TLS schema-bound vector query v2 carrier](0369-mutual-tls-schema-bound-vector-query-v2-carrier.md)
- [Transport security policy](../operations/transport-security.md)

