# ADR 0308: Outbound Raft observation mTLS acquisition

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB cluster, networking, security, query, and Raft maintainers
- **Extends:** [ADR 0307](0307-bounded-raft-observation-partial-io.md)

## Context

Canonical observation frames and partial-I/O owners still left outbound embeddings to join mutual
TLS, node authorization, deadlines, and response correlation. Incorrect glue could write before
authorizing the remote node, wait forever, or expose a response from another acquisition.

## Decision

`RaftObservationTlsClient` owns one exact request over one already-connected maintained `TlsSocket`.
It completes the handshake, maps the verified certificate fingerprint through the configured
authenticator, and authorizes that principal for the immutable target node before writing any
request byte. It then uses the canonical cursor and bounded response reader, enforcing separate
positive handshake and exchange deadlines.

A response must reverse source/target and repeat the exact group and correlation identity. The
client exposes an observation only after that check; a correlated non-OK status becomes the exact
result error. Carrier failures and deadlines are sticky. One event-loop thread serializes calls;
the TLS context, descriptor, authenticator, and authorizer are borrowed and must outlive it.

TCP connection ownership, address selection, inbound serving, retries, fan-out, pair selection, and
packaged bounded-stale construction remain separate tasks.

## Consequences and validation

The client retains one 84-byte request, one bounded response reader, a 4 KiB read scratch buffer,
TLS session state, and constant correlation/deadline metadata. Focused real-mTLS tests prove exact
authenticated acquisition, denial before request writing, and exact sticky timeout behavior. The
installed consumer covers the public interface. Broader disconnect, allocation, and multi-node
matrices remain deferred.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Raft Observation Transport v1](../formats/raft-observation-transport-v1.md)
- [Maintained mutual-TLS client socket](0172-maintained-mutual-tls-client-socket.md)
- [Bounded Raft observation partial-I/O ownership](0307-bounded-raft-observation-partial-io.md)
