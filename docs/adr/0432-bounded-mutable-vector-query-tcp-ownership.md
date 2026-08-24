# ADR 0432: Bounded mutable vector query TCP ownership

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB cluster, networking, query, and security maintainers
- **Extends:** [ADR 0431](0431-mutual-tls-mutable-vector-query-carrier.md),
  [ADR 0175](0175-nonblocking-ipv4-tcp-descriptor-ownership.md)

## Context

The mutable query mutual-TLS carrier required an already-connected socket and therefore did not
own connection establishment, listener admission, descriptor lifetime, or polling. Leaving those
operations to each embedding would duplicate deadline and teardown rules at exactly the boundary
where a split-leader query crosses processes.

## Decision

`DistributedMutableVectorQueryTcpClient` is a move-only, single-threaded nonblocking owner for one
immutable attempt. It validates the complete request, target, TLS dependencies, peer address, and
limits before opening a socket. It gives connection establishment a distinct exact monotonic
deadline, confirms completion through `SO_ERROR`, creates TLS only after connection success, and
then delegates protocol readiness to the accepted mutable TLS client. The TLS carrier is destroyed
before the descriptor it borrows. Responses remain unavailable until the carrier publishes a
complete terminal vector.

`DistributedMutableVectorQueryTcpServer` owns one nonblocking IPv4 listener, one long-lived TLS
context, preallocated poll storage, and a finite vector of stable heap-allocated connection
records. It bounds total active connections and accepts per poll, authenticates through the
mutable TLS server before request bytes, advances each connection at most once per poll, and
removes completed or failed carriers before their descriptors. Startup rejects invalid limits
before listening. Shutdown closes every connection, closes the listener, and is idempotent.

The client and server contain no retry, route-selection, worker-acquisition, or native-result
policy. Those remain the responsibility of subsequent execution and packaged-service owners.

## Consequences

One reusable owner now defines real TCP connect/listen behavior for the distinct mutable protocol.
Connection, handshake, exchange, frame-count, response-byte, active-connection, and accept-batch
bounds are all explicit. Server metrics saturate rather than wrapping and distinguish accepted,
rejected, completed, failed, and accept-error outcomes.

All calls for one owner are serialized by one event-loop thread. No inter-thread memory-ordering
algorithm is introduced. Stable heap records prevent vector relocation from invalidating carrier
or socket addresses; reverse member destruction and explicit removal preserve carrier-before-
descriptor teardown.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): the exact proof-bound attempt is retained through
  connection establishment and TLS exchange.
- [Invariant 10](../architecture/invariants.md): TLS wraps the independently checksummed mutable
  request and schema-bound response frames.
- [Invariant 14](../architecture/invariants.md): TCP ownership does not reinterpret the distinct
  mutable protocol version.
- [Invariant 15](../architecture/invariants.md): deadlines, admission, accepts, connections, frames,
  and bytes are finite.
- [Invariant 18](../architecture/invariants.md): connect, authentication, route, protocol, and
  terminal failures close the connection without publishing partial results.

## Validation

A real loopback test drives nonblocking connect, bounded listener admission, mutual TLS with both
certificate fingerprints, source/target node authorization, one worker invocation, one complete
terminal response, metrics, and idempotent shutdown. A deterministic deadline test proves the
connect expires at the exact boundary, closes its descriptor, and remains sticky. An allocation
sweep covers request revalidation and client-owner construction and requires every injected
failure to return `RESOURCE_EXHAUSTED`. Public-header self-containment and installed-package
linkage cover the exported API.

## Migration and rollback

This is additive and is not yet composed into the packaged daemon. Rollback removes the TCP owners
without changing durable bytes, mutable request bytes, or shared vector response bytes.

## References

- [Distributed Mutable Vector Query Transport v1](../formats/distributed-mutable-vector-query-transport-v1.md)
- [Mutual-TLS mutable vector query carrier](0431-mutual-tls-mutable-vector-query-carrier.md)
- [Transport security policy](../operations/transport-security.md)
