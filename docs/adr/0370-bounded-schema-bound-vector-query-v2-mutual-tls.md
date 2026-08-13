# ADR 0370: Bounded schema-bound vector query v2 mutual TLS

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB cluster, query, and networking maintainers
- **Extends:** [ADR 0173](0173-bounded-outbound-distributed-query-tls-carrier.md),
  [ADR 0174](0174-bounded-inbound-distributed-query-tls-carrier.md),
  [ADR 0369](0369-authenticated-schema-bound-vector-query-receiver-v2.md)

## Context

The authenticated v2 receiver still required embeddings to join mutual-TLS readiness, certificate
fingerprint authentication, request/response short I/O, terminal stream closure, schema lifetime,
and timeouts. Unlike the older fixed-size aggregate carriers, one vector request can exceed 4 MiB
and one response can approach 16 MiB, so fixed maximum-frame scratch arrays would make every live
session unnecessarily large.

## Decision

`DistributedVectorQueryTlsClientV2` and `DistributedVectorQueryTlsServerV2` are move-only,
single-threaded owners for one already-connected nonblocking `TlsSocket`. Every readiness call
performs at most one TLS operation. Separate positive handshake and exchange timeouts use
caller-supplied monotonic time and make terminal failure sticky.

Before protocol bytes, both sides complete mutual TLS and map the verified peer-certificate
fingerprint through the borrowed `ConnectionAuthenticator`. The client additionally authorizes the
authenticated server principal for the exact immutable target node. The server authenticates the
client before any request read, then delegates claimed-source authorization and local-target
validation to `DistributedVectorQueryReceiverV2`.

Client creation exact-decodes its attempt, exact-matches the attempt target, reconstructs a typed
request cursor, and transfers the Fragment-v2 result schema into its response reader. It retains
decoded responses internally but exposes them only after one failure response or a contiguous
one-based success stream reaches a terminal Result-Exchange-v2 payload. Reverse route, query,
tablet, schema, sequence, frame count, and total encoded response bytes are checked. Any terminal
failure clears the retained prefix; a coalesced suffix after terminal is corruption.

The server uses the header-first v2 request reader, rejects a coalesced second request, invokes the
authenticated receiver once, and revalidates the complete encoded response vector against the
request schema and correlation before constructing typed response cursors. It completes only after
the last cursor is fully written. Both sides use fixed 16-KiB TLS scratch arrays; the existing
header-first readers own only the exact current frame after its fixed header passes.

TLS limits independently bound response frames and the sum of encoded response bytes, with the same
65,536-frame and 1-GiB hard ceilings as the receiver. TLS contexts, descriptors, authenticators,
authorizers, and the receiver remain embedding-owned and must outlive their carriers. No retry
policy, listener, connector, or background callback is owned here.

## Alternatives considered

- **Allocate maximum request/response scratch per connection:** rejected because it would retain
  more than 20 MiB before useful work in every session.
- **Infer the response schema from the first response:** rejected because the admitted Fragment-v2
  schema is the authority and must be owned before any response byte is interpreted.
- **Expose each decoded response immediately:** rejected because a later TLS, sequence, or schema
  failure could leak partial query success.
- **Reuse the connection for another request:** rejected because multiplexing, correlation, and
  cancellation require a separately versioned bounded owner.

## Consequences

No application byte is written or read before the peer certificate maps to an authorized node
principal. Retained carrier memory is bounded by one canonical request cursor, one exact current
reader frame, 16-KiB scratch, decoded responses under count/byte limits, and constant TLS/deadline
metadata. Work is linear in the complete request and response stream. One event-loop thread
serializes calls, so no synchronization or memory-ordering argument is required.

TCP connection acquisition, listener/admission ownership, retry arbitration, production vector
worker execution, schema-bound coordination, and process integration remain incomplete.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): TLS adds no application framing or version
  reinterpretation.
- [Invariant 6](../architecture/invariants.md): header-first readers and independent count/byte
  limits bound retained untrusted data.
- [Invariant 10](../architecture/invariants.md): the Fragment-v2 schema is transferred into the
  client reader and applied again by the server before response writes.
- [Invariant 14](../architecture/invariants.md): reverse route and query/tablet identity remain exact
  across the authenticated connection.
- [Invariant 15](../architecture/invariants.md): certificate authentication and node authorization
  precede application I/O; no consistency or protocol downgrade exists.
- [Invariant 18](../architecture/invariants.md): socket/context borrowing, move-only ownership,
  single-thread affinity, and terminal publication are explicit.

## Validation plan

A real nonblocking socket-pair test completes mutual TLS on both endpoints, proves both certificate
fingerprints are authenticated, invokes the receiver once, and exposes two schema-bound responses
only after terminal closure. Focused cases prove invalid count/byte bounds, attempt-target mismatch,
the exact sticky handshake deadline, and rejection of a valid 200-byte terminal response under a
199-byte client limit. Header self-containment, installed consumption, ASan/UBSan, relevant static
analysis, formatting, and the full serialized suite are required before completion.

## Migration or rollback considerations

No durable or network bytes change. Embeddings can adopt these carriers around already-connected
mutual-TLS sockets. Rollback returns readiness glue to the embedding, which must preserve
authentication-before-I/O, exact schema ownership, complete-stream publication, count/byte bounds,
and sticky deadlines.

## References

- [Distributed Vector Query Transport v2](../formats/distributed-vector-query-transport-v2.md)
- [Authenticated schema-bound vector query receiver v2](0369-authenticated-schema-bound-vector-query-receiver-v2.md)
- [Maintained mutual-TLS client socket](0172-maintained-mutual-tls-client-socket.md)
