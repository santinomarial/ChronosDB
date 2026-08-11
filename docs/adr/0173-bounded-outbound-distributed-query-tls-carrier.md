# ADR 0173: Bounded outbound distributed-query TLS carrier

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** cluster, query, and network subsystems
- **Extends:** [ADR 0169](0169-bounded-distributed-query-carrier-lifecycle.md), [ADR 0172](0172-maintained-mutual-tls-client-socket.md)

## Context

The distributed-query sender produced immutable bounded attempts, and the maintained TLS client
could authenticate a server on a connected descriptor, but embeddings still had to join TLS
readiness, application-principal authorization, short writes, response fragmentation, and timeouts.
Inconsistent glue could emit query bytes before node authorization, retain an attempt forever, feed
a coalesced response suffix into another attempt, or introduce a retry policy competing with the
existing sender.

## Decision

`DistributedQueryTlsClient` is a move-only single-owner carrier for exactly one
`DistributedQueryAttempt` and one already-connected nonblocking `TlsSocket`. Creation exact-decodes
the attempt and verifies that its claimed target equals its embedded canonical request. It then owns
the validated request write cursor and a fixed 244-byte response buffer plus response reader.

The event-loop caller supplies readable/writable readiness and monotonic time. The carrier exposes
the next read/write interest and performs at most one TLS operation per call. It first completes the
maintained TLS handshake, maps the verified server-certificate fingerprint through the borrowed
`ConnectionAuthenticator`, and authorizes that stable principal for the immutable target node
through `ClusterNodePrincipalAuthorizer`. No request byte is written before both checks succeed.

After authorization, the carrier writes only the cursor's unwritten suffix and reads at most one
canonical response. A coalesced suffix is a protocol error rather than input to another attempt.
The exact received response bytes remain borrowed from the completed carrier so the existing sender
performs the definitive reverse-route/query/tablet correlation and retry transition.

Separate positive handshake and exchange timeouts are converted to sticky `UNAVAILABLE` transport
failures at exact monotonic deadlines. The carrier contains no retry policy: its caller reports a
terminal carrier failure to `DistributedQuerySender` or `DistributedQueryExecution`, which alone
decides bounded retry/backoff.

The TLS context, descriptor, authenticator, and node authorizer remain embedding-owned and must
outlive the carrier/session. One event-loop thread serializes all calls, so no synchronization or
memory-order argument is needed.

## Consequences and validation

Each live attempt retains its bounded request vector, one 244-byte carrier response array, the
reader's separate 244-byte fixed array, TLS session state, and constant deadline/interest metadata.
Work is linear in request plus response bytes. No peer-declared value allocates carrier storage.

A real nonblocking socket-pair test drives maintained TLS on both endpoints, verifies certificate
fingerprint authentication and target-node authorization, streams the request and response, and
feeds the exact retained response into the sender. Separate tests prove a rejected server principal
fails before request writing, attempt/target mismatch fails at construction, pre-deadline calls do
not expire, and the exact deadline creates a sticky retryable failure.

Inbound distributed-query TLS serving, connector/address ownership, cancellation frames, pooled
connections, and multi-node failure simulation remain separate work.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## Alternatives considered

- **Put TLS and timers inside `DistributedQuerySender`:** rejected because sender policy is portable
  and deterministic while descriptors and readiness belong to an event-loop embedding.
- **Trust only the certificate hostname:** rejected because cluster authorization also binds the
  authenticated application principal to the claimed target node.
- **Copy a response into a dynamically sized buffer:** rejected because the protocol already has a
  244-byte hard maximum.
- **Reuse a connection for a coalesced next response:** rejected because one carrier represents one
  exact attempt; multiplexing needs an explicit bounded correlation owner.

## Migration and rollback

This adds no durable or wire format. Embeddings may continue equivalent event-loop integration only
if they preserve authentication-before-write, exact attempt binding, fixed response bounds, sticky
deadlines, sole sender retry authority, and response lifetime through sender correlation. Removing
the class returns outbound socket scheduling to the embedding.

## References

- [Authenticated distributed query transport](0168-authenticated-distributed-query-transport.md)
- [Bounded distributed query carrier lifecycle](0169-bounded-distributed-query-carrier-lifecycle.md)
- [Maintained mutual-TLS client socket](0172-maintained-mutual-tls-client-socket.md)
- [Distributed Query Transport v1](../formats/distributed-query-transport-v1.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
