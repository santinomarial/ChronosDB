# ADR 0174: Bounded inbound distributed-query TLS carrier

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** cluster, query, and network subsystems
- **Extends:** [ADR 0168](0168-authenticated-distributed-query-transport.md), [ADR 0173](0173-bounded-outbound-distributed-query-tls-carrier.md)

## Context

The outbound attempt carrier could originate an authenticated exchange, but a cluster listener still
had to join accepted TLS readiness, client-certificate authentication, bounded request framing,
receiver dispatch, and response short writes. Incorrect glue could parse request bytes before
authentication, invoke the worker more than once, accept a coalesced second request without an
explicit multiplexing contract, or retain a stalled session forever.

## Decision

`DistributedQueryTlsServer` is a move-only, single-owner carrier for exactly one inbound exchange
over one accepted nonblocking `TlsSocket`. The event-loop caller supplies readable/writable readiness
and monotonic time, receives the next readiness interest, and remains responsible for destroying the
session before closing its borrowed descriptor.

The carrier first completes the maintained mutual-TLS handshake and maps the verified client
certificate fingerprint through its borrowed `ConnectionAuthenticator`. Only an authorized nonzero
stable principal permits request reads. It then receives at most one canonical request into a fixed
16,772-byte buffer and the existing fixed reader. A coalesced suffix is a protocol failure rather
than an implicit next request.

The complete exact request and authenticated principal are passed once to the borrowed
`DistributedQueryReceiver`. That boundary authorizes the principal for the claimed source node,
checks the local target, invokes the proof-revalidating worker service, and constructs one correlated
canonical response. The carrier owns that response through the existing move-only write cursor and
becomes complete only after all response bytes have been accepted by TLS.

Positive handshake and exchange timeouts expire exactly at their monotonic deadlines as sticky
`UNAVAILABLE` failures. Authentication, decoding, receiver, worker-result, and write failures are
also sticky. The carrier performs at most one TLS operation per readiness call and owns no event
loop, listener, descriptor, clock, retry, or background callback. The TLS server context,
authenticator, receiver, and descriptor must outlive it; one event-loop thread serializes calls.

## Consequences and validation

Each admitted connection retains the TLS session, one 16,772-byte carrier request array, the
reader's separate fixed array, at most one bounded encoded response vector, and constant readiness/
deadline/authentication metadata. Processing is linear in the request and response lengths, and no
peer-declared length allocates carrier storage.

A real nonblocking socket-pair test drives the outbound and inbound carriers together through
maintained mutual TLS. It verifies both certificate fingerprints are authenticated, the client
principal is authorized for source node 1, the worker runs exactly once, the response completes,
and the original sender accepts its exact bytes. Separate tests reject an unauthorized certificate
principal before worker dispatch and prove invalid configuration plus exact sticky timeout behavior.

Listener/connector ownership, descriptor admission caps, cancellation frames, multiplexed pooled
connections, asynchronous worker completion, leader hints, and multi-node fault simulation remain
separate work.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## Alternatives considered

- **Route cluster frames through the native client reactor:** rejected because cluster principals,
  protocol framing, authorization, and lifecycle are a separate trust domain.
- **Read before the TLS principal is mapped:** rejected because even decoded work must not cross the
  authentication boundary.
- **Keep a connection open for an implicit next request:** rejected because multiplexing and request
  correlation need an explicit bounded protocol owner.
- **Retry worker execution inside the carrier:** rejected because authority refresh and sender retry
  are coordinator policy, not socket readiness behavior.

## Migration and rollback

This adds no durable or wire format. A listener can wrap each accepted maintained TLS session in
this owner and close the descriptor after terminal state. Equivalent custom integration must retain
authentication-before-read, exact one-request framing, once-only receiver dispatch, bounded
deadlines, and complete-response ownership. Removing it returns inbound readiness glue to the
embedding without changing existing codecs or receivers.

## References

- [Authenticated distributed query transport](0168-authenticated-distributed-query-transport.md)
- [Bounded distributed query carrier lifecycle](0169-bounded-distributed-query-carrier-lifecycle.md)
- [Maintained mutual-TLS client socket](0172-maintained-mutual-tls-client-socket.md)
- [Bounded outbound distributed-query TLS carrier](0173-bounded-outbound-distributed-query-tls-carrier.md)
- [Distributed Query Transport v1](../formats/distributed-query-transport-v1.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
