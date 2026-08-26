# ADR 0540: Mutually authenticated grouped reducer-job control session

- **Status:** accepted
- **Date:** 2026-08-26
- **Owners:** ChronosDB distributed-query, networking, and security maintainers
- **Extends:** [ADR 0539](0539-header-first-grouped-reducer-job-control-transport.md)

## Context

Bounded readers and cursors did not prove who sent PREPARE or SEAL, who answered, whether a response
belonged to the request, or how long partial TLS I/O could remain live. The reducer-job service must
never see unauthenticated bytes, and a coordinator must not route sources using an endpoint from a
different query or reducer.

## Decision

Add one single-thread-affine client and server session over an already-connected nonblocking mutual-
TLS socket. Both have finite handshake and exchange deadlines and retain the exact bounded
request/response transport owners from ADR 0539.

After its TLS handshake, the client maps the verified server certificate to a stable nonzero
principal and authorizes that principal for the request's claimed target reducer node. Only then
does it write one canonical PREPARE or SEAL. It accepts one exact response only when action, query,
coordinator, and target all equal the request. A structurally valid response with different
correlation fails as corruption and cannot publish its status or endpoint.

The server completes TLS and maps the verified client certificate before reading application
bytes. An unauthorized or zero principal fails without invoking the job service. After one exact
decoded request, it passes the authenticated peer result and admission time to the bounded service;
the service performs coordinator-node authorization and target validation. The server encodes the
service result and publishes no response byte until the complete canonical response owner exists.

TLS sessions own their TLS objects and partial I/O. Authentication policy, node authorization,
service, contexts, and the connected descriptor lifetime are borrowed from the embedding and must
outlive the session. The embedding closes the descriptor only after destroying the session.

## Consequences

Reducer-job admission and sealing now have a complete authenticated, deadline-bound, exactly
correlated session boundary. Authentication precedes all application allocation driven by remote
bytes, and failed correlation cannot influence shuffle routing.

The session deliberately does not connect, listen, admit multiple sockets, multiplex the committed
query-control endpoint, or poll reducer jobs. Shared endpoint ownership and packaged daemon
composition remain separate.

A coordinator-side composite now validates the complete request, deployment limits, numeric route,
timeouts, TLS context, authenticator, and authorizer before opening one nonblocking TCP socket. It
owns the connect deadline and `SO_ERROR` completion, then transfers the descriptor to this TLS
session while retaining teardown order. This is a single-attempt building block; reducer-set
PREPARE/SEAL scheduling and retry policy remain above it.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): response publication requires exact request action,
  query, coordinator, and target identity.
- [Invariant 10](../architecture/invariants.md): authenticated bytes still pass header and complete
  frame integrity before service publication.
- [Invariant 11](../architecture/invariants.md): TLS, partial reads/writes, dependency lifetimes, and
  descriptor teardown order are explicit.
- [Invariant 14](../architecture/invariants.md): mutual TLS encloses the unchanged 1.0 request and
  response protocols.
- [Invariant 15](../architecture/invariants.md): authentication, frame limits, one exchange, and
  handshake/exchange deadlines bound every session.
- [Invariant 18](../architecture/invariants.md): the carrier adds identity and deadline checks
  without weakening job-service proof or seal ordering.

## Validation plan

Use real nonblocking socket pairs and mutual TLS. PREPARE one all-local reducer job and issue SEAL on
a distinct connection, requiring both complete correlated responses and certificate fingerprints.
Deny the client principal and prove the service saw no PREPARE. Send a valid response for a different
query and require client corruption. Expire a handshake exactly at its deadline. Sweep client
construction and inject server-owner allocation failure, requiring resource exhaustion. Run full
cluster, allocation-failure, sanitizer, formatting, static-analysis, and diff gates.

## Migration or rollback considerations

No durable or wire bytes change. Rollback removes an unadvertised standalone session. Any shared
listener dispatch using it must be removed first; reducer daemons must not fall back to
unauthenticated control.

## Unresolved questions

- Compose finite reducer-set PREPARE/SEAL scheduling and whole-query cancellation above the
  single-attempt TCP client.

[ADR 0541](0541-shared-grouped-reducer-job-control-endpoint.md) now owns bounded multi-connection
dispatch and job progress under the committed query endpoint.

## References

- [Header-first control transport](0539-header-first-grouped-reducer-job-control-transport.md)
- [Bounded reducer-job service](0538-bounded-grouped-shuffle-reducer-job-service.md)
- [Authenticated shared query-control endpoint](0467-authenticated-shared-query-control-endpoint.md)
