# ADR 0310: Bounded inbound Raft observation mTLS session

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB cluster, networking, security, query, and Raft maintainers
- **Extends:** [ADR 0306](0306-authenticated-raft-observation-transport.md),
  [ADR 0307](0307-bounded-raft-observation-partial-io.md)

## Context

The authenticated receiver and stream owners did not join an accepted mTLS socket to client
certificate authentication, one-request framing, once-only observation dispatch, response short
writes, and session deadlines. Ad hoc glue could read before authentication, invoke the service
twice, or accept an implicit second request.

## Decision

`RaftObservationTlsServer` owns exactly one inbound exchange over one accepted maintained
`TlsSocket`. It completes the handshake, maps the verified client-certificate fingerprint through
the configured authenticator, and permits no request read until that yields an authorized nonzero
principal.

The session retains one fixed request buffer and canonical reader. It rejects a coalesced suffix,
passes the exact request and authenticated principal once to `RaftObservationReceiver`, then owns
the canonical response until every byte is accepted by TLS. Separate positive handshake and
exchange deadlines expire as sticky failures. One event-loop thread serializes calls; the TLS
context, descriptor, authenticator, and receiver are borrowed and must outlive the session.

This object owns no listener, descriptor, admission table, retry, fan-out, or worker thread.

## Consequences and validation

Each session retains the reader's fixed 84-byte store, a fixed 168-byte carrier read buffer, one
bounded response vector, TLS state, and constant deadline/authentication metadata. A real
nonblocking mTLS pair test proves exact
end-to-end observation and one service invocation. Separate tests reject the client principal before
service dispatch and prove invalid configuration plus exact sticky deadline behavior. The installed
consumer covers the public session interface.

Bounded listener admission, remote peer fan-out, pair selection, and packaged query construction
remain incomplete.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Authenticated Raft observation transport](0306-authenticated-raft-observation-transport.md)
- [Bounded Raft observation partial-I/O ownership](0307-bounded-raft-observation-partial-io.md)
- [Bounded inbound distributed-query TLS carrier](0174-bounded-inbound-distributed-query-tls-carrier.md)
