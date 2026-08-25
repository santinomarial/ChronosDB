# ADR 0462: Bounded Raft read-authority partial I/O and mutual TLS

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB cluster transport, networking, security, query, and Raft maintainers
- **Extends:** [ADR 0461](0461-authenticated-remote-raft-read-authority.md)

## Context

The canonical read-authority codec and receiver accepted only complete frames. A nonblocking carrier
must survive fragmentation and short writes, reject hostile lengths before allocation, authenticate
both peers before protocol dispatch, impose finite deadlines, and never issue one barrier request
twice because readiness changed.

## Decision

Add header-first request and response length gates, fixed request storage, an exactly bounded response
reader, and a move-only frame write cursor. Each reader consumes at most one frame per call, reports
the exact consumed prefix, resets only after complete decode, and makes failure sticky. The response
reader retains only its fixed 128-byte header until status, presence, route, version, CRC, reserved
bytes, and the configured nested-observation bound establish one exact safe allocation. Moving a
partial response reader transfers progress and leaves the source empty. Moving a cursor transfers
the sole write obligation and leaves the source complete.

`RaftReadAuthorityTlsClient` owns one request over an already-connected maintained TLS socket. It
completes mutual-TLS handshake, maps the verified server certificate to a stable principal, and
authorizes that principal for the exact target node before writing. It accepts only a reverse-route,
same-group, same-correlation response and exposes only a canonical success authority.

`RaftReadAuthorityTlsServer` owns one accepted TLS socket and authenticates the client certificate
before reading request bytes. It rejects coalesced suffixes, dispatches one exact request once to the
authenticated receiver, then retains the canonical response until every byte is accepted by TLS.
Both owners have separate positive handshake and exchange deadlines and sticky terminal failure.

These owners are single-thread-affine and perform no internal synchronization. TLS contexts,
descriptors, authenticators, authorizers, and receivers are borrowed and must outlive their owner.
The caller serializes readiness callbacks and destroys the owner before closing its descriptor.

## Consequences

Request reading is allocation-free. The default maximum authority frame is 1,296 bytes; the hard
configured maximum is 131,376 bytes. A response reader retains its 128-byte header plus at most one
validated frame. The client additionally retains a 4 KiB scratch buffer; the server retains a
168-byte two-request detection buffer. Processing is linear in the bounded frame size.

The TLS owners deliberately do not own DNS resolution, TCP connect/listen, descriptor admission,
retry, leader selection, all-group fan-out, or daemon integration. Those remain separate lifecycle
boundaries. No durable or consensus bytes change.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): the carrier transports the complete barrier and
  leader proof without weakening applied/publication requirements.
- [Invariant 6](../architecture/invariants.md): both certificate identities, source/target route,
  group, correlation, term, membership, and indexes remain fail-closed.
- [Invariant 10](../architecture/invariants.md) and
  [Invariant 11](../architecture/invariants.md): every retained buffer and deadline is bounded and
  one owner holds each in-flight request/response.
- [Invariant 14](../architecture/invariants.md): partial I/O reassembles only canonical v1 frames and
  rejects hostile declared lengths before allocation.
- [Invariant 15](../architecture/invariants.md): verified mutual TLS and principal-to-node policy
  precede protocol dispatch in both directions.

## Validation

Focused tests enumerate every request and response split, consume a coalesced successor by exact
prefix, reject hostile headers before allocation, prove sticky failures and moved partial state,
and exercise short-write cursor transfer. A real nonblocking mutual-TLS socket pair proves exact
end-to-end authority acquisition and one service invocation. Separate cases deny the client
principal before service access and enforce exact sticky client/server deadlines. Broader suite,
sanitizer, format, and static-analysis evidence is recorded with the implementing commit. Before
commit, all 214 normal cluster tests and all 28 cluster allocation-failure tests passed with
loopback socket permission. All eight focused authority tests passed under ASan/UBSan with leak
detection disabled because Apple's sanitizer runtime does not support LeakSanitizer. All three
changed production sources passed repository-pinned clang-tidy 18; all changed C++ files passed
clang-format 18; and the diff passed whitespace review.

## Migration or rollback considerations

No wire or durable format change. A deployment must enable the later TCP endpoint only when both
peers support exact v1. Rolling back these unused owners removes no stored state; after integration,
roll back the complete authority endpoint rather than substituting the observation protocol.

## Unresolved questions

- Whether the final daemon endpoint should share admission capacity with observation traffic or use
  a separately measured control listener.
- How finite route retry and whole-query attempt cancellation should surface peer churn.

## References

- [Raft Read Authority Transport v1](../formats/raft-read-authority-transport-v1.md)
- [Maintained mutual-TLS sockets](0172-maintained-mutual-tls-client-socket.md)
- [Linearizable Raft read barriers](../learning/linearizable-raft-read-barrier.md)
