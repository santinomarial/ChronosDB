# ADR 0335: Bounded grouped-query mutual TLS

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0173](0173-bounded-outbound-distributed-query-tls-carrier.md),
  [ADR 0174](0174-bounded-inbound-distributed-query-tls-carrier.md),
  [ADR 0331](0331-bounded-grouped-query-partial-io.md),
  [ADR 0332](0332-authenticated-grouped-query-receiver.md)

## Context

The ungrouped mutual-TLS carriers own exactly one response, while a successful grouped tablet can
produce many correlated partials before its terminal frame. Reusing the unary closure rule would
truncate valid streams. Exposing each decoded prefix directly would instead permit a later TLS,
correlation, or sequence failure to leak partial query success.

## Decision

`DistributedGroupedQueryTlsClient` and `DistributedGroupedQueryTlsServer` are move-only,
single-threaded owners for one already-connected nonblocking `TlsSocket`. Each readiness call
performs at most one TLS operation. Both apply separate positive handshake and exchange timeouts
using caller-supplied monotonic time and make every terminal failure sticky.

Before any protocol byte, each side finishes the maintained mutual-TLS handshake, maps the exact
peer-certificate fingerprint through `ConnectionAuthenticator`, and applies its existing node
authorization boundary. The client exact-decodes its immutable attempt before construction and
authorizes the authenticated server principal for that exact target. The server authenticates the
client before reading, then delegates source authorization and local-target validation to the
authenticated grouped receiver.

The client owns its validated request cursor, fixed maximum response scratch/reader storage, and a
value-owned response vector bounded by the configured positive frame limit and the coordinator's
65,536-message hard ceiling. It exact-correlates reverse route, query, and tablet, requires
one-based contiguous success sequences, accepts terminal-only only at sequence one, and rejects a
failure after any successful prefix. `responses()` remains unavailable until a terminal partial,
terminal-only frame, or single failure frame completes the stream. Any carrier failure clears the
retained prefix.

The server owns fixed maximum request scratch/reader storage. After one complete canonical request,
it invokes the authenticated receiver exactly once, constructs every response cursor before
writing, and writes that bounded vector in exact order. It completes only after the final cursor is
fully consumed. TLS context and connection descriptor lifetime remain embedding-owned and must
outlive the carrier.

No durable or network bytes change. TCP connection/listener acquisition, retry arbitration,
sender/coordinator integration, and packaged multi-tablet grouped execution remain separate.

## Consequences and validation

Client memory is bounded by one maximum request, two fixed 252-byte response buffers, and at most
the configured number of decoded response values. Server memory is bounded by two maximum-request
buffers plus the receiver's bounded encoded vector and one cursor per response. Work is linear in
the complete request and response byte stream. One event-loop thread serializes all calls, so no
synchronization or memory-ordering argument is required.

A focused real nonblocking socket-pair test drives mutual TLS on both endpoints, proves both
certificate fingerprints are authenticated, authorizes the exact principal/node mappings, invokes
the receiver once, and returns two exact contiguous grouped partials only after terminal closure.
A second case proves invalid response bounds are rejected and the exact handshake deadline becomes
a sticky `UNAVAILABLE` failure. The installed external-consumer gate constructs both public owners.

ADR 0336 subsequently supplies outbound nonblocking TCP connection ownership. Inbound TCP
listener/server ownership, mid-stream peer-close fault injection, sender/coordinator integration,
packaged multi-tablet grouped execution, multi-process failover, and broad fault/measurement
evidence remain incomplete. No Phase 16 exit gate is claimed.

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## References

- [Distributed Grouped FLOAT64 Query Transport v1](../formats/distributed-grouped-float64-query-transport-v1.md)
- [Authenticated grouped query receiver](0332-authenticated-grouped-query-receiver.md)
- [Bounded grouped query partial I/O](0331-bounded-grouped-query-partial-io.md)
- [Maintained mutual-TLS client socket](0172-maintained-mutual-tls-client-socket.md)
