# ADR 0508: Deadline-bound grouped shuffle TCP client

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB cluster, distributed-query, and networking maintainers
- **Extends:** [ADR 0506](0506-bounded-grouped-shuffle-mutual-tls-session.md),
  [ADR 0507](0507-finite-immutable-route-grouped-shuffle-retry.md)

## Context

The grouped-shuffle mutual-TLS client accepted an already connected descriptor, while the retry
owner produced a fresh immutable stream sender for each attempt. Embeddings still had to preserve
that attempt through a nonblocking connect, prove `SO_ERROR`, enforce a distinct connect deadline,
bind authentication metadata to the actual route, and destroy TLS before its borrowed descriptor.
Incorrect glue could open a socket before validating route authority, authenticate a different
address, or report success before the reverse receipt.

## Decision

Add a move-only, single-thread-affine TCP client for one grouped-shuffle retry attempt. Before
opening a descriptor, `begin` validates nonzero attempt identity, exact target agreement with the
stream edge, the edge against the complete immutable authority, all connect/handshake/exchange and
stream limits, required borrowed dependencies, and equality between the authentication peer IPv4
address and the actual remote endpoint.

The client starts one move-only nonblocking `TcpSocket`, requests write readiness while connecting,
and applies a separate positive monotonic connect deadline. It creates no TLS state until
`finish_connect` proves `SO_ERROR` success. It then creates the maintained client `TlsSocket` and
transfers the stream sender into `DistributedVectorGroupedAggregateShuffleTlsClient`, which remains
the sole owner of certificate authentication, destination authorization, stream progression, and
exact receipt validation.

Any connect or carrier failure is sticky and resets the TLS carrier before explicitly closing the
descriptor. Field order enforces the same carrier-before-descriptor destruction on normal owner
destruction. Completion means the nested TLS carrier validated `CHDVGAK1`; TCP connect or stream
write completion is never sufficient. The attempt number and immutable target remain inspectable
for the retry scheduler.

The TLS context, complete authority, authenticator, and node authorizer are borrowed and must
outlive the client. The owner performs no retry, address rotation, listener admission, reducer
installation, or duplicate arbitration. One event-loop thread serializes all calls, so no shared
concurrent algorithm or memory-ordering argument is required.

## Detailed rationale

Separating connection establishment from TLS keeps the kernel's connect result authoritative and
lets the retry policy remain transport independent. Prevalidating all configuration avoids
spending a finite descriptor on an impossible attempt. Exact route/address equality prevents peer
authentication context from describing a different network destination than the socket.

## Alternatives considered

- **Create TLS while connect is in progress.** Rejected because application and TLS I/O cannot
  begin before the connection result is authoritative.
- **Let the embedding pair an arbitrary peer address with the socket.** Rejected because
  certificate authentication and node authorization must describe the route being established.
- **Move retry/backoff into this client.** Rejected because one descriptor/TLS session is one
  attempt and immutable retry state already has a dedicated owner.
- **Retain failed descriptors for diagnostics.** Rejected because terminal attempts must release
  finite kernel resources immediately; the sticky status retains the diagnostic.

## Consequences

One grouped-shuffle attempt is now process-addressable through a deadline-bound outbound TCP/TLS
lifecycle. Memory and kernel ownership remain bounded by one socket, one TLS carrier, and one
complete framed stream. A scheduler can report terminal status to the retry owner without
reconstructing or inspecting partial bytes.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): the immutable authority edge and attempt survive
  unchanged from retry construction through receipt.
- [Invariant 10](../architecture/invariants.md): TCP cannot bypass the existing TLS, stream, frame,
  or receipt validation boundaries.
- [Invariant 11](../architecture/invariants.md): attempt, TLS carrier, descriptor, and borrowed
  dependency lifetimes have explicit destruction order.
- [Invariant 15](../architecture/invariants.md): connect, handshake, exchange, frame, byte, and
  decode limits are independently finite.
- [Invariant 18](../architecture/invariants.md): route acquisition cannot change canonical
  partition identity or success semantics.

## Validation plan

A real loopback test drives nonblocking connect into mutual TLS, authenticates both certificate
fingerprints, authorizes exact source/destination nodes, transfers a two-frame stream, requires the
reverse receipt, and extracts the complete server stream. Negative coverage proves route mismatch
and attempt-target drift fail before connect, while pre-deadline and exact-deadline calls prove
sticky timeout plus descriptor release. Allocation injection classifies owner construction and
implicitly proves the descriptor closes on failure. The warning-as-error ASan/UBSan build, all 282
cluster tests, and all 46 cluster allocation-failure tests pass. Changed-source clang-tidy reaches
only the known LLVM 18/macOS 26 libc++ builtin incompatibility without a ChronosDB-source finding.

## Migration or rollback considerations

No durable or wire bytes change. Rollback removes the TCP composite while retaining retry and
already-connected TLS owners; embeddings must then provide equivalent validated descriptor
acquisition or leave remote shuffle unavailable.

## Unresolved questions

- Add bounded listener/admission ownership and server result delivery.
- Make destination reducer admission idempotent across lost receipts and retries.
- Compose finite address rotation and all-edge scheduling under one query deadline.

## References

- [Nonblocking IPv4 TCP descriptor ownership](0175-nonblocking-ipv4-tcp-descriptor-ownership.md)
- [Deadline-bound grouped sufficient-state TCP client](0480-deadline-bound-grouped-sufficient-state-tcp-client.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
