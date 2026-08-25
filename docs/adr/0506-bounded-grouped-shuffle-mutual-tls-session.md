# ADR 0506: Bounded grouped shuffle mutual-TLS session

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB cluster, distributed-query, networking, and security maintainers
- **Extends:** [ADR 0504](0504-atomic-authorized-grouped-shuffle-streams.md) and
  [ADR 0505](0505-correlated-grouped-shuffle-success-acknowledgment.md)

## Context

Complete authorized stream and success-receipt owners existed without a connected transport.
Socket write completion could not replace the receipt, and accepting application bytes before
certificate authentication would let an unauthenticated peer consume decode/allocation work.
Existing grouped query TLS carriers bind Fragment-v2 request/response authority, not the distinct
source-push shuffle edge, stream extent, and reverse receipt.

## Decision

Add move-only, single-thread-affine client and server owners around one already connected
nonblocking `TlsSocket`. Each readiness call performs at most one TLS handshake, read, or write.
Positive handshake and exchange deadlines use caller monotonic time, expire exactly, and make every
terminal failure sticky.

Both sides finish TLS and authenticate the peer certificate fingerprint before application I/O.
The client additionally authorizes the server principal for the immutable destination node before
exposing the first stream byte. The server creates the atomic stream receiver only after client
authentication; that receiver authorizes the claimed source node and exact local destination.

The client transfers one already validated self-contained stream sender and does not complete when
its final byte is written. It reads exactly one `CHDVGAK1` receipt and requires the original edge,
accepted frame count, and accepted byte count to equal its immutable attempt. The server privately
retains the complete decoded stream, constructs the receipt only after terminal extraction, and
exposes the stream only after the entire receipt is written. TLS failure clears every server-side
unacknowledged prefix/result.

Both owners use fixed 16-KiB I/O scratch arrays. Frame, stream, nested decode, and deadline limits
remain independently finite. TLS contexts, descriptors, authority, authenticators, and authorizers
are embedding-owned and outlive the session. TCP connection/listener ownership, address rotation,
retry/backoff, cancellation, duplicate reducer admission, and packaged scheduling remain separate.

## Detailed rationale

Composing the accepted stream and receipt owners keeps TLS responsible only for authentication,
readiness, deadlines, and exact byte progression. Requiring the reverse receipt preserves the
distinction between local transport progress and destination application acceptance. One operation
per readiness call keeps the owner compatible with the existing nonblocking poll architecture.

## Alternatives considered

- **Reuse the Fragment-v2 grouped query TLS carrier.** Rejected because that protocol is request/
  response and cannot authorize destination partitions or acknowledge source-pushed stream extent.
- **Complete after the final TLS write.** Rejected because the destination may still reject decode,
  authority, terminal, or resource limits.
- **Authenticate after reading the first frame.** Rejected because certificate verification must
  precede application reads; source-node authorization still follows exact header decode.
- **Perform blocking TLS I/O.** Rejected because it would violate the existing reactor ownership
  and bounded-progress model.

## Consequences

One remote source-partition stream now has a complete authenticated connected lifecycle and exact
positive acceptance boundary. A lost receipt can still cause a byte-identical retry after the
server accepted a stream, so the future reducer admission owner must arbitrate duplicates
idempotently. This increment does not yet make the carrier process-addressable or retrying.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): immutable query/edge/extent authority survives the
  complete connected attempt.
- [Invariant 10](../architecture/invariants.md): TLS does not bypass outer, nested, or receipt
  integrity validation.
- [Invariant 11](../architecture/invariants.md): sockets, borrowed security dependencies, private
  prefixes, receipt cursor, and extracted stream lifetimes are explicit.
- [Invariant 14](../architecture/invariants.md): TLS carries the unchanged independently versioned
  `CHDVGSF1`, `CHDVGEX1`, and `CHDVGAK1` formats.
- [Invariant 15](../architecture/invariants.md): authentication precedes application I/O and all
  time, frame, byte, key, group, and state influence is bounded.
- [Invariant 18](../architecture/invariants.md): transport completion cannot weaken exact stream or
  receipt acceptance semantics.

## Validation plan

Real nonblocking socket-pair tests use maintained certificates to prove both fingerprints,
destination authorization before stream writes, source authorization during receive, complete
two-group delivery, exact receipt-gated client completion, server result extraction, rejected
destination principal, and exact deadline expiry. Allocation injection covers client and server
owner construction. The warning-as-error build, 278 cluster tests, 44 cluster allocation-failure
tests, and focused ASan/UBSan cases pass. Changed-source clang-tidy reaches only the known LLVM 18/
macOS 26 libc++ builtin incompatibility after no remaining ChronosDB-source diagnostic.

## Migration or rollback considerations

No wire bytes change. Rollback removes the connected owner while leaving stream and receipt codecs
available; remote shuffle must then remain unavailable rather than fall back to plaintext or write-
completion success.

## Unresolved questions

- Add deadline-bound nonblocking TCP connection and bounded listener ownership.
- Add finite immutable-route retries with byte-identical attempt reconstruction.
- Make destination reducer admission idempotent across a lost receipt and retry.

## References

- [Distributed Vector Grouped Aggregate Shuffle Frame v1](../formats/distributed-vector-grouped-aggregate-shuffle-frame-v1.md)
- [Distributed Vector Grouped Aggregate Shuffle Acknowledgment v1](../formats/distributed-vector-grouped-aggregate-shuffle-ack-v1.md)
- [Network security boundary](../learning/network-security-boundary.md)
- [Implementation roadmap](../roadmap.md)
