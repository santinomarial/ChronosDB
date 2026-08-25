# ADR 0505: Correlated grouped shuffle success acknowledgment

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB cluster, distributed-query, and protocol maintainers
- **Extends:** [ADR 0503](0503-authority-bound-grouped-shuffle-frame.md) and
  [ADR 0504](0504-atomic-authorized-grouped-shuffle-streams.md)

## Context

Completing a local TLS write does not prove that the destination validated or retained the whole
stream. Treating EOF as success would make truncation indistinguishable from acceptance, while a
generic status frame could be replayed across query edges. Finite retry needs one exact positive
receipt without exposing receiver failure as partial success.

## Decision

Adopt fixed `CHDVGAK1` 1.0 bytes as specified by
[Distributed Vector Grouped Aggregate Shuffle Acknowledgment v1](../formats/distributed-vector-grouped-aggregate-shuffle-ack-v1.md).
The receipt reverses the original edge route and binds query, source tablet, destination partition,
partition count, hash version, accepted frame count, and accepted outer byte count. Header and full
frame CRC32C cover every interpreted field. Encoding and decoding require the same immutable
whole-query shuffle authority.

The acknowledgment represents success only and may be constructed only after exact-once extraction
of a complete authorized stream. Failure closes the attempt; it does not emit a receipt. A sender
must require the accepted count and bytes to equal its immutable attempt before publishing success.
The fixed reader and move-only write cursor are single-thread-affine and bounded. This decision does
not define TLS, retries, duplicate installation, reducer admission, or durable query state.

## Detailed rationale

An explicit positive receipt separates kernel/TLS write completion from application acceptance.
Carrying both logical edge identity and physical attempt extent rejects stale or cross-edge success
without placing a content digest or mutable attempt number into canonical partition data.

## Alternatives considered

- **Treat successful write or clean EOF as acceptance.** Rejected because the receiver may fail
  after the sender's final write or before validating terminal closure.
- **Echo the terminal data frame.** Rejected because direction and meaning would be ambiguous and a
  data frame does not bind complete-stream extent.
- **Return detailed failure payloads.** Deferred; connection failure already drives retry, while
  this first receipt has one unambiguous success meaning.

## Consequences

Finite retry and TLS owners now have a canonical success boundary. Every accepted stream adds 132
response bytes. An acknowledgment lost after acceptance may cause a byte-identical retry; the
future reducer/attempt owner must make that duplicate idempotent rather than infer success.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): success names one immutable query edge and complete
  extent.
- [Invariant 10](../architecture/invariants.md): every acknowledgment interpretation field is
  covered by header and complete-frame CRC32C.
- [Invariant 14](../architecture/invariants.md): the receipt has explicit 1.0 compatibility and
  rejects unknown versions.
- [Invariant 15](../architecture/invariants.md): receipt size is fixed and accepted count/bytes are
  bounded.
- [Invariant 18](../architecture/invariants.md): acknowledgment does not weaken all-or-none stream
  validation or make transport write completion equivalent to acceptance.

## Validation plan

Focused tests cover exact reverse-route round trip, every fragmented read split, seven-byte writes,
moved-from completion, invalid extent, damaged authority fields, and checksum-valid unknown version.
Allocation injection covers encoding and cursor construction. The warning-as-error build, 276
cluster tests, 43 cluster allocation-failure tests, and focused ASan/UBSan cases pass. Changed-source
clang-tidy reaches only the known LLVM 18/macOS 26 libc++ builtin incompatibility after no remaining
ChronosDB-source diagnostic.

## Migration or rollback considerations

This is additive pre-alpha network state with no durable bytes. Rollback removes acknowledgment use
and leaves stream/frame owners intact, but a transport must then remain unavailable rather than
treating write completion or EOF as success.

## Unresolved questions

- Compose the receipt with mutual-TLS client/server and exact destination-principal authorization.
- Add byte-identical finite retry and idempotent duplicate receiver/reducer admission.
- Decide whether measured multiplexing needs an attempt epoch in a future negotiated version.

## References

- [Distributed Vector Grouped Aggregate Shuffle Acknowledgment v1](../formats/distributed-vector-grouped-aggregate-shuffle-ack-v1.md)
- [Distributed Vector Grouped Aggregate Shuffle Frame v1](../formats/distributed-vector-grouped-aggregate-shuffle-frame-v1.md)
- [Implementation roadmap](../roadmap.md)
