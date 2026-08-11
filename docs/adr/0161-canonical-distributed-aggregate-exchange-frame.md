# ADR 0161: Canonical distributed aggregate exchange frame

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB query, networking, and distributed-systems maintainers
- **Extends:** [ADR 0070](0070-feature-pass-logical-boundaries.md)

## Context

The Phase 16 logical boundary already merges ungrouped aggregate state and provides bounded
in-memory handoff, but it assigns no bytes to a worker/coordinator exchange. Reusing native client
protocol payloads would conflate client request multiplexing with distributed query identity and
would still leave aggregate state layout implicit.

## Decision

Distributed Aggregate Exchange v1 is one canonical fixed 128-byte frame. It binds a nonnil query
UUID, nonnil tablet UUID, nonzero per-tablet sequence, terminal flag, and the exact binary64 bits of
count/sum/minimum/maximum/mean/M2 state. Explicit presence bits distinguish extrema from their
numeric values. Empty state has a single canonical representation using positive zero.

The frame carries magic, exact version and length, zero reserved bytes, and CRC32C over every byte
before the checksum. Exact decoding rejects both truncation and trailing bytes. Integrity is checked
before any field controls interpretation; checksum-valid unknown versions are classified as
unsupported. The in-memory bounded exchange and coordinator use the same aggregate-state
validation so callers cannot bypass wire invariants.

The encoder owns its fixed array. The decoder borrows input only for the duration of the call and
returns value-owned identities and state. Neither object allocates on the successful data path.

## Consequences and validation

The fixed layout is simple to frame under partial I/O and preserves floating-point state without
text conversion. It spends bytes on explicit identities and reserved evolution space. Sequence
ordering and duplicate policy remain coordinator/transport responsibilities; CRC32C is not a
security proof.

Focused tests freeze field order, endianness, flags, and a deterministic checksum; preserve all
aggregate bits on round trip; and reject truncation, trailing bytes, corruption, unknown versions,
nonzero reserved bytes, noncanonical absent extrema, and inconsistent in-memory messages. The
installed public target consumer references both codec symbols. General physical fragment,
grouping, ordering, cancellation, retry, and streaming carrier contracts remain Phase 16 work.

Invariants 3, 8, 10, 11, 13, 14, and 18 apply.

## Migration and rollback

No earlier aggregate exchange byte format exists. Peers negotiate exact version 1.0 out of band and
reject every other encoded version. Rollback may stop emitting these frames; it must not reinterpret
them as native client frames or silently accept a different aggregate layout.

## References

- [Distributed Aggregate Exchange v1](../formats/distributed-aggregate-exchange-v1.md)
- [Feature-pass logical boundaries](0070-feature-pass-logical-boundaries.md)
