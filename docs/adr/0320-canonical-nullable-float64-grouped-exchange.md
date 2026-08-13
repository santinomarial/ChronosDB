# ADR 0320: Canonical nullable-FLOAT64 grouped exchange

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, networking, and distributed-systems maintainers
- **Extends:** [ADR 0050](0050-canonical-query-accounted-group-hashing.md),
  [ADR 0161](0161-canonical-distributed-aggregate-exchange-frame.md)

## Context

Distributed Aggregate Exchange v1 deliberately froze only one ungrouped aggregate state. The local
vector engine already has exact nullable FLOAT64 group equality, including signed-zero and NaN
canonicalization, but no network format could carry even one grouped partial without inventing
peer-specific bytes or altering the ungrouped frame.

## Decision

Distributed Grouped FLOAT64 Aggregate Exchange v1 is a distinct fixed 136-byte frame. It binds
query, tablet, nonzero per-tablet sequence, terminal state, one nullable FLOAT64 group key, and the
existing count/sum/minimum/maximum/mean/M2 state. Magic, exact version/length, zero reserved bytes,
and a final CRC32C make framing and corruption classification independent of the ungrouped protocol.

SQL NULL uses an absent-key flag and positive-zero payload. Present signed zeros encode as positive
zero. Every present NaN sign and payload encodes as quiet-NaN bits `0x7ff8000000000000`; all other
FLOAT64 bits are preserved. Exact decoding rejects noncanonical negative-zero, NaN, and absent-key
payloads even when the checksum is valid. Aggregate-state canonicalization remains identical to the
ungrouped frame.

The encoder owns its fixed array. Exact decoding borrows bytes only for the call and returns owned
identities and values. Neither successful path allocates. This slice defines only the first
grouping-state codec: it does not define fragment plan fields, multiple or non-FLOAT64 keys,
coordinator grouping/order, stream carriers, top-N, or LIMIT.

## Consequences and validation

Peers can now exchange a single grouped partial without weakening local FLOAT64 grouping equality
or changing deployed ungrouped v1 bytes. Separate magic prevents either frame from being silently
reinterpreted as the other.

Focused tests freeze every field and flag, verify checksum coverage, canonicalize negative zero and
distinct NaN bits, round-trip NULL, and reject truncation, trailing bytes, damage, unknown versions,
noncanonical key payloads, reserved bytes, and invalid aggregate state. The installed-consumer gate
covers both codec symbols.

General grouped planning/execution, multi-key and variable-width formats, grouped coordination,
ordering/top-N/LIMIT, and broader differential/fault evidence remain incomplete. No Phase 16 exit
gate is claimed.

Invariants 6, 10, 11, 14, and 18 apply.

## References

- [Distributed Grouped FLOAT64 Aggregate Exchange
  v1](../formats/distributed-grouped-float64-exchange-v1.md)
- [Canonical query-accounted group hashing](0050-canonical-query-accounted-group-hashing.md)
- [Canonical distributed aggregate exchange frame](0161-canonical-distributed-aggregate-exchange-frame.md)
