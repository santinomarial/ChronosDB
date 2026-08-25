# Distributed Vector Grouped Aggregate Exchange v1

> **Status: accepted and implemented for exact encoding, decoding, and bounded partial-I/O
> ownership.** Worker plan execution, cross-tablet merging, authenticated query transport, and
> partitioned shuffle remain separate owners.

This distinct frame binds one multi-column group key tuple and zero or more
[Mergeable Vector Aggregate State v1](mergeable-vector-aggregate-state-v1.md) values to one query,
tablet, and canonical tablet-local group position. It is not compatible with the legacy nullable-
FLOAT64 grouped protocol or the ungrouped aggregate envelope. All integers are unsigned little-
endian, UUIDs use canonical network-order bytes, and reserved bytes are zero.

The hard maximum frame is 64 MiB. One group's combined canonical key payload is at most 1 MiB.
Key, aggregate, and group counts are each at most 4,096; deployments may impose lower limits.

## Outer layout

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVGEX1` |
| 8 | 2 | major | `1` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `128` |
| 16 | 8 | frame length | Exactly header + both sections + four-byte trailer |
| 24 | 16 | query ID | Nonzero |
| 40 | 16 | tablet ID | Nonzero |
| 56 | 8 | sequence | `group ordinal + 1`, or `1` for an empty terminal |
| 64 | 4 | group ordinal | Zero-based, or zero for an empty terminal |
| 68 | 4 | group count | `1..4,096`, or zero for an empty terminal |
| 72 | 4 | key count | Exact fragment key width, or zero for an empty terminal |
| 76 | 4 | aggregate count | Exact fragment aggregate width, or zero for an empty terminal |
| 80 | 4 | flags | Bit 0 `TERMINAL`, bit 1 `EMPTY`; all other bits zero |
| 84 | 4 | key-section length | Exact bytes, including key-entry headers |
| 88 | 4 | state-section length | Exact bytes, including state-entry headers |
| 92 | 4 | payload CRC32C | Both complete sections |
| 96 | 4 | header CRC32C | Bytes `[0,96)` |
| 100 | 28 | reserved | Zero |
| 128 | variable | key section | Exact key-count entries |
| following | variable | state section | Exact aggregate-count entries |
| final - 4 | 4 | frame CRC32C | Every preceding frame byte |

A nonempty tablet emits groups in its deterministic local first-seen order. Sequence and ordinal
are canonical, and `TERMINAL` is set exactly on the last group. SQL does not infer result ordering
from this position. A tablet with no groups emits one distinct `TERMINAL|EMPTY` frame with sequence
one, zero group/count/section fields, and no fabricated NULL key or aggregate state.

## Key entries

Each key entry precedes its canonical scalar payload:

| Relative offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 2 | logical-type code | Exact fragment key type |
| 2 | 2 | type parameter 0 | Exact fragment parameter |
| 4 | 2 | type parameter 1 | Exact fragment parameter |
| 6 | 2 | flags | Bit 0 declared nullable, bit 1 value is NULL |
| 8 | 4 | payload length | Exact canonical scalar length; zero for NULL |
| 12 | 4 | payload CRC32C | Exact scalar bytes; CRC32C of empty bytes for NULL |
| 16 | variable | payload | Existing canonical scalar representation |

The caller supplies the ordered `VectorGroupKeyDefinition` vector authorized by the admitted
fragment. Column ordinals are unique and bounded; type, parameters, declared nullability, count,
and order must match exactly. A NULL value is valid only for a nullable definition. STRING and
SYMBOL bytes remain distinct typed UTF-8 values; BINARY is opaque; Boolean and every fixed-width
type retain their canonical scalar bytes.

## State entries

Each aggregate entry is:

| Relative offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 4 | aggregate ordinal | Exact zero-based entry position |
| 4 | 4 | state length | Exact nested frame length |
| 8 | 4 | state CRC32C | Exact nested bytes |
| 12 | 4 | reserved | Zero |
| 16 | variable | state | Mergeable Vector Aggregate State v1 |

The supplied ordered aggregate-definition vector may be empty for key-only grouping. Otherwise,
every decoded nested definition must equal the fragment definition at its ordinal, including input
ordinal, type parameters, and nullability. AVG, variance, exact SUM, and extrema therefore remain
sufficient states rather than prematurely finalized cells.

## Validation and ownership

The decoder validates caller limits and fragment authority first. Magic and header integrity pass
before any frame-length allocation. Exact framing, canonical position/flags/counts, hard and caller
limits, reserved bytes, complete-frame CRC, and payload CRC pass before key or nested-state
allocation. Per-key and per-state CRCs then pass before canonical scalar or nested-state decode.
Checksum-valid unknown versions are unsupported; damaged/noncanonical bytes are corruption; lower
deployment bounds are resource exhaustion. After decode, the shared query-accounted grouped table
can synchronously copy the key, merge the sufficient states, and release the message. The enclosing
coordinator must supply one canonical accepted-tablet/group order because floating sufficient-state
merge order is observable.

Decoded key containers and payloads retain one conservative query-memory reservation. Nested
variable extrema retain their independent existing reservations. Failure exposes no partial group
and releases every reservation. The header-first reader owns one exact frame, reports the consumed
prefix, leaves a coalesced successor caller-owned, and fails sticky. The move-only cursor exposes
only its unwritten suffix, rejects over-advance without progress, and makes its moved-from source
complete.

CRC32C provides accidental-damage detection, not authentication. An enclosing mutually
authenticated transport must bind the exact fragment and peer identities. Stream retry,
all-tablet closure, duplicate arbitration, canonical merge ordering, skew bounds, final projection,
ORDER BY, and LIMIT remain enclosing responsibilities.
