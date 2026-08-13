# Distributed Vector Aggregate Exchange v1

> **Status: accepted and implemented for ungrouped aggregates.** This is a correlated envelope
> around exactly one [Mergeable Vector Aggregate State v1](mergeable-vector-aggregate-state-v1.md).
> Group keys require a distinct future protocol.

All integers are unsigned little-endian. UUIDs use network-order bytes. Reserved bytes are zero.
The maximum frame length is 1,048,792 bytes; an enclosing transport may impose a lower bound.

## Layout

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVAEX1` |
| 8 | 2 | major | `1` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `96` |
| 16 | 8 | frame length | Exactly `96 + state length + 4` |
| 24 | 16 | query ID | Nonzero |
| 40 | 16 | tablet ID | Nonzero |
| 56 | 8 | sequence | Exactly `aggregate ordinal + 1` |
| 64 | 4 | aggregate ordinal | Zero-based and below aggregate count |
| 68 | 4 | aggregate count | Exact fragment-bound definition count; `1..4,096` |
| 72 | 4 | flags | Bit 0 is `TERMINAL`; every other bit is zero |
| 76 | 4 | state length | Exact nested state length |
| 80 | 4 | state CRC32C | CRC32C of the exact nested state bytes |
| 84 | 4 | header CRC32C | CRC32C of bytes `[0,84)` |
| 88 | 8 | reserved | Zero |
| 96 | variable | state | Exact Mergeable Vector Aggregate State v1 |
| final - 4 | 4 | frame CRC32C | CRC32C of every preceding frame byte |

`TERMINAL` is set exactly when `aggregate ordinal + 1 == aggregate count`. Thus an ungrouped
tablet produces one canonical position for each aggregate definition; no empty-state or
terminal-only surrogate is needed because every aggregate has a sufficient empty state.

## Schema and fragment binding

The frame is never decoded against a caller-inferred type. The caller supplies the complete,
ordered `VectorAggregateDefinition` vector derived from the admitted Fragment v2 plan, projected
input shapes, and result schema. The aggregate count must equal that vector's width, and the nested
state definition must exactly equal the definition at the encoded ordinal. SQL output names remain
in the Fragment v2 result schema and are not repeated here.

The implemented binder accepts only `UNGROUPED_AGGREGATE`. It validates the plan and result schema,
then derives each operation, input ordinal, logical type, and nullability. A grouped plan fails
closed: optional group material cannot be added to this frame without a new versioned contract.

## Validation and partial I/O

Readers validate caller limits and definitions, magic, and header integrity before trusting any
length. They then enforce hard and caller frame/state bounds, canonical count/ordinal/sequence/
terminal relations, reserved bytes, exact identity, and exact framing before retaining a complete
frame. Complete-frame and nested-state integrity pass before nested state decode or variable
extremum allocation. Unknown versions are unsupported; malformed wire bytes are corruption;
invalid local contracts are invalid arguments; lower deployment bounds are resource exhaustion.

The nonmovable reader owns one bounded fragmented frame, consumes no bytes from a coalesced
successor, and makes any failure sticky. The move-only write cursor owns canonical encoded bytes,
rejects over-advance, and leaves the moved-from cursor complete. Nested variable extrema reserve
credit from the supplied query resource context before copying and release it with the decoded
state.

This codec validates one frame's canonical position. Cross-frame identity continuity, exact retry
arbitration, all-tablet closure, state merge, final ORDER BY/LIMIT, transport authentication, and
authority rebinding belong to enclosing owners and are not inferred here.

