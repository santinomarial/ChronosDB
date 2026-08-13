# ADR 0381: Canonical mergeable vector aggregate state bytes

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and distributed-query maintainers
- **Extends:** [ADR 0380](0380-mergeable-all-type-vector-aggregate-state.md)

## Context

The shared in-memory vector aggregate kernel can merge every supported operation, but private C++
fields are neither portable bytes nor an authenticated exchange contract. Final result cells remain
insufficient for AVG, variance, and exact sums. A decoder also cannot allocate a peer-declared
variable extremum before integrity and hard limits are proven.

## Decision

Mergeable Vector Aggregate State v1 is one versioned variable-length nested frame. A fixed
integrity-protected 112-byte header carries the exact operation/input definition and operation-
specific sufficient state. Only MIN/MAX may append one canonical scalar payload. A final CRC32C
covers the complete frame.

Unused fields have one positive-zero representation. Empty floating states are positive zero;
nonempty states preserve produced IEEE bits. Exact sums retain all eight 32-bit signed-magnitude
limbs and reject negative zero. Fixed extrema use their canonical physical widths; text is validated
UTF-8 and binary remains opaque. DECIMAL parameters and coefficients remain exact.

The decoder takes caller frame and variable-extremum limits plus one query resource context.
Header integrity and allocation-driving lengths pass before frame retention; complete integrity
passes before variable payload allocation. Variable decode reserves query credit before copying and
installs state only after validation succeeds.

The nonmovable reader owns one fragmented frame and leaves coalesced successors caller-owned. The
move-only cursor owns one encoded frame and makes moved-from progress terminal. Exact codec,
partial-I/O, and installed-consumer APIs live in the query library beside the aggregate kernel.

This frame is intentionally not a complete cross-service message. It carries no query/tablet,
group-key, sequence, terminal, schema-authority, or authentication fields. The next distinct
exchange contract must provide those values and must not send this nested frame on its own.

## Consequences and validation

Workers and coordinators now have one portable representation for every mergeable local state
without exposing implementation object bytes or prematurely finalizing values. The maximum frame
is 1,048,692 bytes and readers may enforce lower deployment bounds. Encoding is O(payload), exact
decode is O(frame), and retained variable memory is explicitly query-accounted. Each object is
thread-affine, so no inter-thread memory-ordering argument applies.

Focused tests round-trip every sufficient numeric state and MIN/MAX for all 18 logical types,
re-encode decoded values byte-for-byte, freeze the header, reject canonical damage and lower
limits, enumerate every two-part read split plus coalesced suffixes, prove sticky failure and cursor
move/over-advance behavior, and inject every owned allocation failure while checking credit release.
A dedicated deterministic libFuzzer target exercises exact and fragmented decoding.

The enclosing schema-bound aggregate exchange, grouped-key values, worker/coordinator execution,
global ordering/finalization, authority rebinding, and process integration remain separate tasks.
No Phase 16 exit gate is claimed.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Mergeable Vector Aggregate State v1](../formats/mergeable-vector-aggregate-state-v1.md)
- [Mergeable all-type vector aggregate state](0380-mergeable-all-type-vector-aggregate-state.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Architecture invariants](../architecture/invariants.md)
