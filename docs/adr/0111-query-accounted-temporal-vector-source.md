# ADR 0111: Query-accounted temporal vector source

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB query and temporal-history maintainers

## Context

Temporal mutable-state and CSEG v2 resolution intentionally return the scalar reference model so
winner semantics remain auditable. The vector engine could not consume that result without a
canonical physical adapter. Returning borrowed scalar storage would couple chunk lifetime to maps
and strings owned elsewhere, while constructing one vector per row would violate bounded execution
and create allocation-heavy data paths.

The adapter must preserve schema order, row order, logical types, nullable validity, packed BOOL,
little-endian fixed-width values, variable offsets, decimal/UUID bytes, cancellation, and query-wide
memory credit. A resolved tombstone is already absent from `ScalarTableSnapshot`; the adapter must
not reinterpret temporal winner rules.

## Accepted decision

Add `ScalarSnapshotScanOperator`, a pull-based physical source over one immutable owned
`ScalarTableSnapshot`. Construction retains the shared snapshot. Each pull:

1. checks cancellation;
2. chooses at most the configured rows and `VectorChunk` row limit;
3. precomputes canonical buffer sizes and a conservative retained-memory charge;
4. reserves query memory before materializing retained buffers;
5. copies schema-order scalar values into canonical physical columns without per-row allocation;
6. returns one identity-selected `AccountedVectorChunk`.

Every emitted chunk owns its columns, so it outlives the source and scalar snapshot. Empty input
emits sticky end without an empty chunk. A failed reservation does not advance the cursor and may be
retried with a different query context. Cancellation is checked before planning and between column
materializations. All schema logical types use the same canonical representation accepted by
`OwnedPhysicalColumn`; nullable fixed slots and variable ranges remain zero/empty.

This is a scalar-winner-to-vector bridge, not a second row-version resolver. Direct SIMD/vector
winner selection over mutable heads/CSEG histories remains an optimization that must compare
against the scalar oracle.

## Consequences

- Current or `FOR SYSTEM_TIME AS OF` snapshots can enter the existing vector operator pipeline.
- Temporal semantics remain centralized in the scalar provider/CSEG resolver.
- Output copies values once and charges owned buffers conservatively; no borrowed lifetime or
  per-row allocation is introduced.
- Direct physical-plan lowering from a temporal SQL scan remains separate orchestration work.

## Alternatives considered

- **Borrow scalar storage:** strings, binary identities, maps, and row vectors do not form canonical
  physical columns and would create fragile cross-owner lifetimes.
- **Resolve winners directly in the adapter:** duplicates temporal semantics and weakens the scalar
  differential boundary.
- **One chunk per row:** bounded but creates excessive allocation and scheduling overhead.
- **Unaccounted conversion before query admission:** permits a large history result to evade the
  query memory limit.

## Affected invariants and validation

Invariants 6, 7, 11, 13, and 18 apply. Focused tests resolve correction/tombstone history, emit
one-row chunk boundaries, preserve row and column order, materialize nullable variable values,
retain sticky end, reject insufficient query credit without cursor advancement, and honor
cancellation. Full logical-type matrices, allocation faults, scalar/vector differential SQL,
direct lowering, and scale measurement remain Phase 18 work.
