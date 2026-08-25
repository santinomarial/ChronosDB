# ADR 0470: Canonical multi-key grouped sufficient-state exchange

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB query, distributed-query, cluster, and Native Protocol maintainers
- **Extends:** [ADR 0380](0380-mergeable-all-type-vector-aggregate-state.md),
  [ADR 0381](0381-canonical-mergeable-vector-aggregate-state-bytes.md), and
  [ADR 0459](0459-bounded-row-backed-distributed-grouped-sql.md)

## Context

The scalable grouped path needs to move key-partitioned sufficient state rather than every source
row. Existing portable aggregate-state bytes carry no key or stream identity. The earlier grouped
protocol carries only one nullable FLOAT64 key and a reduced aggregate vocabulary; extending it
would make old bytes ambiguous and would not cover the local multi-key/all-type SQL contract.

## Decision

Distributed Vector Grouped Aggregate Exchange v1 is a distinct checksummed frame. One nonempty
message binds an exact query/tablet identity, canonical tablet-local group ordinal/count/sequence,
the complete ordered canonical scalar key tuple, and one Mergeable Vector Aggregate State v1 per
fragment-authorized aggregate. Key-only grouping permits zero aggregate states. An empty tablet has
one distinct terminal-only frame and cannot fabricate a NULL-key group.

Encode and decode require the complete ordered key and aggregate definition vectors. Key column
ordinals are unique; type parameters and nullability match each value. Nested definitions match
their exact aggregate ordinal. A 64 MiB frame bound, 1 MiB combined key-payload bound, 4,096-entry
width/count bounds, and lower caller limits apply before allocation.

Header and complete integrity pass before allocation-driving fields or payloads are trusted.
Decoded key/container bytes reserve query credit; variable aggregate extrema retain the existing
independent credit. A header-first reader owns one exact fragmented frame and leaves coalesced bytes
with the caller. A move-only write cursor owns short-write progress.

This decision freezes group/state bytes only. It does not yet expose the local grouped hash table,
execute preparation/group accumulation on workers, merge equal tuples at the coordinator, add an
authenticated transport envelope, define hash partition destinations, or replace the row-backed
differential path.

## Consequences

Every current key type and sufficient aggregate state now has one portable correlated group frame.
AVG/variance/exact sums are not rounded before global merge. The cost is one outer frame per local
group plus nested-state framing and CRC work. Floating aggregate merge order remains a later
coordinator decision and must be deterministic.

The codec is thread-affine and publishes no shared state, so no inter-thread memory-ordering
argument applies.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): exact query/tablet/key/state identity is retained for
  later all-tablet merge under one authority-covered attempt.
- [Invariant 10](../architecture/invariants.md) and
  [Invariant 14](../architecture/invariants.md): all interpretation and nested payload bytes are
  versioned and integrity-covered; legacy grouped and ungrouped formats are unchanged.
- [Invariant 11](../architecture/invariants.md): frame, key, nested state, partial-read, and
  short-write ownership is explicit and move-safe.
- [Invariant 15](../architecture/invariants.md): frame, key payload, group/key/aggregate count,
  nested state, and query-memory influence are bounded.
- [Invariant 18](../architecture/invariants.md): the frame reuses canonical scalar and the single
  all-type mergeable state kernel rather than introducing reduced semantics.

## Validation

Focused tests freeze the outer layout and CRC fields, round-trip a variable STRING plus nullable
Boolean key with multiple sufficient states, prove the distinct empty terminal, reject identity,
shape, sequence, duplicate-key, truncation, checksum, and lower-limit failures, enumerate every
fragmented-read split with a coalesced suffix, and prove sticky-reader and move/over-advance cursor
behavior. Allocation injection covers encode, exact decode, and reader ownership while checking
query-credit release. The full query suite passed 414 of 414 tests and its allocation-failure suite
passed 58 of 58 tests. Focused normal and allocation cases passed under ASan/UBSan with leak
detection disabled because Apple's sanitizer runtime does not provide LeakSanitizer. The new
production source passed the repository-pinned clang-tidy 18 warning-as-error gate; all changed C++
passed clang-format 18 and whitespace review.

## Migration and rollback

This additive pre-alpha format has no durable migration. Rollback removes its producer/consumer
registration while retaining the row-backed grouped path and unchanged legacy protocol bytes.

## Unresolved questions

- How the worker exposes the existing query-accounted grouped table without creating a second key
  equality/hash implementation.
- Whether the first production shuffle routes groups directly to hash partitions or returns local
  groups to one coordinator before partition fan-out.
- Exact stream retry/duplicate arbitration and authenticated request/response framing.

## References

- [Distributed Vector Grouped Aggregate Exchange v1](../formats/distributed-vector-grouped-aggregate-exchange-v1.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Bounded grouped aggregates](../learning/bounded-grouped-aggregates.md)
- [Implementation roadmap](../roadmap.md)
