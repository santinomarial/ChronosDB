# ADR 0471: Shared mergeable grouped-state owner

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB query and distributed-query maintainers
- **Extends:** [ADR 0042](0042-query-accounted-bounded-grouped-aggregates.md),
  [ADR 0380](0380-mergeable-all-type-vector-aggregate-state.md), and
  [ADR 0470](0470-canonical-multi-key-grouped-sufficient-state-exchange.md)

## Context

The local grouped operator already owned the canonical multi-key hash/equality table and all-type
mergeable states, but that owner was private to final row materialization. A distributed worker
could not emit the new grouped sufficient-state frame without duplicating grouping semantics or
prematurely finalizing AVG, variance, and exact sums.

## Decision

`MergeableVectorGroupedAggregateTable` is the single move-only, thread-affine group-state owner.
It retains the existing fixed-capacity query-accounted table, canonical type-framed hash, exact
collision comparison, signed-zero/NaN normalization, NULL grouping, first-seen order, group/key/
extremum limits, cancellation checks, and `MergeableVectorAggregateState` kernels.

Accumulation accepts only an `AccountedVectorChunk` belonging to the supplied query resource
context. After accumulation, callers can borrow exact key and sufficient-state spans by stable
group ordinal until the next mutating call, table move/destruction, or local materialization of that
group. The grouped exchange encoder has a synchronous borrowing overload so a worker can produce
canonical bytes without copying, moving, or releasing the table's reservations.

`GroupedAggregateOperator` now composes this table and remains the sole final-row materializer.
Therefore local SQL and future distributed worker partials share one grouping oracle. This change
does not yet merge decoded remote groups, split a physical plan at the grouped stage, or add
transport/partition routing.

## Consequences

Worker pushdown no longer requires a second hash/equality implementation. Borrowed spans cannot
cross another mutating table call, a table move/destruction, or local group materialization. All
dynamic state retains the same query credit and releases immediately on failure. There is no shared
publication and no new memory-ordering argument.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): local and distributed paths derive group identity
  and sufficient state from the same exact source shapes and kernels.
- [Invariant 11](../architecture/invariants.md): the table owns keys/states/reservations; exchange
  encoding only borrows them synchronously.
- [Invariant 15](../architecture/invariants.md): input chunks must be query-accounted and all prior
  group/cardinality/payload limits remain enforced.
- [Invariant 18](../architecture/invariants.md): distributed pushdown reuses, rather than weakens or
  reimplements, the proved local grouping semantics.

## Validation

A focused table test accumulates nullable variable keys and sufficient COUNT/SUM state, verifies
first-seen groups, encodes every group through the borrowing exchange boundary, decodes/finalizes
the state, and checks exact results plus out-of-range rejection. The unchanged local grouped suite
continues to cover all logical key types, signed zero/NaN, deliberate hash collisions, variable
extrema, limits, ownership, and differential properties. The full query suite passed 415 of 415
tests and the allocation-failure suite passed 58 of 58. Focused table, operator, and exchange cases
passed ASan/UBSan with leak detection disabled on Apple's runtime. Both changed production sources
passed the pinned clang-tidy 18 warning-as-error target; formatting and whitespace checks passed.

## Migration and rollback

No wire or durable migration. Rollback makes the table private to `GroupedAggregateOperator` again
and removes the borrowing encoder while retaining Grouped Exchange v1 bytes.

## Unresolved questions

- Exact worker-side pre-group physical-plan splitting and terminal stream construction.
- Deterministic coordinator merge of decoded equal-key states without losing table accounting.
- Partition/shuffle authority and authenticated transport ownership.

## References

- [Bounded grouped aggregates](../learning/bounded-grouped-aggregates.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Distributed Vector Grouped Aggregate Exchange v1](../formats/distributed-vector-grouped-aggregate-exchange-v1.md)
