# ADR 0472: Query-accounted grouped partial-state merge

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB query and distributed-query maintainers
- **Extends:** [ADR 0380](0380-mergeable-all-type-vector-aggregate-state.md),
  [ADR 0470](0470-canonical-multi-key-grouped-sufficient-state-exchange.md), and
  [ADR 0471](0471-shared-mergeable-grouped-state-owner.md)

## Context

Grouped Exchange v1 could carry exact multi-key sufficient states and the shared grouped table
could produce them, but a coordinator still had no proved path for inserting decoded keys,
coalescing equal groups, merging sufficient states, or materializing the final rows. Implementing a
second coordinator hash table would create a separate grouping oracle and risk differences for
NULL, signed zero, NaN, decimal parameters, and variable-width values.

## Decision

`MergeableVectorGroupedAggregateTable::merge_group` synchronously borrows one exact scalar key tuple
and one sufficient state per admitted aggregate. It validates width, type, nullability, and exact
aggregate definitions before mutation. The table hashes scalar keys with the same type framing,
canonical bytes, signed-zero normalization, and NaN normalization as physical input rows, then uses
the same exact total comparison to resolve collisions. New groups retain copied keys and empty
destination states under the table's existing limits; every source state merges through
`MergeableVectorAggregateState::merge`.

The first storage-producing call binds the table's retained reservations to one
`QueryResourceContext`. Later accumulation, merge, and materialization must use that context.
Allocation or state-merge failure destroys the complete table before returning, so a partially
merged group is neither observable nor retryable. `materialize_group` starts a terminal output
phase, requires first-seen ordinal order, and rejects later accumulation or merge.

Merge order remains caller order. An enclosing grouped stream coordinator must therefore arbitrate
tablet attempts and feed accepted tablets/groups in one canonical order before output. This ADR
does not claim that all-tablet stream owner, retry protocol, authenticated transport, or partition
routing.

## Consequences

Local rows, worker partials, and coordinator partials now share one group-identity and aggregate-
state implementation. Coordinator merge remains bounded by the existing maximum group count, key
payload, aggregate width, variable extremum, retained configuration, and query-wide memory limits.
Decoded source messages may be released immediately after the synchronous merge. There is no
shared publication and no new memory-ordering argument; the table remains uniquely owned and
thread-affine.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): remote partial definitions and key shapes must match
  the admitted fragment exactly before mutation.
- [Invariant 11](../architecture/invariants.md): borrowed source spans never outlive the merge call;
  the table owns every retained copy and reservation.
- [Invariant 15](../architecture/invariants.md): all group, payload, extremum, and query-memory bounds
  apply during coordinator merge, and failure exposes no partial state.
- [Invariant 18](../architecture/invariants.md): physical-row and remote-scalar group identity share
  one implementation and one all-type differential test boundary.

## Validation

Cross-tablet tests coalesce equal variable keys, preserve deterministic caller-defined first-seen
order, merge COUNT/SUM/AVG sufficient states, enforce output-phase sealing, and finalize exact
rows. Physical/scalar differential cases cover every frozen logical type and decimal parameters,
plus signed-zero and NaN normalization. Allocation injection covers variable key copies and
variable extrema, proving that every failure discards the table and releases new query credit. The
full query suite passed 417 of 417 tests, the allocation-failure suite passed 60 of 60, and focused
table/operator cases passed ASan/UBSan with leak detection disabled on Apple's runtime. The changed
production target passed pinned clang-tidy 18; formatting and whitespace checks passed.

## Migration and rollback

No wire or durable migration. Rollback removes the merge/materialization surface while retaining
the shared worker table and Grouped Exchange v1 bytes.

## Unresolved questions

- Canonical all-tablet attempt arbitration, duplicate suppression, and closure ownership.
- Worker-side pre-group plan splitting and terminal stream construction.
- Authenticated transport, partition/shuffle authority, and bounded skew policy.

## References

- [Bounded grouped aggregates](../learning/bounded-grouped-aggregates.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Distributed Vector Grouped Aggregate Exchange v1](../formats/distributed-vector-grouped-aggregate-exchange-v1.md)
