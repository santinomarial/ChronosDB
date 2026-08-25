# ADR 0474: Proof-revalidated grouped sufficient-state worker v2

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB query, Manifest, and distributed-query maintainers
- **Extends:** [ADR 0384](0384-proof-revalidated-vector-aggregate-worker-v2.md),
  [ADR 0470](0470-canonical-multi-key-grouped-sufficient-state-exchange.md), and
  [ADR 0471](0471-shared-mergeable-grouped-state-owner.md)

## Context

Fragment v2 already froze a schema-neutral grouped intent, Grouped Exchange v1 could carry exact
multi-key/all-type sufficient state, and the shared grouped table could expose canonical worker
state. The real-CSEG vector worker still rejected grouped mode, so none of those pieces formed a
proof-gated worker result.

## Decision

`execute_distributed_vector_grouped_aggregate_fragment_v2` accepts only grouped Fragment-v2 plans.
It canonically validates the dispatch and reuses the row worker's exact node, placement, read
barrier, Manifest generation, database/table/tablet, destination schema, Raft source, applied
position, part-range, temporal winner-resolution, and event-time-filter gates before grouping.

The worker derives ordered key and aggregate definitions from the exact projected physical shapes
and proves the Fragment-v2 result descriptors at the same boundary. It materializes the complete
fragment projection, accumulates selected chunks in the shared `MergeableVectorGroupedAggregateTable`,
and synchronously borrows each first-seen group only while encoding an owned Grouped Exchange v1
frame. An empty tablet returns the distinct terminal-only frame. No aggregate is finalized.

The returned authority, projected shapes, grouped table configuration, query memory, group/key/
aggregate widths, variable payloads, frame count, individual frames, and total retained encoded
bytes are finite. Allocation, shape, ownership, group-cardinality, or encoded-byte failure publishes
no result. The worker is synchronous and thread-affine, and a loader must invoke its borrowed part
consumer exactly once.

This direct-input subset intentionally ignores worker-local ORDER BY and LIMIT. It does not execute
computed WHERE predicates, computed group keys, or computed aggregate arguments; those still use
the row-backed coordinator plan until an explicit physical-plan split is implemented. Transport,
retry scheduling, partition routing, and final grouped SQL projection/order/limit remain separate.

## Consequences

Real committed temporal CSEGs can now produce portable sufficient-state streams for direct
multi-key/all-type Fragment-v2 grouped intent without duplicating grouping semantics or rounding
AVG/variance/exact sums. Work is proportional to selected rows times grouped width plus frame
encoding. Encoded result bytes use an independent owner bound because they outlive query-accounted
table state.

No cross-thread state is shared, so no memory-ordering argument applies.

## Affected invariants

- [Invariant 5](../architecture/invariants.md) and
  [Invariant 6](../architecture/invariants.md): grouping sees only authority-proved committed/applied
  winners from one exact schema and snapshot boundary.
- [Invariant 10](../architecture/invariants.md) and
  [Invariant 14](../architecture/invariants.md): worker output is canonical, versioned, checksummed
  Grouped Exchange v1 rather than an in-memory struct dump.
- [Invariant 11](../architecture/invariants.md): the table owns query-accounted keys/states only
  while the encoder borrows them; the result owns complete frame bytes.
- [Invariant 15](../architecture/invariants.md): projection, configuration, table, query memory,
  message count, per-frame bytes, and total encoded bytes are bounded.
- [Invariant 18](../architecture/invariants.md): worker grouping reuses the local SQL table and
  aggregate kernels.

## Validation

The real-CSEG worker test proves Fragment-v2 authority binding, two first-seen groups, canonical
COUNT/SUM state frames, exact correlation and terminal positions, stale route rejection,
exactly-once loader enforcement, group-cardinality exhaustion, and total encoded-byte exhaustion.
A separate authority test covers variable and nullable multi-key shapes. Allocation injection
covers every empty-tablet binding, table, distinct-terminal, and result-frame allocation before
publication. Header self-containment, the complete query/allocation suites, focused sanitizers,
pinned clang-tidy, formatting, and whitespace checks are required by the milestone gate. The full
query suite passed 420 of 420 tests and the allocation-failure suite passed 61 of 61. Focused
worker, grouped exchange, table, and coordinator cases passed 11 of 11 under ASan/UBSan; the worker
allocation sweep passed there separately. The changed production target passed pinned clang-tidy
18, and formatting and whitespace checks passed.

## Migration and rollback

No durable or wire migration. Rollback removes the grouped worker entry points while retaining the
already frozen Fragment-v2 grouped intent, shared table, exchange frame, and coordinator.

## Unresolved questions

- Physical-plan splitting for computed pre-group WHERE/key/aggregate expressions.
- Compatible all-tablet grouped authority ownership and authenticated request/response transport.
- Partition/shuffle routing, skew handling, and final grouped SQL projection/ORDER BY/LIMIT.

## References

- [Distributed Vector Grouped Aggregate Exchange v1](../formats/distributed-vector-grouped-aggregate-exchange-v1.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
