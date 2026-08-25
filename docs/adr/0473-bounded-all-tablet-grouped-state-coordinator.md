# ADR 0473: Bounded all-tablet grouped-state coordinator

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB query and distributed-query maintainers
- **Extends:** [ADR 0470](0470-canonical-multi-key-grouped-sufficient-state-exchange.md),
  [ADR 0471](0471-shared-mergeable-grouped-state-owner.md), and
  [ADR 0472](0472-query-accounted-grouped-partial-state-merge.md)

## Context

The grouped frame, shared worker table, and coordinator merge primitive existed, but an embedding
still had to decide when a tablet stream was complete, retain retry identity, reject conflicting
duplicates, choose merge order independently of network arrival, and prevent output from escaping
before every tablet closed. Floating sufficient-state merge makes an arbitrary arrival-order merge
observable.

## Decision

`DistributedVectorGroupedAggregateCoordinator` is one move-only, single-threaded owner for an exact
query ID, ordered tablet vector, key definitions, aggregate definitions, and finite limits. Every
accepted message is canonically encoded before retention; those exact bytes are the retry identity.
Per-tablet sequence must be gap-free, group count cannot change, a distinct empty terminal is the
only zero-group stream, and messages after terminal are rejected. An identical retained sequence is
idempotent; a byte-different retry at the same sequence is `ALREADY_EXISTS`.

`finish()` publishes no prefix. It requires a canonical terminal for every planned tablet and then
decodes and merges in plan-tablet order followed by tablet-local group ordinal, regardless of
network arrival order. Empty tablets contribute no fabricated group. The shared grouped table
coalesces equal keys and owns query-accounted state. Only successful closure and merge enable
`next()`, which emits query-accounted first-seen rows. Input is sealed before the first row.

Canonical frames remain retained when finish-time allocation or query-resource acquisition fails,
so the caller may retry `finish()`. Once output materialization begins, a failure becomes sticky and
the partially materialized table is discarded. The first failure reported by an incomplete worker
is sticky; a failure reported after that tablet's terminal is ignored. Message count, per-tablet
count, encoded bytes, decoded frame/key/state limits, global group table limits, retained
configuration, query memory, and output chunk bounds are all finite.

This is an in-memory coordinator. It does not split a worker plan, define an authenticated request
transport, bind Raft read authority, route a partitioned shuffle, or apply final grouped SQL
projection/ORDER BY/LIMIT.

## Consequences

Network scheduling no longer determines floating merge order or first-seen output order. No final
row can escape from a successful tablet prefix. Retrying an accepted frame is exact and bounded,
while conflicting recomputation fails closed. Retained encoded bytes are coordinator-owned but not
charged to query execution memory; they have an independent hard byte bound. Decoded keys, states,
the merge table, and output chunks use one query resource context. One thread serializes every
transition, so no inter-thread memory-ordering argument applies.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): one exact query/tablet/key/aggregate authority set
  gates every retained and decoded frame.
- [Invariant 11](../architecture/invariants.md): canonical frames, decoded messages, merge state,
  query resources, and emitted chunks have explicit nonoverlapping owners.
- [Invariant 15](../architecture/invariants.md): every retention, decode, group, memory, and output
  dimension is bounded before publication.
- [Invariant 18](../architecture/invariants.md): all-tablet closure is atomic and retry identity is
  exact canonical bytes rather than arrival timing.

## Validation

Focused tests interleave two nonempty tablet streams and one empty tablet, reject a per-tablet gap,
accept an exact duplicate, reject a conflicting duplicate, refuse incomplete finish, merge equal
keys in plan order, and publish exact COUNT/SUM rows only after all terminals. Authority, duplicate
tablet, stream-shape drift, sticky worker failure, completed-tablet failure, and empty-query output
cases pass. Allocation injection covers construction, canonical retention, retryable decode/merge,
variable key/extremum ownership, and sticky output failure. The full query suite passed 419 of 419
tests, the allocation-failure suite passed 61 of 61, and focused table/coordinator cases passed 6 of
6 under ASan/UBSan with leak detection disabled on Apple's runtime. The new production source
passed pinned clang-tidy 18; header self-containment, formatting, and whitespace checks passed.

## Migration and rollback

No wire or durable migration. Rollback removes the in-memory coordinator while retaining Grouped
Exchange v1, the shared table, and its coordinator merge surface.

## Unresolved questions

- Worker-side pre-group physical-plan splitting and canonical terminal construction.
- Authenticated query transport, read-authority binding, retry scheduling, and cancellation owner.
- Partition/shuffle routing, skew policy, final grouped projection, ORDER BY, and LIMIT integration.

## References

- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Distributed Vector Grouped Aggregate Exchange v1](../formats/distributed-vector-grouped-aggregate-exchange-v1.md)
