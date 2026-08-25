# ADR 0475: Owned cross-tablet grouped vector authority

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB query, metadata, and distributed-query maintainers
- **Extends:** [ADR 0383](0383-owned-cross-tablet-vector-aggregate-definitions.md),
  [ADR 0397](0397-metadata-backed-schema-bound-vector-v2-snapshots.md), and
  [ADR 0474](0474-proof-revalidated-grouped-sufficient-state-worker-v2.md)

## Context

The proof-revalidated grouped worker could derive exact key and aggregate definitions from one
Fragment-v2 dispatch, but the compatible all-tablet snapshot discarded those grouped definitions.
A later scheduler would therefore have to reconstruct authority independently for every worker and
coordinator, reopening the cross-tablet schema join. Final result descriptors cannot recover hidden
COUNT/AVG/variance input types or nullability.

## Decision

`CompatibleDistributedVectorSnapshotV2` now owns two grouped-only vectors: ordered
`VectorGroupKeyDefinition` values and ordered `VectorAggregateDefinition` values. During compatible
binding, every grouped dispatch derives both from its exact destination schema projection and the
single owned result schema. The first tablet supplies the retained canonical vectors; every later
tablet must derive exactly equal key ordinals/types/nullability and aggregate operations/input
ordinals/types/nullability before the owner is published.

Ungrouped definitions retain their existing distinct accessor. Ungrouped and row plans expose empty
grouped spans; grouped plans expose an empty ungrouped span. Key-only grouping is valid and retains
nonempty key authority plus an empty grouped aggregate vector. The move-only owner continues to pin
the one compatible Manifest epoch, result schema, and plan-ordered dispatches that authorize the
borrowed spans.

Metadata-backed, leader-group-backed, and correlated follower constructors already delegate to the
compatible v2 binder, so the new authority automatically follows their existing catalog,
placement, group, read-proof, snapshot, and schema gates. No authority is inferred after binding.

## Consequences

Schedulers can pair every dispatch with one cross-tablet-proved grouped authority for worker
execution, exchange decoding, and coordinator construction. Schema evolution that preserves final
result shape but changes a hidden aggregate input now fails before execution. The retained vectors
are bounded by the existing 4,096 key/aggregate plan limits and stored once per query.

Construction is synchronous and single-threaded. Returned spans borrow the move-only owner, so no
cross-thread publication or memory-ordering argument is added.

## Affected invariants

- [Invariant 5](../architecture/invariants.md) and
  [Invariant 6](../architecture/invariants.md): every grouped authority vector is derived inside the
  same committed all-tablet snapshot and exact destination schema join.
- [Invariant 11](../architecture/invariants.md): dispatches, schema, grouped authority, and Manifest
  pin share one explicit move-only lifetime.
- [Invariant 15](../architecture/invariants.md): key and aggregate authority widths remain under the
  existing finite plan limits and are retained once.
- [Invariant 18](../architecture/invariants.md): worker and coordinator authority is not
  reconstructed from weaker output descriptors.

## Validation

The compatible two-tablet grouped binding test proves exact retained key and SUM input authority,
empty ungrouped authority, Fragment-v2 re-encoding, and rejection when a later tablet changes a
hidden COUNT input type while preserving the same final descriptors. The group-backed v2
constructor proves the same grouped authority survives the committed catalog/group proof path.
Allocation injection covers grouped authority derivation; full suites, focused sanitizers, pinned
clang-tidy, header self-containment, formatting, and whitespace checks remain milestone gates. The
full query suite passed 420 of 420 tests and the allocation-failure suite passed 61 of 61. All eight
fragment-binding cases and the grouped-authority allocation sweep passed under ASan/UBSan. The
changed production target passed pinned clang-tidy 18; formatting and whitespace checks passed.

## Migration and rollback

No durable or wire migration. Rollback removes the two grouped accessors and retained vectors, but
must restore fail-closed grouped scheduling rather than reconstructing authority elsewhere.

## Unresolved questions

- Owned scheduler requests that pair these spans with each plan-ordered dispatch.
- Authenticated grouped request/response transport and retry scheduling.
- Computed pre-group physical-plan splitting and final grouped SQL integration.

## References

- [Proof-revalidated grouped sufficient-state worker v2](0474-proof-revalidated-grouped-sufficient-state-worker-v2.md)
- [Distributed Vector Fragment v2](../formats/distributed-vector-fragment-v2.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
