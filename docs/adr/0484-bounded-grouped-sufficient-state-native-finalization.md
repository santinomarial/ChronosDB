# ADR 0484: Bounded grouped sufficient-state Native finalization

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB query, cluster, and protocol maintainers
- **Extends:** [ADR 0473](0473-bounded-all-tablet-grouped-state-coordinator.md),
  [ADR 0476](0476-portable-pinned-grouped-sufficient-state-execution-owner.md), and
  [ADR 0483](0483-pinned-grouped-sufficient-state-tcp-scheduling.md)

## Context

The grouped sufficient-state coordinator produced globally merged query-accounted physical chunks,
but callers still had to apply the pinned plan's final order and limit and encode Native results.
Using another memory authority for those operators would violate physical-pipeline chunk ownership.

## Decision

`finalize_distributed_vector_grouped_aggregate_v2` consumes one successfully finished portable
grouped execution. It revalidates the pinned grouped plan, exact key/aggregate authority, aggregate
output shapes, and result schema. It constructs only the plan-declared `SortStage` and `LimitStage`,
then instantiates them over an owning source that drains the grouped execution.

The coordinator exposes a copied handle to its output `QueryResourceContext` only while output is
ready. Final sort/limit runs under that same authority, so retained input chunks and new operator
state share one query budget. No second unaccounted pool exists. Each successful output chunk is
encoded as a bounded Native Protocol v1 batch; zero rows produce one schema-bearing batch. Output
row, batch, byte, sort-row/key/state, column, name, and protocol payload limits are explicit.

This boundary supports the direct grouped sufficient-state schema: canonical group keys followed by
aggregate results, with plan output ordering and limit. It does not add computed final projection,
computed pre-group expressions, shuffle routing, or daemon request composition.

## Consequences

The scalable grouped path can now produce exact Native batches after global all-tablet closure.
Sort semantics reuse the ordinary checked physical operator, including direction, NULL placement,
stable ties, and query accounting. The consuming call publishes no prefix on validation, pipeline,
allocation, or encoding failure. One thread owns all calls; no inter-thread memory ordering applies.

## Validation

Two tablet streams produce distinct FLOAT64 keys and COUNT states under a pinned descending key
order and global limit one. Finalization publishes exactly the larger key and its count in one
decoded Native batch. Invalid limits and unfinished input publish nothing. The focused four grouped
execution/finalization cases pass normally and under ASan/UBSan; the complete cluster suite passes
246 of 246 and allocation-failure neighbors pass 31 of 31. Header self-containment, formatting, and
whitespace checks pass. LLVM 18 static analysis remains blocked by the installed macOS 26 libc++.

## Unresolved questions

- Computed pre-group and final projection splitting.
- Partitioned shuffle routing and multi-process qualification.

**Retrospective (2026-08-25):** [ADR 0485](0485-atomic-grouped-native-tcp-publication.md)
composed finalization into the Manifest-pinned scheduler, and
[ADR 0498](0498-atomic-mutable-grouped-native-publication.md) reused the same finalizer policy for
the distinct mutable authority view. Replicated/package lifecycle selection remains separate.

## References

- [Bounded all-tablet grouped-state coordinator](0473-bounded-all-tablet-grouped-state-coordinator.md)
- [Pinned grouped sufficient-state TCP scheduling](0483-pinned-grouped-sufficient-state-tcp-scheduling.md)
- [Native Protocol v1](../protocol/native-v1.md)
