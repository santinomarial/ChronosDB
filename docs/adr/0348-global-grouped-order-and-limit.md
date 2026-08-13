# ADR 0348: Global grouped order and limit

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and distributed-systems maintainers
- **Extends:** [ADR 0324](0324-bounded-grouped-float64-coordinator.md),
  [ADR 0341](0341-fail-closed-grouped-query-execution-owner.md)

## Context

The grouped coordinator merged all tablet partials into one canonical group set, but exposed only
its internal key order. Applying ORDER BY or LIMIT independently at workers would be incorrect:
equal groups must merge across tablets before ordering, and a local limit can discard a globally
winning key. Embeddings also needed a bounded top-N contract without changing the frozen exchange or
dispatch bytes.

## Decision

`DistributedGroupedFloat64ResultOptions` configures ascending or descending group-key order,
explicit NULLS FIRST or NULLS LAST placement, and an optional unsigned LIMIT. Options are retained by
the grouped coordinator and carried through `DistributedGroupedQueryExecutionLimits` into packaged
execution.

`finish` first verifies every tablet terminal, merges every canonical key globally, materializes the
complete bounded result set, then orders and truncates it. FLOAT64 comparison matches the scalar
ordering contract: ordinary values use numeric order, signed zero is already canonicalized, and NaN
sorts after ordinary values in ascending order and before them in descending order. Null placement
is independent of direction. LIMIT zero returns an empty result only after complete validation and
merge. The existing message bound remains the retained group/result bound.

This is group-key ordering only. Ordering by aggregate expressions, multi-key tuples, arbitrary row
exchange, and general physical plan serialization remain separate contracts. No durable or network
format changes.

## Consequences and validation

Ordering costs `O(groups log groups)` after the existing `O(messages log groups)` merge and retains
one bounded result vector. The coordinator is single-threaded, so no inter-thread memory-ordering
argument applies.

The focused query test merges one key across two tablets, includes negative, NaN, and NULL keys,
applies descending NULLS LAST LIMIT 2, and proves NaN then the globally merged key are returned. It
also covers LIMIT zero and invalid enum rejection. A cluster-owner test proves the options cross the
execution boundary and select the global key 7 over key 5. Header self-containment and installed
consumer compilation cover the public API.

General vector fragments/exchanges, aggregate-expression ordering, multi-key/non-FLOAT64 grouping,
real multi-process failover, and broad fault/measurement evidence remain incomplete. No Phase 16
exit gate is claimed.

Invariants 5, 6, 10, 14, 15, and 18 apply.

## References

- [Bounded grouped FLOAT64 coordinator](0324-bounded-grouped-float64-coordinator.md)
- [Fail-closed grouped query execution owner](0341-fail-closed-grouped-query-execution-owner.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Architecture invariants](../architecture/invariants.md)
