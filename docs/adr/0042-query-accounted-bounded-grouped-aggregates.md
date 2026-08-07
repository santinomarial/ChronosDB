# ADR 0042: Query-Accounted Bounded Grouped Aggregates

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-planning and execution maintainers

## Context

The global aggregate operator retains one fixed state, but GROUP BY creates data-dependent state.
Group keys may contain variable-width values and NULL, and the number of groups is unknown until
execution. Treating a configured maximum as sufficient without query credit would let one operator
retain memory outside the query admission limit. Implementing an unbounded hash table or spill
before defining its ownership boundary would violate the Phase 9 resource invariant.

Variable-width MIN/MAX additionally replace a retained payload as values arrive. That requires a
reservation-growth protocol separate from retaining a variable-width grouping key once at group
creation.

## Decision

- `GroupedAggregateOperator` consumes accounted chunks, forms at most a caller-bounded number of
  groups, and emits one canonical accounted row per pull after input completion. Empty input emits
  no rows. Output columns are group keys in declared order followed by aggregate results in declared
  order.
- Group definitions carry exact physical ordinal, logical type parameters, and nullability. Every
  input chunk is shape- and query-owner-checked before cell access. The physical plan validates and
  propagates the same exact shapes.
- Group equality uses the scalar engine's deterministic total comparison for fixed-width values and
  exact bytes for STRING, SYMBOL, and BINARY. NULL key cells compare equal, so all NULLs occupy one
  group. First-seen group order is deterministic implementation behavior for evidence, not a SQL
  ordering guarantee.
- Group count, key width, aggregate width, per-group variable key bytes, retained configuration,
  and output chunks have explicit nonzero limits. Exceeding a limit is `RESOURCE_EXHAUSTED`.
- Before allocating group slots or a new group's owned key/state vectors, the operator acquires
  conservative `QueryResourceContext` credit. Charges include vector storage, variable key payload,
  and allocator overhead. Reservations remain attached to the retained group and are released after
  that group's canonical output is materialized or immediately when execution fails.
- Group lookup is an allocation-free linear scan over existing groups. This is the correctness
  baseline; adopting a hash table requires canonical hash semantics, bucket accounting, and profile
  evidence.
- The existing COUNT/SUM/AVG/fixed-width MIN/MAX/variance kernels and their NULL, NaN, overflow, and
  result-shape semantics are reused exactly. Variable-width MIN/MAX remain rejected.
- Output uses the existing canonical column-output boundary. A present nullable key or aggregate
  result carries an all-valid bitmap; a NULL key/result carries the ordinary all-null one-row form.
- Cancellation is polled every 256 selected rows. Child, state, output, ownership, or cancellation
  failure requests cancellation and destroys the child plus every retained group before returning.
- Partial aggregation, parallel merge, hash partitioning, spill, ORDER BY, and bound-SQL GROUP BY
  lowering are deferred.

This decision changes no SQL semantics, durable format, storage representation, dependency, WAL
behavior, or concurrency publication rule.

## Rationale and consequences

Linear lookup and one-row output chunks are deliberately simple and make ownership reviewable.
They can be slow for many groups and allocate during each emitted row, but they establish the exact
semantic and accounting oracle needed to measure a hash table and batched output safely. Variable
keys are admitted because their bytes are known before copying and can be charged once; replaceable
variable extrema are a different growth problem and remain excluded.

## Validation

Deterministic tests cover variable and NULL keys, sparse selection, all supported aggregate result
shapes, empty input, group and byte limits, exact plan propagation, cancellation, and credit release.
A fixed-seed 257-row property compares first-seen groups across three chunk boundaries with an
independent scalar model. Exhaustive allocation-failure injection covers creation, slot/key/state
retention, and output. Hostile physical-plan fuzzing, sanitizers, static analysis, installed external
consumption, and grouped cardinality microbenchmarks complete the evidence boundary.

## References

- [ADR 0008](0008-custom-sql-and-vectorized-execution.md)
- [ADR 0012](0012-correctness-testing-and-performance-evidence.md)
- [ADR 0020](0020-bounded-vector-chunk-representation.md)
- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0022](0022-pull-based-physical-operator-lifecycle.md)
- [ADR 0023](0023-bounded-physical-pipeline-plan.md)
- [ADR 0040](0040-streaming-ungrouped-vector-aggregates.md)
- [SQL v1](../product/sql-v1.md)
- [Bounded grouped aggregate guide](../learning/bounded-grouped-aggregates.md)
