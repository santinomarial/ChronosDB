# ADR 0043: Bound Grouped Aggregate Physical Lowering

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-planning and execution maintainers

## Context

ADR 0042 introduced finite query-accounted grouped physical state, but bound SQL `GROUP BY` still
stopped before physical planning. Group expressions in the SELECT list have different source spans
from their equivalent GROUP BY syntax, aggregate arguments may be computed, and final expressions
may combine grouped keys and aggregate results. Lowering must preserve binder identity and exact
physical shapes without allowing an ungrouped source column to cross the aggregate boundary.

## Decision

- `lower_bound_sql_select()` accepts bounded, single-source grouped SELECTs in addition to global
  aggregates. ORDER BY, LATEST, joins, subscriptions, EXPLAIN execution, variable-width MIN/MAX,
  hash grouping, and spill remain outside this decision.
- WHERE retains its existing first position. One bounded `ColumnOutputStage` then materializes every
  GROUP BY expression in declared order followed by each non-star aggregate argument in aggregate
  traversal order. Direct and computed positions use the same checked vector-expression boundary.
- Every group key definition records its prepared ordinal plus the binder's exact logical type and
  nullability. Every aggregate definition records its exact prepared ordinal and independently
  derived kernel shape. The grouped operator emits keys first and aggregate results second.
- Aggregate calls are identified by their exact bound source spans. Group expressions are matched
  structurally using the binder's rule: resolved source/column ordinals for column leaves and exact
  syntax identity recursively for all other nodes. The mapping is used only while constructing the
  owning physical plan.
- Final SELECT expressions compile as bounded vector programs over grouped keys and aggregate
  outputs. Any remaining source-column leaf is an internal lowering failure. Direct grouped-column
  outputs take this expression path rather than bypassing the aggregate boundary.
- Empty grouped input emits no rows. LIMIT follows final projection. Without ORDER BY, output order
  remains unspecified SQL behavior even though the current physical baseline emits first-seen
  groups deterministically.
- `PhysicalSelectLoweringLimits` adds `GroupedAggregateLimits`; plan creation rechecks group, key,
  aggregate, configuration, output, and expression bounds before returning a plan.

This decision changes no binding rule, SQL semantic, durable format, storage representation,
dependency, WAL behavior, or concurrency publication rule.

## Rationale and consequences

A single preparation stage makes the aggregate input schema explicit and reviewable. It can copy
direct keys or arguments, but fusion and projection pruning require profile evidence and must
preserve query-accounting peaks. Structural matching is required because equivalent GROUP BY and
SELECT expressions are distinct AST objects and spans; reimplementing expression semantics through
text comparison would be incorrect for qualified and resolved columns.

The current aggregate traversal can compute an identical aggregate call more than once when it
appears at multiple source spans. Common-subexpression elimination is an optimizer decision and is
not part of correctness lowering.

## Validation

End-to-end tests cover WHERE-before-group order, computed and direct keys, multiple key order,
computed aggregate arguments, final expressions, nullable variable keys, empty groups, LIMIT,
grouped limits, and the variable-extremum rejection. A fixed-seed 257-row property compares output
as an unordered set against an independent grouped model. Allocation-failure injection, hostile
lowering fuzzing, sanitizers, static analysis, installation consumption, and a retained-bound-plan
microbenchmark protect the new path.

## References

- [ADR 0008](0008-custom-sql-and-vectorized-execution.md)
- [ADR 0012](0012-correctness-testing-and-performance-evidence.md)
- [ADR 0023](0023-bounded-physical-pipeline-plan.md)
- [ADR 0036](0036-bound-select-to-physical-pipeline-lowering.md)
- [ADR 0041](0041-bound-global-aggregate-physical-lowering.md)
- [ADR 0042](0042-query-accounted-bounded-grouped-aggregates.md)
- [SQL v1](../product/sql-v1.md)
- [Bound SELECT physical lowering guide](../learning/bound-select-physical-lowering.md)
- [Bounded grouped aggregate guide](../learning/bounded-grouped-aggregates.md)
