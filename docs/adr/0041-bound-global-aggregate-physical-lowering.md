# ADR 0041: Bound Global Aggregate Physical Lowering

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-planning and execution maintainers

## Context

ADR 0040 introduced a bounded streaming operator for one global group, while bound SQL aggregate
queries still stopped before physical planning. Lowering must preserve the binder's aggregate
identity, result shape, WHERE-before-aggregate order, empty-input cardinality, expression semantics,
and finite configuration limits. Aggregate arguments may themselves be vector expressions, and
SELECT results may combine multiple aggregate results in a final expression.

Grouped aggregation has materially different dynamic key-state, memory-accounting, and spill
requirements. It is not required to connect the existing fixed-state global operator to SQL.

## Decision

- `lower_bound_sql_select()` accepts single-source global aggregate SELECTs and continues to reject
  GROUP BY, ORDER BY, LATEST, joins, subscriptions, and EXPLAIN execution.
- WHERE is lowered and applied before aggregate input preparation. Direct source-column aggregate
  arguments keep their schema ordinal when every argument is direct. If any argument is computed,
  one bounded `ColumnOutputStage` materializes all non-star arguments in aggregate order.
- Each bound aggregate call becomes one ordered `VectorAggregateDefinition`. `COUNT(*)` has no
  input; every other definition records the exact materialized ordinal, type, and nullability.
- Lowering independently derives every physical aggregate result shape and requires exact agreement
  with the binder. Aggregate source spans then act as physical leaves for the final SELECT output
  expressions. No ungrouped source column may survive into a global aggregate result expression.
- Final expressions are ordinary bounded vector programs over the one-row aggregate result, followed
  by the existing output stage and then LIMIT. Empty input therefore still produces the SQL global
  group unless LIMIT removes it.
- `PhysicalSelectLoweringLimits` carries the existing `UngroupedAggregateLimits`; plan validation and
  operator construction recheck width, retained configuration, runtime input shape, and output
  limits.
- A present result from an operation whose declared result is nullable is materialized with a valid
  canonical validity bitmap and `null_count == 0`. `ConstantColumnOutputPosition::force_nullable`
  makes this physical-shape requirement explicit; typed NULL remains all-null and COUNT remains
  nonnullable.
- Variable-width MIN/MAX remain unsupported because ADR 0040 deliberately defers their retained
  payload accounting. Grouped state, partial aggregation, merge, spill, and optimizer rewrites are
  also deferred.

This decision changes no SQL binding rule, durable format, storage representation, dependency,
WAL behavior, or concurrency ownership model.

## Rationale and consequences

Using source spans preserves the binder's already-validated aggregate identity without adding a
second SQL expression representation. A single input-materialization stage keeps argument order and
runtime shape checks explicit. It may copy direct arguments when mixed with computed arguments;
projection pruning or fusion requires profile evidence and must preserve peak-memory accounting.

The nullable-constant capability repairs a general shape invariant rather than special-casing the
aggregate operator. Consumers of a declared nullable output can now trust the physical metadata on
both present and NULL executions.

## Validation

End-to-end tests cover all eight aggregate operations after WHERE, computed arguments, final
expressions, empty input, LIMIT zero, exact stage order, grouped/variable-extremum rejection, and
aggregate width limits. All frozen logical types exercise present forced-nullable constant
materialization. Allocation-failure injection, lowering fuzzing, sanitizer runs, static analysis,
installed external-consumer compilation, and a retained-bound-plan lowering microbenchmark protect
the implementation boundary.

## References

- [ADR 0008](0008-custom-sql-and-vectorized-execution.md)
- [ADR 0012](0012-correctness-testing-and-performance-evidence.md)
- [ADR 0023](0023-bounded-physical-pipeline-plan.md)
- [ADR 0036](0036-bound-select-to-physical-pipeline-lowering.md)
- [ADR 0040](0040-streaming-ungrouped-vector-aggregates.md)
- [SQL v1](../product/sql-v1.md)
- [Bound SELECT physical lowering guide](../learning/bound-select-physical-lowering.md)
- [Streaming aggregate guide](../learning/streaming-ungrouped-aggregates.md)
