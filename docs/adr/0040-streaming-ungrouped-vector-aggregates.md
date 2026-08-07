# ADR 0040: Streaming Ungrouped Vector Aggregates

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-planning and execution maintainers

## Context

The scalar SQL v1 engine already fixes aggregate result types, NULL behavior, exact numeric
accumulation, IEEE behavior, and NaN ordering. The vector pipeline can filter, transform, and
materialize chunks, but previously had no cardinality-changing aggregate stage. Retaining all input
rows for a later scalar pass would violate the bounded streaming operator model and duplicate the
query resource lifetime.

Grouped aggregation and variable-width extrema additionally require a policy for dynamically
growing key and payload state. That policy is not necessary for one global group and must not be
silently selected by this increment.

## Decision

- `UngroupedAggregateOperator` consumes its complete input stream one chunk at a time, retains no
  input chunk, and keeps one fixed-size state per declared aggregate. It emits exactly one
  canonical one-row chunk, including for empty input.
- Supported operations are `COUNT(*)`, `COUNT(expr)`, `SUM`, `AVG`, `MIN`, `MAX`, `VAR_POP`, and
  `VAR_SAMP`. `COUNT(expr)` accepts every physical input type without materializing the value.
  Other numeric aggregates accept numeric types. `MIN` and `MAX` accept fixed-width comparable
  types and use the scalar engine's total ordering, including NaNs.
- Integer, unsigned-integer, and DECIMAL `SUM` use the shared exact accumulator and validate the
  final declared result. FLOAT32/FLOAT64 `SUM`, AVG, and Welford variance intentionally match the
  scalar oracle's floating evaluation order and result types.
- NULL inputs do not contribute. Empty `COUNT` is zero; every other empty aggregate is NULL.
  `VAR_SAMP` is also NULL for one contributing value.
- Aggregate definitions carry exact source ordinal, logical type parameters, and nullability.
  Plan construction propagates exact result shapes; every runtime chunk is checked again before
  cell access.
- Aggregate width, retained fixed-state bytes, and output vector limits are finite and checked.
  Operator configuration/state is coordinator-owned bounded memory rather than input-chunk credit.
  The emitted canonical chunk is charged to the query resource context.
- The operator polls cancellation before work, between child pulls, and every 256 selected rows.
  Child/output failures request shared cancellation; RAII releases consumed input and partial
  output credit.
- Variable-width `MIN`/`MAX`, grouped state, state spill, parallel partial-state merging, and bound
  SQL aggregate lowering remain unsupported until their accounting and planning decisions exist.

This decision changes no SQL language rule, durable format, storage representation, dependency,
WAL behavior, or concurrency ownership model.

## Rationale and consequences

One global group provides the first cardinality-changing vector operator without introducing an
unbounded hash table. Reusing canonical column-output materialization keeps one physical-buffer
builder and one exact query-credit boundary. The current row-oriented state update is a correctness
baseline; column-specialized kernels or parallel merges require profiles plus differential proof
that floating evaluation and error behavior remain within the accepted SQL contract.

## Validation

Unit and deterministic property tests cover all operations across chunk and selection boundaries,
empty inputs, NULLs, NaNs, exact unsigned/DECIMAL sums, final overflow, runtime shape and query-owner
rejection, pre-cancellation, plan propagation, and variable-width `COUNT`. Allocation-failure
injection proves classified construction/output failure and exact credit release. Hostile physical-
plan fuzzing exercises aggregate definitions and valid execution. Sanitizers, self-contained public
headers, installed external consumption, full regression, and dense/sparse multi-chunk
microbenchmarks complete the evidence boundary.

## References

- [ADR 0008](0008-custom-sql-and-vectorized-execution.md)
- [ADR 0012](0012-correctness-testing-and-performance-evidence.md)
- [ADR 0020](0020-bounded-vector-chunk-representation.md)
- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0022](0022-pull-based-physical-operator-lifecycle.md)
- [ADR 0023](0023-bounded-physical-pipeline-plan.md)
- [SQL v1](../product/sql-v1.md)
- [Streaming aggregate guide](../learning/streaming-ungrouped-aggregates.md)
