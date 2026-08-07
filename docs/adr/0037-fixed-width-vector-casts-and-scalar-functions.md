# ADR 0037: Fixed-Width Vector Casts and Scalar Functions

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-planning and execution maintainers

## Context

The checked vector expression program and first bound-SELECT lowerer cover arithmetic, comparison,
Boolean logic, BETWEEN, IN, and ABS. SQL v1 also has checked numeric/decimal/temporal casts, lazy
COALESCE, and epoch-aligned `time_bucket`. Falling back to row scalar evaluation would hide
allocation and error semantics inside the vector path. Variable-width LOWER/UPPER needs a distinct
output-sizing design and should not be bundled into the fixed-width program.

## Decision

- `VectorCastExpression` owns one earlier operand index and exact target `LogicalType`. Creation
  admits only the binder's fixed-width numeric-to-numeric, temporal-to-temporal, or identity
  conversions and derives exact target type/nullability.
- `kCoalesce` is a lazy binary instruction over one exact type. Lowering inserts checked casts to
  the bound common type and folds an argument list into a left-associated chain. A non-NULL left
  value prevents evaluation of the right branch, including its possible runtime error.
- `kTimeBucket` consumes an INT64 nanosecond width and TIMESTAMP_NS point. It rejects nonpositive
  widths and rounds negative timestamps toward the lower epoch-aligned boundary with checked
  multiplication.
- Cast execution independently implements the scalar oracle's range, truncation, DATE/TIMESTAMP,
  decimal-rescale, and IEEE conversion rules. Any runtime failure cancels the physical pipeline and
  releases accounted output.
- STRING/SYMBOL casts and LOWER/UPPER remain unsupported by the vector program until transformed
  variable bytes have an exact pre-allocation and accounting contract.

This decision changes no SQL binding rule, durable format, storage representation, dependency, or
WAL behavior.

## Rationale and consequences

Dedicated physical instructions keep plan validation independent of SQL AST lifetime and preserve
the fixed automatic memo bound. Binary COALESCE avoids retaining a variable argument vector inside
one instruction while preserving exact short-circuit behavior. Keeping variable-width operations
out prevents an apparently convenient scalar fallback from adding per-row heap allocation.

The supported single-source lowering intersection now includes SQL v1 fixed-width numeric,
Boolean, temporal, decimal, and UUID scalar operations. Variable-width transforms, aggregates,
ordering, and wider relational operators remain future work.

## Validation

Validation covers hostile instruction shapes, exact inferred nullability, all major cast families,
negative time bucketing, lazy error suppression, runtime range failure and cancellation, exhaustive
allocation failure, deterministic scalar differential execution over 257 rows, lowering fuzzing,
sanitizers, installation, and isolated cast/COALESCE measurement.

## References

- [ADR 0008](0008-custom-sql-and-vectorized-execution.md)
- [ADR 0035](0035-bounded-checked-vector-expression-programs.md)
- [ADR 0036](0036-bound-select-to-physical-pipeline-lowering.md)
- [Vector expression guide](../learning/vector-expression-programs.md)

## Subsequent decision

[ADR 0038](0038-borrowed-variable-width-vector-materialization.md) adds the deliberately deferred
STRING/SYMBOL cast and ASCII case path with exact two-pass sizing. It does not alter this ADR's
fixed-width kernels or permit a row-scalar fallback.
