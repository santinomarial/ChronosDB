# ADR 0035: Bounded Checked Vector Expression Programs

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-execution and resource-accounting maintainers

## Context

ADR 0034 established an accounted result boundary for source columns and typed constants, but a
physical plan still could not compute a value. Routing arithmetic through the scalar reference
executor once per row would couple the optimized path to AST/source-span lookup, permit per-row
heap allocation as variable-width semantics are added, and make physical shape and configuration
bounds implicit. The first computed-vector contract needs to preserve the scalar oracle's checked
numeric, IEEE, decimal, NULL, and Boolean short-circuit behavior while remaining independently
constructible and auditable.

## Decision

- `VectorExpression` is an immutable, copyable physical program containing at most 256 postorder
  instructions. Source, constant, unary, and binary instructions form a DAG by referencing only
  earlier instructions; the final instruction is the result.
- Source leaves carry an exact physical ordinal, logical type, and nullability. Constants must be
  typed. This program version accepts numeric and Boolean values plus DATE/TIMESTAMP_NS leaves for
  exact comparison. Variable-width and UUID leaves are rejected until their kernels are specified.
- Program creation derives every intermediate type and nullability. It supports unary positive,
  negative, NOT, IS NULL/IS NOT NULL, and ABS; SQL AND/OR; compatible comparisons; and checked
  add/subtract/multiply/divide/remainder. Signed and unsigned overflow, zero integer divisors,
  decimal precision, IEEE floating behavior, SQL NULL, NaN comparison, and three-valued truth match
  the scalar reference rules.
- Evaluation is lazy from the final instruction and memoized in fixed stack storage. AND/OR do not
  evaluate an unneeded branch, so an error in a scalar-unreachable branch remains unobserved.
  Successful instruction and row evaluation allocates no heap storage; constructing a diagnostic
  for a failing row may allocate.
- `ComputedColumnOutputPosition` extends `ColumnOutputPosition`. The existing output planner checks
  exact input shapes and fixed output bytes before query reservation, and the materializer writes
  canonical validity, Boolean, and fixed-width buffers over the same compacted or empty-progress
  domain as source and constant positions.
- Expression instruction/shape capacities are included in direct-operator and physical-plan
  retained-configuration accounting. Runtime output credit is reserved while input credit remains
  live. Failure requests cooperative cancellation and releases both charges through RAII.

This decision adds no durable representation, SQL lowering, casts, BETWEEN/IN, COALESCE,
LOWER/UPPER, time bucketing, aggregation, join, scheduler, spill, or optimizer rule.

## Detailed rationale

A small typed physical DAG separates execution shape from SQL syntax without prematurely defining a
general optimizer IR. Earlier-only references make validation linear and cycles impossible. Lazy
row evaluation is required for observable SQL error semantics; eager postorder evaluation would
incorrectly execute a division-by-zero branch beneath `FALSE AND ...`. Fixed stack memoization is a
bounded baseline that can later be replaced by measured column kernels without changing the public
program or results.

## Alternatives considered

- **Invoke the scalar AST evaluator per row:** rejected because it retains parser/binder identity in
  the physical data path and does not provide a no-per-row-allocation contract.
- **Eagerly materialize every intermediate vector:** rejected for the baseline because it multiplies
  peak memory and would need liveness/reuse planning before evidence shows that it wins.
- **Define every SQL v1 scalar operation now:** rejected because casts and variable-width functions
  need their own exact size-planning and transformed-byte contracts.
- **Use native structs or function pointers as bytecode:** rejected because explicit variants are
  easier to validate, fuzz, install, and inspect and do not create an ABI-dependent serialized
  format.

## Consequences

Physical pipelines can now produce checked computed numeric and Boolean columns in arbitrary output
order, with exact shapes and bounded configuration. Evaluation currently revisits instructions per
selected row and uses a conservative fixed memo array; it is a correctness baseline, not a claim of
optimal kernel throughput. Bound-SQL lowering and the remaining scalar operations must be added
before general SELECT execution can use this path.

## Affected invariants

This decision supports invariants [6, 11, and 18](../architecture/invariants.md): programs retain
exact bound physical shapes, output ownership/credit is explicit, and the computed path is compared
with deterministic scalar rules under failure, fuzz, sanitizer, and benchmark evidence.

## Validation plan

- Unit tests cover hostile programs, exact inferred shapes, input-shape mismatch, SQL NULL and
  short-circuit behavior, runtime overflow, cancellation, and resource release.
- A fixed-seed arithmetic property compares all signed kernels and comparisons over 257 values with
  an independent scalar model across canonical output cells.
- Physical-plan tests verify exact propagation, retained configuration, stage-order execution, and
  invalid current shapes.
- Allocation-failure injection, hostile physical-plan fuzzing, installed-consumer compilation, and
  self-contained-header checks cover construction and ownership boundaries.
- `materialize_checked_numeric_expression` measures a six-instruction computed predicate at dense
  and sparse selections with source construction excluded.

## Migration or rollback considerations

The program is in-memory only and has no persistence or network compatibility obligation. Rollback
removes the computed output alternative and expression API without changing source/constant output
or durable bytes. A replacement must preserve exact checked errors, NULL/NaN truth, short-circuit
behavior, finite configuration, query accounting, and canonical output.

## Unresolved questions

Physical casts, multi-comparisons, COALESCE, variable-width functions, time bucketing, bound-SQL
lowering, expression fusion, vector intermediates, aggregates, joins, hidden versions, scheduling,
and spill remain later Phase 9 decisions.

## Subsequent decisions

[ADR 0036](0036-bound-select-to-physical-pipeline-lowering.md) adds BETWEEN/IN expansion and exact
bound-SQL lowering. [ADR 0037](0037-fixed-width-vector-casts-and-scalar-functions.md) adds checked
fixed-width casts, lazy COALESCE, and `time_bucket`; variable-width functions remain unresolved.

## References

- [ADR 0008](0008-custom-sql-and-vectorized-execution.md)
- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0023](0023-bounded-physical-pipeline-plan.md)
- [ADR 0034](0034-accounted-typed-constant-vector-outputs.md)
- [Vector-expression guide](../learning/vector-expression-programs.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
