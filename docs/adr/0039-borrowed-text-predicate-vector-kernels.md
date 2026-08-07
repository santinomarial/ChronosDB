# ADR 0039: Borrowed Text-Predicate Vector Kernels

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-planning and execution maintainers

## Context

ADR 0038 keeps STRING/SYMBOL values borrowed through casts, ASCII case conversion, COALESCE, and
variable-width output, but rejects operations whose result is fixed-width. SQL v1 also requires
text equality, ordering, BETWEEN, IN, and NULL predicates. Converting a borrowed value to the owned
scalar representation for those operations would allocate one string per evaluated row.

## Decision

- One bounded hybrid row evaluator memoizes either an owned fixed-width `ScalarValue` or a borrowed
  text value containing a byte span, NULL state, and identity/lower/upper transform. It replaces the
  separate fixed and variable evaluators without changing the public instruction representation.
- STRING/SYMBOL comparisons require operands of one exact logical type. Equality and ordering
  compare transformed unsigned UTF-8 bytes lexicographically; a shared prefix sorts before its
  longer extension. This is the SQL v1 binary byte-order contract and never depends on physical
  dictionary identifiers.
- An ordinary comparison with either NULL operand returns nullable BOOL UNKNOWN. Text `IS NULL` and
  `IS NOT NULL` return nonnullable BOOL without reading or copying payload bytes.
- Comparison results compose with existing lazy AND/OR/NOT, BETWEEN, and IN lowering. Unreachable
  branches remain unevaluated, and borrowed spans never escape synchronous row evaluation.
- Successful row evaluation uses one fixed 256-slot automatic memo and performs no heap allocation.
  Output remains canonical BOOL validity/value bitmaps admitted by the existing exact output plan.

This decision changes no SQL binding rule, durable format, storage representation, dependency, or
WAL behavior. BINARY comparison remains outside the vector-expression leaf set.

## Rationale and consequences

A hybrid memo preserves one lazy DAG execution model while making ownership explicit at every
instruction. Direct transformed-byte comparison avoids scratch strings and is naturally compatible
with sparse selections. The larger fixed memo is a measured correctness baseline; specialization
or column-wise fusion requires profile evidence and the same scalar-differential semantics.

## Validation

Tests cover all six comparison operations, exact type rejection, transformed and non-ASCII bytes,
NULL/UNKNOWN, nonnullable NULL predicates, Boolean composition, bound SQL execution, deterministic
257-row sparse comparison against an independent unsigned-byte model, allocation failure, lowering
fuzz cases, sanitizers, installed consumers, and dense/sparse 16-byte predicate measurements.

## References

- [ADR 0008](0008-custom-sql-and-vectorized-execution.md)
- [ADR 0035](0035-bounded-checked-vector-expression-programs.md)
- [ADR 0036](0036-bound-select-to-physical-pipeline-lowering.md)
- [ADR 0038](0038-borrowed-variable-width-vector-materialization.md)
- [Vector expression guide](../learning/vector-expression-programs.md)
