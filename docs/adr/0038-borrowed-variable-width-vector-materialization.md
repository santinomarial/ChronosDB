# ADR 0038: Borrowed Variable-Width Vector Materialization

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-planning and execution maintainers

## Context

SQL v1 binds STRING/SYMBOL identity casts, lazy common-type COALESCE, and ASCII-only LOWER/UPPER.
Unlike fixed-width results, transformed text needs exact offsets and value-buffer admission before
the output reservation. Converting every physical cell to the owned scalar reference representation
would allocate per row and obscure peak memory.

## Decision

- Variable expression programs admit STRING/SYMBOL source and constant leaves, casts between those
  two logical types, ASCII LOWER/UPPER, and lazy exact-type COALESCE.
- A successful row result is a borrowed byte span plus NULL state and one identity/lower/upper
  transform. It borrows immutable input or program storage only during synchronous evaluation.
- The last case transform dominates earlier case transforms; this is equivalent for ASCII letters,
  while every non-ASCII byte remains unchanged. Physical input and constants are already UTF-8
  validated, and byte-preserving ASCII mapping therefore preserves validity.
- Planning evaluates selected rows without copying payloads, checks the exact `(rows + 1) * 4`
  offset bytes and concatenated value bytes, enforces UINT32 offsets and configured buffer limits,
  then reserves query memory. Materialization reevaluates borrowed rows and writes canonical offsets,
  validity, and transformed values directly into owned buffers.
- Operations that consume variable-width instructions must currently return a variable-width
  result. Text comparisons, text IS NULL predicates, and other fixed-width results remain rejected
  until a borrowed fixed-result kernel exists; they do not fall back to the owned scalar evaluator.

This changes no SQL binding rule, durable format, dependency, storage representation, or WAL
behavior.

## Rationale and consequences

The explicit two-pass contract makes peak memory and failure order reviewable. A compact transform
tag handles arbitrary nested case calls without intermediate strings. Re-evaluation costs extra CPU
but avoids offset scratch state and hidden ownership; benchmarks record that baseline before any
fusion. Keeping text-dependent predicates rejected prevents accidental per-row allocation.

## Validation

Tests cover exact type/nullability inference, invalid operations, nullable source/fallback
COALESCE, nested transforms, STRING-to-SYMBOL output, canonical offsets, sparse execution,
allocation failure, bound SQL execution, lowering fuzz cases, sanitizers, external consumers, and
dense/sparse 16-byte text measurements.

## References

- [ADR 0008](0008-custom-sql-and-vectorized-execution.md)
- [ADR 0035](0035-bounded-checked-vector-expression-programs.md)
- [ADR 0036](0036-bound-select-to-physical-pipeline-lowering.md)
- [ADR 0037](0037-fixed-width-vector-casts-and-scalar-functions.md)
- [Vector expression guide](../learning/vector-expression-programs.md)
