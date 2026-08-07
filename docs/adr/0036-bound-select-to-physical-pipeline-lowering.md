# ADR 0036: Bound SELECT to Physical Pipeline Lowering

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-planning and execution maintainers

## Context

The SQL v1 binder retains exact catalog identities, source spans, types, and nullability. ADR 0035
adds checked physical expression programs, but callers still have to reconstruct bound SELECT
meaning manually. That gap prevents differential execution through the real binder/physical-plan
boundary and risks ordinal or NULL-semantics drift.

## Decision

- `lower_bound_sql_select()` lowers one already-bound, single-source, nonaggregate SELECT into an
  immutable `PhysicalPipelinePlan`. Its input is the primary source schema in exact schema-ordinal
  type/nullability order.
- WHERE becomes an owned output stage containing the source columns plus one checked Boolean
  position, followed by `BooleanFilterStage`. SELECT outputs then become ordered source, typed
  constant, or computed positions. LIMIT remains last.
- Column and literal leaves, unary/binary checked kernels, IS NULL, ABS, and SQL BETWEEN/IN are
  supported. BETWEEN and IN expand into the accepted comparison and three-valued Boolean DAG
  operations; they add no new runtime instruction kind. Identity casts may disappear. Other casts,
  variable-width computation, COALESCE, case conversion, and time bucketing fail as unsupported.
- Aggregate/grouping, ASOF, LATEST, ORDER BY, SUBSCRIBE, EXPLAIN modes, and multiple-source
  lowering fail explicitly. There is no scalar fallback and no claim that storage-source
  construction or result naming is owned here.
- Expression, output-chunk, and physical-plan limits are caller supplied and checked. Allocation
  and container failures become resource diagnostics. Unsupported bound features retain their most
  relevant source span.

This decision adds no durable format, dependency, optimizer, storage visibility rule, or scheduler.

## Rationale and consequences

The ordered unary pipeline is sufficient for the executable subset and preserves the existing
shape/accounting boundaries. Copying source columns into the temporary WHERE stage is conservative
but makes predicate lifetime and ownership explicit. Projection pruning and predicate fusion need
profile evidence and can later replace this baseline while remaining differential with it.

The vector engine can now consume real binder output for a useful SELECT subset. Remaining scalar
kernels, aggregates, ordering, temporal joins, complete storage visibility, and scheduling still
gate general SQL execution.

## Validation

Tests cover exact stage order and shapes, end-to-end WHERE/projection/LIMIT results, star and
variable-constant output, unsupported-feature classification, exact expression limits, installed
consumer compilation, sanitizers, and full repository regression. Lowering benchmarks exclude
parse and bind setup.

## References

- [ADR 0008](0008-custom-sql-and-vectorized-execution.md)
- [ADR 0023](0023-bounded-physical-pipeline-plan.md)
- [ADR 0035](0035-bounded-checked-vector-expression-programs.md)
- [Physical lowering guide](../learning/bound-select-physical-lowering.md)
