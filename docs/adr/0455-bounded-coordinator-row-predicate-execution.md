# ADR 0455: Bounded coordinator row-predicate execution

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB SQL, query, cluster, replicated-service, and daemon maintainers
- **Extends:** [ADR 0035](0035-bounded-checked-vector-expression-programs.md),
  [ADR 0379](0379-bounded-global-vector-row-finalization-v2.md), and
  [ADR 0454](0454-bounded-coordinator-row-expression-execution.md)

## Context

Distributed row SQL executed exact event-time ranges at workers but rejected the rest of SQL v1's
checked Boolean expression surface. The coordinator can now evaluate the shared immutable vector
program over canonical Native rows, so retaining a separately checked predicate closes this
semantic gap without adding worker bytecode or a second scalar engine.

## Decision

The coordinator projection may own one optional `VectorExpression` predicate. Lowering retains an
exact event-time comparison/positive-BETWEEN conjunction as the existing worker predicate. Every
other bound Boolean WHERE expression is lowered into the coordinator program, including direct
Boolean columns, constants, arithmetic and text comparisons, NULL predicates, BETWEEN/IN, and
three-valued AND/OR/NOT supported by the shared vector engine.

A source-dependent coordinator predicate causes workers to carry the complete source schema in
exact ordinal order. A source-independent predicate does not widen an otherwise sufficient worker
projection. Mixed event-time and general predicates currently retain the complete Boolean program
at the coordinator; they are not partially pushed down.

After all authenticated tablet streams close and their schemas are validated, the finalizer
evaluates the predicate once for every decoded row. TRUE retains the row; FALSE and NULL discard it,
matching SQL WHERE truth. Global direct-column ORDER BY and LIMIT run only on the retained rows.
Visible computed outputs are still evaluated after LIMIT. A predicate failure aborts the complete
query before any result batch is published.

Predicate instructions, derived shapes, canonical row views, decoded rows, sort state, computed
outputs, and Native responses remain under existing finite configuration, working-memory, input,
and output limits. The synchronous finalizer remains single-threaded and publishes one owned
terminal value, so this decision adds no memory-ordering argument or acknowledged-write guarantee.

This decision adds no durable or network format, worker expression bytecode, partial predicate
pushdown, computed ORDER BY, aggregate predicate, join predicate, retry rule, or visibility rule.

[ADR 0456](0456-bounded-coordinator-global-aggregate-predicates.md) subsequently applies the same
checked coordinator predicate semantics before replicated global aggregate accumulation.

## Consequences

Replicated Native row queries can execute the existing checked SQL v1 WHERE expression intersection
over local and remote tablets. General predicates may transfer rows that workers could eventually
discard; exact event-time-only predicates retain their existing worker execution and pruning path.
The coordinator baseline is deliberately differential with local vector evaluation and provides a
correctness oracle for later versioned worker pushdown.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): one bound schema and exact source ordinals are
  preserved across predicate execution.
- [Invariant 11](../architecture/invariants.md): canonical row cells are borrowed only during the
  synchronous evaluation call while decoded batches remain owned.
- [Invariant 14](../architecture/invariants.md): existing fragment and result-exchange bytes are
  unchanged.
- [Invariant 15](../architecture/invariants.md): predicate configuration and row-view state are
  explicitly bounded.
- [Invariant 18](../architecture/invariants.md): worker event-time specialization and coordinator
  evaluation retain the same SQL truth contract.

## Validation

Unit coverage requires general numeric/text Boolean lowering, direct and constant predicate
programs, SQL NULL rejection, filter-before-global-order/LIMIT behavior, stale/non-Boolean shape
rejection, checked runtime errors, expression limits, and allocation-failure classification. The
two-tablet service integration requires byte-identical local and remote mTLS results for a computed
predicate. The Linux three-daemon source gate carries the same query but remains executable only on
Linux.

The complete normal query, cluster, and service suites pass with 408, 200, and 106 tests; their
allocation-failure suites pass with 56, 27, and 3 tests. Focused ASan/UBSan runs pass predicate
lowering, lowering allocation failure, filter/order/limit and hostile finalization, finalizer
allocation failure, and the two-tablet service integration with leak detection disabled. The three
changed production sources pass LLVM 18 clang-tidy, and formatting plus the installed public-target
consumer pass. The Linux-only process source is compiled only by Linux CI and is not claimed as a
local macOS pass.

## Migration and rollback

The predicate is coordinator-owned and in-memory only. Existing event-time-only plans retain their
wire bytes and worker execution. Rollback rejects non-event-time WHERE expressions again without
changing durable state or protocol compatibility.

## References

- [Distributed row SQL lowering](../learning/distributed-row-sql-lowering.md)
- [Checked vector expression programs](../learning/vector-expression-programs.md)
- [Distributed Vector Plan Intent v1](../formats/distributed-vector-plan-intent-v1.md)
