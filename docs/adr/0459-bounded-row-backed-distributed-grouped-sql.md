# ADR 0459: Bounded row-backed distributed grouped SQL

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB SQL, query, cluster, replicated-service, and Native Protocol maintainers
- **Extends:** [ADR 0042](0042-query-accounted-bounded-grouped-aggregates.md),
  [ADR 0043](0043-bound-grouped-aggregate-physical-lowering.md),
  [ADR 0366](0366-schema-bound-distributed-vector-result-exchange.md), and
  [ADR 0379](0379-bounded-global-vector-row-finalization-v2.md)

## Context

The local physical engine already executes checked, query-accounted multi-key GROUP BY across the
SQL v1 scalar and aggregate types. The replicated query plane already returns complete
schema-bound mutable rows from every authority-proved tablet, but distributed Native SQL rejected
every GROUP BY. The existing sufficient-state grouped exchange supports only one nullable FLOAT64
key and is not a valid carrier for the broader local grouped surface.

## Decision

Grouped distributed lowering produces two owned products. Workers receive an unlimited identity row
plan over every source column in exact bound schema order. The coordinator receives the ordinary
immutable `PhysicalPipelinePlan` produced by `lower_bound_sql_select`, plus the exact named client
result schema. This is an in-process plan only; no physical operator or expression program is added
to network bytes.

After all local and authenticated remote tablet streams close, the finalizer independently
revalidates identity-plan shape, query/tablet correlation, contiguous terminal streams, descriptor
identity, row/message/encoded-byte limits, physical input/output shapes, and result names. It
preflights the exact canonical column-buffer size for each Native batch, then converts one batch at
a time into a query-accounted `VectorChunk`. A thread-affine source feeds the existing physical
pipeline in deterministic plan/tablet/message order.

The shared pipeline applies WHERE, computed group keys, multi-key canonical hashing, computed
aggregate arguments, all current aggregate kernels, final expressions, global ORDER BY with
deterministic group-key ties, and LIMIT. No result payload escapes until the pipeline ends
successfully. Empty grouped input returns one zero-row schema-bearing Native payload.

Input rows, messages, exchange bytes, per-batch materialization, query-wide pipeline memory, output
rows, output batches, output bytes, and Native shapes have independent finite limits. Allocation,
container growth, arithmetic overflow, group cardinality, key payload, aggregate extrema, sort, and
output exhaustion fail the whole query. Existing whole-query retry discards the failed attempt and
reacquires fresh authority before this finalization boundary.

This baseline intentionally transfers full source rows and performs grouping at the coordinator.
It does not change durable or wire formats, execute local physical plans at workers, add spill,
claim sufficient-state multi-key transport, or change snapshot, authority, retry, cancellation, or
acknowledged-write guarantees.

## Consequences

Replicated Native SQL now executes the same bounded single-source GROUP BY surface as local vector
SQL, including non-FLOAT64 and variable-width keys, multiple keys, computed keys and aggregate
inputs, final expressions, global ordering, and LIMIT. The cost is `O(all selected source bytes)`
exchange and coordinator work `O(rows * (keys + aggregates))` plus any final sort. Query memory is
bounded by the admitted batch plus the existing grouped, sort, and output operators.

The finalizer and every instantiated operator remain single-owner and thread-affine. No new shared
publication exists, so no memory-ordering argument applies.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): workers expose only committed/applied snapshot rows.
- [Invariant 6](../architecture/invariants.md): one bound source schema and one compatible all-tablet
  authority snapshot cover worker rows and coordinator execution.
- [Invariant 11](../architecture/invariants.md): encoded messages outlive synchronous decode, while
  each pipeline chunk and all retained group keys own query-accounted bytes.
- [Invariant 14](../architecture/invariants.md): Result Exchange v2 and mutable fragment bytes are
  unchanged; the physical plan remains process-local.
- [Invariant 15](../architecture/invariants.md): every input, materialization, grouped-state, sort,
  query-memory, and output dimension has a finite checked ceiling.
- [Invariant 18](../architecture/invariants.md): the coordinator reuses the proved local physical
  semantics rather than introducing a reduced grouped evaluator.

## Validation

Lowering coverage requires exact full-source identity construction, physical/result shape checks,
unsupported-mode rejection, caller bounds, and allocation injection. Cluster coverage requires a
two-tablet multi-key query with a computed variable key, Boolean key, WHERE, computed aggregate
input, cross-tablet group merge, aggregate ordering, LIMIT, empty input, materialization bounds,
hostile shape rejection, header consumption, and allocation injection. The replicated service gate
requires byte-identical grouped output through remote mutual TLS and co-located workers.

Before this decision was committed, the complete normal query, cluster, and service suites passed
411, 206, and 106 tests respectively; their allocation-failure suites passed 57, 28, and 3 tests.
Focused ASan/UBSan runs passed the grouped query lowering, lowering allocation sweep, grouped
finalizer functional/hostile cases, finalizer allocation sweep, and real local/remote service case.
All three changed production sources passed clang-tidy, all changed C++ files passed clang-format,
the diff passed whitespace validation, and the installed public-target consumer passed.

## Migration and rollback

The grouped plan and finalizer are pre-alpha in-memory APIs. Rollback restores explicit distributed
GROUP BY rejection without changing stored, fragment, exchange, or Native bytes. Existing local
grouped execution and one-key FLOAT64 sufficient-state transport remain independent.

## References

- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Bound SELECT physical lowering](../learning/bound-select-physical-lowering.md)
- [Bounded grouped aggregates](../learning/bounded-grouped-aggregates.md)
- [Distributed Vector Result Exchange v2](../formats/distributed-vector-result-exchange-v2.md)
