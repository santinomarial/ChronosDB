# ADR 0044: Query-Accounted Bounded Physical Sort

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-execution and physical-planning maintainers

## Context

SQL v1 requires deterministic `ORDER BY`, but the current CSEG and mutable-head vector sources do
not yet expose the shared hidden logical/version identity needed to break equal base-row keys.
Lowering SQL ordering while substituting arrival order would silently weaken the frozen contract.
Phase 9 nevertheless needs a real reorder/materialization boundary before hidden-column plumbing,
aggregate ordering, spill, or parallel merge can be implemented.

The existing selection vector is deliberately unique and increasing, so it cannot express an
arbitrary gather. A sort must retain input lifetime and credit while comparing rows, then create an
independent canonical output. It also must admit state before allocation rather than using process
allocation failure as its memory policy.

## Decision

- `SortOperator` is a blocking, thread-affine unary operator over one finite input. It buffers at
  most a configured number of selected rows, sorts them, and emits at most one independently owned
  identity-selected `VectorChunk`. Empty input emits no chunk.
- Each `VectorSortKey` names a current physical column plus ascending/descending direction and
  explicit NULL placement. All frozen logical types use the same total order as the scalar oracle:
  bytewise STRING/SYMBOL/BINARY, numeric and decimal order, UUID network-byte order, and NaN after
  positive infinity before NULL in ascending value order. Descending reverses non-NULL values;
  explicit NULL placement is not reversed.
- Equal configured keys preserve logical input order through a deterministic bottom-up stable merge.
  This is an operator-local tie rule, not a substitute for SQL's hidden row identity. SQL lowering
  must provide the required identity keys or remain unsupported.
- Before pulling input, the operator reserves a conservative fixed state charge derived from the
  configured row bound. That charge covers retained-chunk owners, primary and scratch row-reference
  arrays, and allocation overhead. Every buffered chunk retains its existing query reservation.
- Row references contain only chunk and selected-row ordinals. Variable keys compare borrowed
  canonical bytes without per-row or per-comparison payload allocation.
- Output planning performs checked size passes before requesting output credit. Input chunks and
  state credit coexist with output credit until every selected cell has been copied into canonical
  validity, Boolean, fixed-width, offset, and value buffers. State and input credit are released
  before the output owner is returned.
- The default bound is 2,048 rows, 256 keys, and 32 MiB of state. These are process policy defaults,
  not SQL limits or durable values. Output uses ordinary `VectorChunkLimits`.
- `SortStage` preserves exact physical shape in `PhysicalPipelinePlan`; plan configuration accounts
  for key-vector capacity and validates limits and ordinals before execution.
- Pull, comparison-pass, and output-column boundaries poll cooperative cancellation. Any child or
  local execution failure requests cancellation and releases all retained state through RAII.

This decision changes no durable or network format and adds no dependency.

## Consequences

ChronosDB now has an honest bounded reorder primitive and arbitrary-row canonical gather without
weakening the increasing-selection invariant. It can order grouped output once complete key/tie
columns are present and is the in-memory baseline for later external runs.

The baseline intentionally retains all input and produces one chunk. It cannot sort more rows than
the configured/output row domains, spill, stream runs, merge parallel partitions, or by itself meet
base-row SQL tie semantics. Fixed reservation may conservatively deny a small actual input when the
configured maximum is too large for the query budget.

## Validation plan

- Unit tests cover multi-chunk and multi-key ordering, explicit NULL placement, descending values,
  variable keys, stable ties, empty input, shape/ordinal/query-identity failures, and plan/limit
  composition.
- A fixed-seed model property compares varied keys and chunk boundaries with an independent stable
  scalar sort.
- Exhaustive allocation-failure injection requires `RESOURCE_EXHAUSTED`, cancellation, and zero
  leaked credit at every owned allocation.
- The physical-plan fuzzer drives hostile sort limits/ordinals and valid end-to-end sort execution
  under ASan/UBSan. ASan/UBSan, ThreadSanitizer, public-header, installation, and external-consumer
  checks cover the exported surface.
- Microbenchmarks vary row count and duplicate-key density, report allocations, and exclude source
  construction from any operator-performance interpretation.

## Unresolved questions

Hidden logical/version column plumbing, exact bound-SQL `ORDER BY` lowering, top-N selection, run
generation, spill bytes and credit transfer, parallel merge, scheduler integration, and cost rules
remain later Phase 9 work.

## References

- [SQL v1 contract](../product/sql-v1.md)
- [ADR 0020](0020-bounded-vector-chunk-representation.md)
- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0022](0022-pull-based-physical-operator-lifecycle.md)
- [ADR 0023](0023-bounded-physical-pipeline-plan.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
