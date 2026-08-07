# Physical Pipeline Plan

## Purpose and boundary

The sixth Phase 9 increment turned the first independent vector operators into one validated,
reusable physical pipeline. The thirteenth increment adds exact timestamp-range truth.
`PhysicalPipelinePlan` now composes Boolean filtering, timestamp-range filtering, stable column-
subset projection, and global LIMIT in explicit order and propagates the exact physical column
shape through every stage.

This is deliberately not the SQL physical planner. It does not lower a `BoundSqlSelect`, optimize
stage order, scan CSEG/head storage, build computed vectors, aggregate, join, sort, schedule work,
spill, or materialize client results. Those paths need ownership and allocation contracts that do
not exist yet.

## Public interface

`chronos/query/physical_plan.hpp` exposes:

- `PhysicalColumnShape`: exact logical type parameters plus nullability, without durable identity;
- `BooleanFilterStage`, `TimestampRangeFilterStage`, `ColumnSubsetStage`, and `LimitStage`;
- `PhysicalPipelinePlanLimits`: finite input-width, stage-count, and retained-configuration bounds;
- `PhysicalPipelinePlan::create`: checked shape propagation and immutable plan construction; and
- `instantiate`: unique composition around one caller-owned `PhysicalOperator` source.

The plan is move-only. Its const accessors borrow immutable vectors, and independent const
instantiation is safe. Returned operator trees follow the existing thread-affine pull contract.

## Shape propagation

The input shape is a planner assertion, not durable schema identity. A Boolean filter requires its
current predicate ordinal to exist and have exact BOOL type, including normal logical-type
parameters. A timestamp-range filter similarly requires exact `TIMESTAMP_NS` type. Both preserve
every column shape. A stable subset requires unique, strictly increasing current ordinals and
compacts the shape in the same order. LIMIT preserves shape.

Validation is sequential, so this is rejected:

```text
input:  [INT64, BOOL]
subset: [0]
filter: predicate 0
```

After the subset, ordinal zero is INT64 rather than BOOL. The plan reports `INVALID_ARGUMENT`
before any source is pulled.

## Runtime source boundary

Instantiation first wraps the source with an internal shape validator. Each chunk must:

1. carry memory credit owned by the same `QueryResourceContext` used for the pull;
2. have the exact planned column count; and
3. match logical type parameters and nullability at every ordinal.

A mismatch requests cooperative cancellation and returns `INVALID_ARGUMENT`. The rejected owning
chunk unwinds immediately, returning its reservation. End-of-stream stays sticky, and an empty
stage list still validates every source chunk.

## Bounds and allocation accounting

The default plan limits are 4,096 input columns, 256 stages, and 2 MiB of retained configuration.
The retained count includes vector capacities for input/output shapes, stage variants, and every
subset-ordinal vector. Capacity rather than logical size prevents a caller from moving an
arbitrarily over-reserved vector into a small-looking plan. Checked multiplication and addition
classify overflow as `RESOURCE_EXHAUSTED`.

Plan/configuration memory and instantiated operator objects are not currently charged to the query
resource budget. They are finitely bounded, coordinator-owned state. Complete allocation charging
remains required before Phase 9 is complete.

## Failure and ownership behavior

Semantic plan mistakes and source-shape mismatches return `INVALID_ARGUMENT`. Limit violations,
size overflow, and factory-internal allocation failures return `RESOURCE_EXHAUSTED`. A null source
is invalid.
Operator-local or child execution errors retain ADR 0022 behavior: the pipeline requests shared
cancellation and RAII releases chunks and reservations.

LIMIT zero or a satisfied LIMIT destroys its unique upstream immediately. In a plan this can also
destroy the source-shape validator and all earlier operators without pulling or cancelling normal
work.

## Complexity

Plan validation is `O(columns + stages + total subset ordinals)` time and retains the corresponding
bounded configuration. Source validation is `O(input columns)` per produced chunk. Instantiation is
`O(stages)` time and one operator allocation per validator/stage. Per-row work remains in the
underlying operators.

The source shape walk is a correctness boundary, not a performance conclusion. It may be safely
elided or fused later only with equivalent trusted construction and profile evidence.

## Correctness evidence

Unit tests cover every validation and ownership boundary. A fixed-seed 128-plan differential test
generates physical values, SQL TRUE/FALSE/NULL predicates, sparse or empty selections, variable
chunk boundaries, stage order, and LIMIT values, then compares vector output with an independent
scalar row model.

Timestamp-range stage tests add current-shape type/ordinal validation and instantiated exact-bound
execution. Its row-level boundary/null/property coverage lives with the underlying operator.

`chronos_physical_plan_fuzz` drives hostile stage configurations and valid end-to-end execution.
`chronos_query_benchmarks` separately measures plan validation and instantiation at 1, 8, 64, and
256 stages. The measurements describe overhead; they are not end-to-end query performance claims.

## Tradeoffs and next steps

The unary variant is easy to audit and sufficient for current differential execution. It cannot
represent scans, branches, joins, exchanges, or sinks. The next increments should first settle
owned/borrowed storage-page backing and exact output-buffer admission, then add typed expression
execution and scan ownership. A later graph/optimizer can lower into or replace this pipeline while
retaining its shape and differential guarantees.

## Likely interview questions

**Why validate source shapes after validating the plan?** The plan proves only its own transitions.
A scan adapter or test source remains an independent producer and must not silently violate type or
nullability assumptions.

**Why omit column identity?** Intermediate columns can be computed or cardinality-only. Durable
`ColumnId` belongs to binding and scan mapping, not the physical buffer currency.

**Why count vector capacity?** Moving a vector with one element and a huge reserved allocation would
otherwise bypass a size-only configuration bound.

**Why is this not an optimizer?** Stage order is explicit and preserved. No semantic equivalence,
cost, statistics, or reordering rule has been accepted or measured yet.
