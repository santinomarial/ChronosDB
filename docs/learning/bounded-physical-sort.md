# Bounded Physical Sort

## Purpose and boundary

`SortOperator` is the first query-accounted arbitrary reorder in the vector engine. It consumes a
finite stream of accounted chunks, sorts selected rows by explicit physical keys, and emits one
independent canonical chunk. It is a physical primitive, not complete SQL `ORDER BY`: base-row SQL
ties still require hidden logical/version identities that current vector sources do not expose.

## Public interface

`chronos/query/sort.hpp` exports `VectorSortKey`, `SortLimits`,
`sort_state_reservation_bytes()`, and `SortOperator::create()`. `SortStage` integrates the same
configuration into `PhysicalPipelinePlan` and preserves the current column shape.

Each key has a current column ordinal, direction, and explicit `ScalarNullPlacement`. Callers must
include every semantic tie key. Equal configured keys retain input order only as the physical
operator's deterministic fallback.

## Algorithm and representation

The first pull reserves fixed state for the configured maximum, then retains each nonempty input
chunk and appends compact `{chunk ordinal, selected-row ordinal}` references. Empty selected chunks
are released immediately. Runtime chunks must share exact column count, type parameters, and
nullability; a plan adds its stronger source-shape validator before the sort.

A bottom-up stable merge uses one equally bounded scratch vector. Comparisons borrow cells from the
retained chunks. Fixed-width cells reuse the scalar total-order implementation; STRING, SYMBOL, and
BINARY compare canonical bytes directly, so comparison creates no owned payload. Descending
reverses only non-NULL comparisons, matching explicit SQL NULL placement.

After sorting, a size pass checks every canonical output buffer. The copy pass gathers arbitrary row
references into freshly owned validity/value/offset buffers and an identity selection. No output
aliases an input backing.

## Ownership, accounting, and failure

Three memory classes coexist at peak:

1. every input chunk's original reservation;
2. the sort-state reservation for chunk owners and two row-reference arrays; and
3. the output reservation for exact canonical buffers, column owners, selection, and overhead.

The operator releases classes 1 and 2 before returning class 3. A row/key/state/output limit,
checked-arithmetic failure, budget denial, or allocation failure returns `RESOURCE_EXHAUSTED`.
Bad ordinals return `OUT_OF_RANGE`; cross-query or changing shapes return `INVALID_ARGUMENT`.
Runtime failures request shared cancellation and destroy the child and state immediately.

Cancellation is polled before every child pull, every merge pass, and every output column. One merge
pass is bounded by configured rows times configured keys. The object is thread-affine and adds no
synchronization or publication primitive.

## Complexity and tradeoffs

For `R` selected rows, `K` keys, `C` columns, and copied variable bytes `V`, comparison is
`O(R log R * K)` and output copying is `O(R * C + V)`. State is `O(R)` row references plus retained
input and one output. Stable merge is easier to audit and propagate comparison errors through than
an opaque comparator passed to a library sort.

This one-chunk baseline deliberately has no top-N shortcut, run streaming, spill, or parallel merge.
Those optimizations require measured benefit and must retain exact order, credit, failure, and tie
semantics.

## Evidence and likely review questions

Deterministic tests cover multi-chunk, nullable variable, ascending/descending, stable tie, plan,
empty, hostile-limit, and ownership behavior. A fixed-seed property compares an independent stable
model. Allocation-failure injection, physical-plan fuzzing, both sanitizer families, installed
consumption, and duplicate-density microbenchmarks cover the remaining boundaries.

**Why not reorder the selection vector?** Its increasing unique order is an intentional filter
invariant. Sort is an explicit gather that produces a new canonical positional domain.

**Why retain all chunks?** Row references borrow their cells through those owners. Releasing input
before materialization would create dangling views and make memory accounting false.

**Why is stable input order not enough for SQL ties?** Scan arrival order may change after
compaction, parallelism, or plan changes. SQL requires stable logical/version identity; the planner
must carry those columns explicitly.
