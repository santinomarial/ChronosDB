# Physical Operator Foundation

## Purpose and phase boundary

The first physical pipeline increment connects bounded vectors to query-wide memory and
cancellation. It defines one owning pull step and implements allocation-free Boolean filtering. It
does not lower a bound SQL plan or read ChronosDB storage, so the Phase 8 scalar executor remains
the only complete SQL execution path.

## Public interfaces

`chronos/query/physical_operator.hpp` exposes:

- `AccountedVectorChunk`, a move-only `VectorChunk` plus its live memory credit;
- `PhysicalOperatorStep`, a move-only tagged owner containing a chunk or explicit end;
- `PhysicalOperator`, the thread-affine `next(resources)` interface; and
- `BooleanFilterOperator`, a uniquely owned unary pipeline stage.

Factories and pulls return `common::Result`. Bad configuration or accounting is
`INVALID_ARGUMENT`, bad predicate ordinals are `OUT_OF_RANGE`, memory admission is
`RESOURCE_EXHAUSTED`, cooperative stop is `CANCELLED`, and a child error is propagated unchanged.

## Step and ownership lifecycle

The downstream caller invokes `next`. A chunk result moves both data and credit out of the child.
The Boolean filter consumes that owner and returns a new wrapper around the same physical columns,
selection allocation, and reservation. The caller may move the result into the next stage or let it
destruct, returning credit exactly once.

End-of-stream is a distinct step with no optional chunk. Calling `take_chunk()` on end fails. A
successfully ended Boolean filter returns end on every later pull. A chunk whose selection is empty
is still a chunk: it records input progress and may carry columns and memory.

Each operator uniquely owns its child and is thread-affine. No two threads may invoke the same
instance concurrently. The `QueryResourceContext` may be shared and polled concurrently, but that
does not make the operator concurrent.

## Accounted chunks

An accounted chunk requires a valid reservation whose charge is at least
`chunk.retained_buffer_bytes()`. The wrapper accepts a larger conservative charge so future builders
can include container objects and allocator overhead. The wrapper does not allocate and cannot prove
that the caller reserved before constructing buffers; builder and scan-adapter contracts must make
that ordering explicit. It does prove query identity: construction and every operator require the
reservation to belong to the same `QueryResourceContext` used for execution. Credit from an
unrelated query is rejected and released.

Credit remains live while a source, operator step, downstream stage, or caller owns the chunk. An
invalid filter releases the owner during error unwinding. Cancellation does not subtract the charge
early; it is returned only when the actual chunk owner destructs.

## Boolean selection semantics

`VectorSelection::where_true` consumes a validated selection and a canonical BOOL physical column.
For each currently selected physical ordinal:

- TRUE is copied to the next output slot;
- FALSE is removed; and
- NULL is removed, matching SQL WHERE rather than three-valued expression output.

The stable compaction loop is linear in selected rows, performs no allocation, preserves ordinal
order, and never duplicates a row. `resize` reduces logical index bytes but keeps capacity, so the
retained-byte count and query charge do not increase. Physical column buffers are unchanged.

`VectorChunk::where_true` applies that operation to a consumed chunk and updates its exact logical
buffer count. `AccountedVectorChunk::where_true` carries the original reservation across the
transformation.

## Pull, backpressure, failure, and cancellation

One pull asks the child for at most one step and returns at most one step. There is no hidden queue,
so a downstream caller that stops pulling stops new upstream output. This is the first backpressure
contract, not a parallel scheduling policy.

Every Boolean-filter pull checks cancellation before touching its child. A child error or local
filter error requests cancellation and returns the original status. This lets sibling contexts stop
at their next poll. No callbacks run and no other owner is reclaimed. If a pipeline already ended,
later cancellation does not turn its completed end into an error.

## Concurrency and memory ordering

Operator state (`ended`, child cursor, and chunk ownership) is plain thread-affine memory. There is
no atomic access and no internal lock. Resource counters and cancellation retain ADR 0021's relaxed
ordering because they publish no chunk or operator state. A future scheduler must release-publish a
complete task/pipeline owner and acquire it before invoking `next` on another thread.

## Complexity and failure behavior

An accounted-wrapper construction and operator step are `O(1)`. Boolean filtering is `O(S)` for
`S` selected rows and `O(1)` additional memory. A virtual call occurs once per chunk. Factory
allocation failure is `RESOURCE_EXHAUSTED`; filter validation failures release the input chunk and
reservation without durable or external effects.

## Verification and measurement

Unit tests cover ownership, credit coverage/release, explicit end, sticky completion, cancellation,
and hostile predicate configuration. A deterministic property test generates 17 chunks with varied
row counts, NULL/FALSE/TRUE predicates, and sparse selections, then compares retained ordinals with
the scalar truth rule. Fuzzing compacts arbitrary valid Boolean chunks under sanitizers.

`chronos_query_benchmarks` measures selection construction plus compaction at 64, 1,024, and 4,096
rows with TRUE densities of 100%, 25%, and 6.25%. It does not claim end-to-end query speed or justify
fusion, branch specialization, or physical materialization.

## Tradeoffs and next steps

Pull and unique child ownership make correctness visible but do not exploit cores. Keeping filtered
rows as a selection avoids copies but may reduce locality after very selective predicates. Both are
deliberate pre-measurement choices.

The next increment should define a typed physical expression/output builder or a storage scan
adapter with exact snapshot pins. Parallel scheduling should follow only after task ownership,
queue capacity, terminal-error arbitration, and cancellation release are specified.

## Likely review questions

**Why return empty chunks?** Empty selection is a valid result of a processed input chunk. Treating
it as end would truncate later input.

**Why does the reservation not shrink after filtering?** Stable compaction retains the same vector
capacity and all physical columns. Logical bytes shrink, but retained memory does not.

**Why virtual dispatch?** It gives the first composable operator boundary at one call per chunk.
Profiles must justify devirtualization or fusion later.

**Can two workers pull the same operator?** No. The instance is thread-affine; parallelism will use
separate tasks/pipelines or an explicitly synchronized scheduler owner.
