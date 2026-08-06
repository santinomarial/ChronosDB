# Physical Operator Foundation

## Purpose and phase boundary

The physical pipeline foundation connects bounded vectors to query-wide memory and cancellation.
It defines one owning pull step and implements allocation-free Boolean filtering plus stable
column-subset projection and global LIMIT. It does not lower a bound SQL plan or read ChronosDB
storage, so the Phase 8 scalar executor remains the only complete SQL execution path.

## Public interfaces

`chronos/query/physical_operator.hpp` exposes:

- `AccountedVectorChunk`, a move-only `VectorChunk` plus its live memory credit;
- `PhysicalOperatorStep`, a move-only tagged owner containing a chunk or explicit end;
- `PhysicalOperator`, the thread-affine `next(resources)` interface; and
- `BooleanFilterOperator`, `ColumnSubsetOperator`, and `LimitOperator`, uniquely owned unary
  pipeline stages.

Factories and pulls return `common::Result`. Bad configuration or accounting is
`INVALID_ARGUMENT`, bad predicate or projection ordinals are `OUT_OF_RANGE`, memory or bounded-plan
admission is `RESOURCE_EXHAUSTED`, cooperative stop is `CANCELLED`, and a child error is propagated
unchanged.

## Step and ownership lifecycle

The downstream caller invokes `next`. A chunk result moves both data and credit out of the child.
A unary operator consumes that owner and returns a wrapper around retained physical columns, the
same selection allocation, and the same reservation. The caller may move the result into the next
stage or let it destruct, returning credit exactly once.

End-of-stream is a distinct step with no optional chunk. Calling `take_chunk()` on end fails. A
successfully ended unary operator returns end on every later pull. A chunk whose selection is empty
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
invalid transformation releases the owner during error unwinding. Cancellation does not subtract
the charge early; it is returned only when the actual chunk owner destructs.

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

## Stable column-subset projection

`VectorChunk::project_columns` accepts only unique, strictly increasing input ordinals. It validates
the complete request before mutation, then move-compacts retained column owners toward the front and
destroys the rest. The operation allocates no memory, preserves physical rows, selection ordinals,
row order, and column order, and recomputes logical and retained canonical-buffer counts. An empty
subset is valid and preserves row cardinality for operators such as `COUNT(*)`.

Duplicate and reordered columns are deliberately not projection pushdown: they require a later typed
output builder with explicit output positions. `ColumnSubsetOperator` bounds its retained ordinal
plan at 4,096 entries. The accounted wrapper carries the original reservation after dropping
buffers. That credit may conservatively exceed the remaining buffers and is returned as one unit;
shrinking it could improve utilization but requires a separately specified resize/transfer
contract. This first stage keeps the original finite credit and never undercounts.

## Global LIMIT semantics

`VectorSelection::take_first` stable-truncates selected ordinals with no allocation. It preserves
physical rows and retained selection capacity, updates identity state exactly, and treats a maximum
larger than the current selection as a no-op. The chunk and accounted wrappers update logical bytes
and carry the original reservation.

`LimitOperator` stores the SQL v1 unsigned 64-bit limit and subtracts selected rows across chunk
boundaries. Empty selected chunks remain progress, are forwarded unchanged, and consume none of the
remaining limit. A partial final chunk keeps its selected prefix. LIMIT zero is immediately ended
without pulling its child. When the limit is reached exactly or partially, the operator destroys
its uniquely owned upstream pipeline before returning the final chunk; this releases every future
buffered chunk and reservation without misclassifying normal LIMIT completion as cancellation.

## Pull, backpressure, failure, and cancellation

One pull asks the child for at most one step and returns at most one step. There is no hidden queue,
so a downstream caller that stops pulling stops new upstream output. This is the first backpressure
contract, not a parallel scheduling policy.

Every unary pull checks cancellation before touching its child. A child error or local operator
error requests cancellation and returns the original status. This lets sibling contexts stop at
their next poll. No callbacks run and no other owner is reclaimed. If a pipeline already ended,
later cancellation does not turn its completed end into an error.

Normal LIMIT completion does not request cancellation: it is a successful local end. Unique child
ownership is what permits eager upstream destruction. A future parallel scheduler will need an
explicit normal-stop signal for already running sibling tasks rather than treating it as failure.

## Concurrency and memory ordering

Operator state (`ended`, child cursor, and chunk ownership) is plain thread-affine memory. There is
no atomic access and no internal lock. Resource counters and cancellation retain ADR 0021's relaxed
ordering because they publish no chunk or operator state. A future scheduler must release-publish a
complete task/pipeline owner and acquire it before invoking `next` on another thread.

## Complexity and failure behavior

An accounted-wrapper construction and operator step are `O(1)` excluding their transformation.
Boolean filtering is `O(S)` for `S` selected rows. Column-subset validation and compaction are
`O(P + C)` for `P` projected and `C` removed columns. Both use `O(1)` additional memory. A virtual
call and LIMIT truncation are `O(1)` per chunk. Factory allocation or an oversized retained
projection plan is `RESOURCE_EXHAUSTED`; validation failures release the input chunk and reservation
without durable or external effects.

## Verification and measurement

Unit tests cover ownership, credit coverage/release, explicit end, sticky completion, cancellation,
hostile predicate configuration, projection bounds/order/range, zero-column output, and buffer
release. Deterministic properties compare filtering against scalar SQL truth across varied chunk
boundaries and verify projected rows and NULL/Boolean cells for all 256 eight-row selection masks.
LIMIT tests cover zero, empty progress, partial and exact boundaries, UINT64 maxima, early future
credit release, failure, and cross-query ownership. A deterministic property compares every limit
around a fixed multi-chunk input with the scalar prefix. Fuzzing drives filtering, truncation, and
projection with valid and hostile ordinals under sanitizers.

`chronos_query_benchmarks` measures selection construction plus filtering at 64, 1,024, and 4,096
rows with TRUE densities of 100%, 25%, and 6.25%. It also isolates ownership compaction and release
for 1, 8, and 64 input columns at 1,024 and 4,096 rows; input construction is paused and therefore
excluded. Batched selection-truncation measurements cover dense and sparse inputs, zero, partial,
and no-op limits with setup and destruction paused. These measurements do not claim end-to-end query
speed or justify fusion, branch specialization, or physical materialization.

## Tradeoffs and next steps

Pull and unique child ownership make correctness visible but do not exploit cores. Keeping filtered
rows as a selection avoids copies but may reduce locality after very selective predicates. Both are
deliberate pre-measurement choices.

The next increment should define the pre-allocation accounting contract needed by a typed physical
expression/output builder, or a storage scan adapter with exact snapshot pins. Parallel scheduling
should follow only after task ownership, queue capacity, terminal-error arbitration, and
cancellation release are specified.

## Likely review questions

**Why return empty chunks?** Empty selection is a valid result of a processed input chunk. Treating
it as end would truncate later input.

**Why does the reservation not shrink after filtering?** Stable compaction retains the same vector
capacity and all physical columns. Logical bytes shrink, but retained memory does not.

**Why does the reservation not shrink after projection drops buffers?** The reservation remains a
conservative owner charge and never undercounts retained buffers. Returning a partial credit would
need a separately specified resize/transfer contract; it is not required for safety or boundedness.

**Why require increasing projection ordinals?** This stage models scan/projection pushdown and can
therefore move existing owners in place. Duplicate or reordered SQL outputs require explicit new
output positions and belong in the typed builder.

**Why forward empty chunks through LIMIT?** They record completed upstream input but consume no
result cardinality. Treating an empty chunk as end would lose later rows.

**Why destroy the child when LIMIT is satisfied?** A sequential uniquely owned pipeline has no more
demand. Destruction is the normal ownership operation that releases unpulled buffered chunks and
their credit; cancellation is reserved for errors or caller stop.

**Why virtual dispatch?** It gives the first composable operator boundary at one call per chunk.
Profiles must justify devirtualization or fusion later.

**Can two workers pull the same operator?** No. The instance is thread-affine; parallelism will use
separate tasks/pipelines or an explicitly synchronized scheduler owner.
