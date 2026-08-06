# Vector Chunk Foundation

## Purpose and phase boundary

The first Phase 9 increment gives `chronos_query` a bounded physical data object without claiming a
vectorized SQL engine. `VectorChunk` combines canonical identity-free physical columns with one
explicit order-preserving selection. It is intended as the common input/output currency for later
scan, expression, aggregate, join, and scheduling work.

This increment does not lower bound plans, scan snapshots or CSEG parts, evaluate expressions,
schedule tasks, reserve query-wide memory, cancel work, spill, or replace the Phase 8 scalar
executor. Those remain separate correctness gates.

## Public interfaces

`chronos/columnar/column_vector.hpp` now exposes `OwnedPhysicalColumn`. It owns the same immutable
validity, offset, and value buffers as `OwnedColumnVector`, but has no `ColumnId`. Its factory uses
the existing `PhysicalColumnView` validator and exposes safe borrowed views and cells.

`chronos/query/vector_chunk.hpp` exposes:

- `VectorSelection`, a move-only owner of strictly increasing UINT32 physical row ordinals;
- `VectorChunkLimits`, finite row, column, logical-byte, and retained-byte limits; and
- `VectorChunk`, a move-only owner of zero or more equal-length physical columns plus one selection.

Factories return `common::Result<T>`. Invalid shape is `INVALID_ARGUMENT`, a configured bound is
`RESOURCE_EXHAUSTED`, and a bad selected row or column access is `OUT_OF_RANGE`.

## Representation and invariants

Every physical column remains in the canonical Columnar Batch v1 memory representation: packed
validity and Boolean bits, little-endian fixed values, UINT32 variable offsets, concatenated
variable bytes, zero null slots, valid text, and bounded decimal coefficients. The query chunk does
not serialize a batch header or descriptor and is not a durable format.

A chunk has a nonzero `physical_row_count`. Each column has exactly that count. A column-free chunk
is valid because relational cardinality exists independently of projected payload. The selection
may be empty, but every selected ordinal is unique, in range, and greater than its predecessor.
Consequently selected access preserves scan/filter order.

The identity selection contains `0..physical_row_count-1` explicitly. This keeps the initial
traversal contract uniform. It may later gain a measured dense representation without changing
the logical sequence returned by `indices()` or selected-cell access.

## Ownership and lifetime

Columns, selection ordinals, and their allocation capacity belong to the chunk. The chunk is
move-only and exposes no mutating accessor. `cell()` returns a borrowed `ColumnCellView`; byte cells
remain valid only while the same unmoved chunk remains alive. A move or destruction invalidates
outstanding cells and column views.

Concurrent const reads are safe because all retained buffers are immutable. No publication or
memory-ordering argument is needed inside the object. A future scheduler must transfer owning
chunks or retain an owner across every task; it cannot enqueue a borrowed cell or view alone.

## Memory bounds

`buffer_bytes()` adds exact column buffer sizes and selected-index bytes with checked arithmetic.
`retained_buffer_bytes()` performs the same accounting with vector capacities. The factory rejects
zero limits, row/column overflow, logical-byte excess, and retained-byte excess before returning
the owner.

These counters deliberately exclude object layout, `std::vector` control blocks, allocator
metadata, operator hash tables, snapshot pins, and scheduler queues. They are a local admission
check, not a total query-memory promise. The future memory manager must reserve before allocation
and charge all those additional domains.

The default 2,048-row and 32 MiB limits are conservative starting values. Callers and differential
tests can force any finite smaller or larger limits. SQL results and errors must not depend on how
the same physical input is divided into chunks.

## Failure behavior and complexity

Selection validation and identity construction are `O(R)` for `R` selected or physical rows.
Chunk shape and accounting are `O(C)` for `C` columns. Selected-cell access is `O(1)`. Construction
does not copy column buffers; ownership moves after their independent validation. Identity
selection performs one allocation, while `from_indices` adopts the caller's allocation after
validation.

No failed factory mutates durable or external state. Allocation and container length failures
during identity selection become `RESOURCE_EXHAUSTED`. Existing physical-column construction
retains its canonical validation errors.

## Verification and measurement

Unit and fixed-seed property tests cover selection order, null-preserving selected access,
column-free cardinality, row mismatches, configured limits, retained spare capacity, and boundary
partitions through 257 rows. The refactored physical owner is also exercised through every existing
columnar batch, codec, CSEG, head, ingest, and Manifest test.

`chronos_vector_chunk_fuzz` feeds arbitrary bytes into selection construction and valid Boolean
chunk access. `chronos_query_benchmarks` measures selection construction and checked selected-cell
traversal at 64, 1,024, and 4,096 physical rows with 100%, 25%, and 6.25% density. Benchmark results
must follow the repository benchmark contract and do not establish a product throughput claim.

## Tradeoffs and next steps

The representation prioritizes one obvious correctness path. Explicit dense indices cost memory;
checked cell access is not a specialized arithmetic kernel; and immutable exact buffers require
future output builders to construct a new owner. Those costs are visible and benchmarkable.

The next Phase 9 boundary should define query-wide memory reservation and cooperative cancellation
before introducing parallel operator scheduling. Physical operator and plan contracts can then
state who owns chunks, reservations, snapshot pins, task completion, and failures.

## Likely review questions

**Why no `ColumnId`?** A computed expression has a bound plan slot and logical type, not a durable
catalog identity. Inventing an ID would blur catalog and execution semantics.

**Why allow zero columns?** Cardinality-only operators still process rows. Requiring a dummy value
column would waste memory and create false semantics.

**Why require increasing selection ordinals?** A filter must preserve input order and multiplicity.
Sort, gather, and join are explicit operations with their own output contracts.

**Does the chunk memory count bound a query?** No. It bounds retained canonical buffers and indices
inside one chunk. Operator state, queued chunks, allocator overhead, and pins require the future
query-wide memory-admission design.
