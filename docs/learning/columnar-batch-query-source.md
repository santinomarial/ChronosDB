# Columnar Batch Query Source

## Purpose and interface

`ColumnarBatchScanOperator` adapts an immutable `OwnedColumnarBatch` to the pull-based vector engine.
It is intended for already-applied committed input that needs immediate query or live-plan
evaluation. Creation takes shared batch ownership and finite chunk limits; each `next()` returns one
owned, query-accounted chunk or sticky end.

## Data and shape invariants

Input columns are already validated against the batch's retained `TableSchema`. Output keeps schema
ordinal order and the same logical type and nullability for every column. Each chunk has a local
zero-based row domain. Nullable validity and Boolean values are rebuilt as LSB-first bitmaps,
variable offsets start at zero and end at the copied value length, and fixed-width bytes remain in
their canonical representation.

The source does not expose row-version columns. Plans requiring LATEST or base-row ORDER BY identity
must use a source that owns authoritative WAL/Raft coordinates. A row-preserving live pipeline can
use this source for expressions, filters, and projection after committed application.

## Ownership, memory, and failure behavior

The operator owns a shared pointer to the batch. Its first pull reserves shared query credit for
the batch buffers plus conservative owner overhead. Output planning computes buffer sizes before
allocation, reserves independent credit, and only advances the row cursor after a complete
`AccountedVectorChunk` exists. Output chunks copy their columns and may outlive the source.

Cancellation is checked before planning and between columns. A different query context is rejected
after source admission. Any allocation or configured size failure releases local partial buffers
through RAII and leaves the current row boundary unchanged. Successful end drops the batch and
shared reservation immediately.

## Complexity and tradeoffs

Planning and materialization are each linear in rows times columns, plus copied variable bytes.
Peak query charge includes the retained input and one output chunk. Copying makes chunk ownership
simple and preserves normalized bitmap/offset contracts; borrowing arbitrary row slices would be
more complex and is not justified without profiles.

## Review questions

- Why is source memory charged separately from output memory?
- Why can this source not execute a plan that requires the row-version suffix?
- What state changes when output admission fails?
- Why are variable offsets rebuilt instead of sliced in place?
