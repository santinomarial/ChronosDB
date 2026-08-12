# ADR 0235: Query-Accounted Columnar Batch Source

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB columnar, query, ingest, and live-plane maintainers

## Context

Committed Columnar Append v1 input is already retained as an immutable `OwnedColumnarBatch`. The
historical query path scans mutable heads or CSEG parts, but the live plane needs to evaluate a
newly applied batch without rescanning the complete table or constructing per-row scalar objects.
No physical source exposed a batch as bounded canonical vector chunks.

The batch may exceed the vector chunk row limit, nullable and Boolean buffers are bit-packed, and
variable offsets are relative to the complete input. A source must therefore slice and normalize
each output chunk while keeping input and output ownership charged to the query.

## Decision

`ColumnarBatchScanOperator` retains one immutable schema-shaped batch and copies it into canonical
identity-selected `VectorChunk`s of at most the configured row limit. Each chunk:

- rebuilds nullable validity and Boolean bitmaps for its local row domain;
- normalizes variable offsets and copies only the selected variable bytes;
- copies fixed-width canonical bytes without native loads or type punning; and
- owns its physical columns independently of the source batch.

The first pull reserves conservative shared query credit for the retained batch and source owner.
Each output reserves separate conservative credit before materialization. Cancellation and foreign
query contexts fail before cursor advancement; successful end releases the batch and shared credit
and remains sticky.

The source exposes schema columns only. It does not fabricate WAL, record-sequence, row-ordinal, or
operation identity, so a caller may use only a physical plan whose exact input shape omits the row-
version suffix. Stateful incremental semantics remain a separate live-plan decision.

## Consequences

Already-applied append batches can enter the existing vector expression/filter/projection pipeline
without a table rescan or per-row heap allocation. Materialization currently copies input bytes once
per live evaluation; a borrowed slice representation would require new bitmap/offset lifetime and
accounting contracts and needs profile evidence.

An output-admission failure does not advance rows. Once shared source credit is acquired, later
pulls are bound to that query context. The source itself does not publish subscription changes or
define result keys.

## Validation

Focused tests split a nullable string/Boolean batch into one-row chunks, verify exact cells and
credit release, reject a foreign query, retry an initial resource failure without advancement, and
instantiate a checked `WHERE` plus projection SQL pipeline over the batch. Allocation sweeps,
all-logical-type matrices, cancellation at every column, large batches, and throughput/allocation
profiles remain deferred.

## References

- [ADR 0015](0015-columnar-batch-v1-and-wal-append-command.md)
- [ADR 0022](0022-pull-based-vector-operator-memory-contract.md)
- [ADR 0096](0096-plan-bound-subscription-snapshot-execution.md)
- [Columnar batch query source](../learning/columnar-batch-query-source.md)
