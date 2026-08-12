# Tablet-state vector query pipeline

## Purpose and interfaces

The tablet-state pipeline adapts one or more `ingest::TabletSnapshot` values to the ordinary checked
physical SQL pipeline. Its input is a nonempty stable set of unique tablet publications for one
table, the table's retained schema lineage, the destination schema identity, and a
`PhysicalPipelinePlan` created by SQL binding/lowering. The single-tablet API delegates to this
table-wide boundary.

For each tablet, the source scans sealed heads in retained order and the active head last. It does not bind SQL,
select schemas, or merge durable CSEG data. Each head uses the established `HeadScanOperator`, so
canonical vector chunks, nullable successor tails, row-version suffixes, and query memory credit
retain their existing semantics.

## Ownership, memory, and failure

Each `TabletSnapshot` pins its publication while source construction copies each `HeadSnapshot` pin.
The serial source owns its child operators and one `QueryMemoryReservation` charged for finite
configuration. It destroys a child as soon as that generation reaches end-of-stream. Failure or a
foreign resource context requests query cancellation, releases the remaining children/credit, and
returns no partial success status.

The physical plan is instantiated once after source concatenation. That ordering is essential:
global aggregates, order, latest resolution, and limits observe the union of tablets and generations
rather than independent per-tablet or per-generation results.

## Complexity and review questions

Construction is O(tablets log tablets + columns + generations), including sorted duplicate-ID
validation. Each visible row is materialized once by the head scan;
the serial adapter adds O(1) work per returned chunk and one transition per generation. No full
tablet copy is created.

Reviewers should ask whether every generation uses the identical destination schema and hidden
suffix, whether the plan is above rather than below concatenation, when each publication pin is
released, and whether cancellation frees the configuration reservation. Performance work should
measure chunk sizing and generation-count overhead rather than infer gains from architecture alone.
