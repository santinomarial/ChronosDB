# Mutable-Head Scan Source

## Purpose and boundary

`HeadScanOperator` converts one acquire-observed `HeadSnapshot` into bounded canonical physical
chunks. It is the query bridge for one active or sealed mutable-head generation: exact snapshot
row boundaries stay pinned, schema evolution is projected, memory is admitted before allocation,
and output can flow through the existing filter/projection/LIMIT operators. The exact event-time
factory automatically retains and removes an unrequested event-time helper.

This is not by itself a complete tablet scan. By default it exposes projected user columns only; callers
may opt into the shared non-null WAL ID, record sequence, row ordinal, and operation suffix from
[ADR 0045](../adr/0045-shared-vector-row-version-suffix.md). The source does not concatenate multiple
heads, combine heads with parts, resolve versions, evaluate non-event-time SQL predicates, schedule
parallel work, or spill. The aggregate `create_snapshot_tablet_scan` factory composes every
published head with the durable subset for today's append-only operation set.

## Public interface

`chronos/query/head_scan.hpp` exposes:

- `HeadScanLimits`, containing finite `VectorChunkLimits` and the default-omit row-version mode; and
- `HeadScanOperator::create(resources, snapshot, lineage, destination_schema_id, tablet_id,
  destination_ordinals, limits)` for raw physical chunks; and
- `HeadScanOperator::create_event_time_filtered(...)`, which additionally accepts an exact
  `TimestampRangePredicate`.

Destination ordinals are unique but may appear in any caller order. The exact source schema must be
retained in the lineage. The destination must be that schema or a v1 descendant, so retained
columns preserve type/nullability and newly appended nullable columns can be synthesized as NULL.
The exact factory filters at the requested event-time output position. If event time was omitted,
it appends that destination ordinal for materialization and removes the final helper after exact
selection. Caller output order and zero-column cardinality are preserved.
If row-version mode is append, helper removal also preserves the suffix after caller user columns.

## Why materialization is required

```text
published HeadSnapshot                         canonical VectorChunk
----------------------                         ---------------------
validity: uint8_t per row        pack bits      validity: LSB bitmap
BOOL:     uint8_t per row       ----------->    BOOL:     LSB bitmap
offsets:  native uint32_t       rebase + LE      offsets:  LE byte array
fixed:    canonical bytes          copy          fixed:    canonical bytes
values:   published prefix        slice          values:   chunk-local bytes
```

The head representation prevents adjacent-row writer/reader races. The query representation is
compact, immutable, and portable. Direct borrowing would violate at least one of those contracts,
especially for a chunk beginning in the middle of a variable-width column.

For each chunk, variable offset zero is rebased to the first selected row's source offset, every
offset is written explicitly little endian, and only bytes through the captured final frontier are
copied. Nullable fixed and Boolean null values remain canonical zero. `OwnedPhysicalColumn::create`
revalidates the result, including UTF-8, decimal, null-slot, offset, and bitmap rules.

## Ownership, accounting, and pull lifecycle

Creation charges the complete conservative `HeadSnapshot::retained_buffer_bytes()` before moving
the snapshot into source state. That count covers the fixed-capacity generation and exact
publication descriptor, not merely visible rows. Projection entries, ordinals, source objects, and
allocator allowances are charged too.

One pull chooses at most `limits.chunk.maximum_rows`, plans selection and canonical column bytes
without allocation, checks logical/retained limits, and reserves output credit. It then allocates
and validates owned columns and an identity selection. Output owns every byte and therefore needs
no head pin. On the final successful pull, source state and its generation charge are released
before the chunk is returned.

Append mode additionally plans and copies 29 non-null metadata bytes per row into UUID, UINT64,
UINT32, and UINT8 columns. These buffers follow user columns, use canonical little-endian integer
storage, and are included in width, byte, retained, allocation, and query-credit limits.

The exact factory wraps this source with `TimestampRangeFilterOperator` and, when needed,
`ColumnSubsetOperator`. Filtering may return an empty progress chunk; it never skips a later head
chunk. Helper removal releases direct-owned helper buffers while the existing output reservation
remains a conservative charge until the result is destroyed.

An empty projection still emits identity selections with the correct cardinality. An empty head
validates normally and returns sticky end on its first pull. Wrong-query use, local validation or
allocation failure, and output-limit failure return no chunk and request cooperative cancellation.
A pre-cancelled pull does no materialization and retains source ownership until normal unwind.

## Snapshot and concurrency argument

`HeadSnapshot` owns the exact publication descriptor observed by an acquire load. The writer's
release publication occurs after every row byte and offset is initialized. The source reads only
the captured `row_count` and variable frontiers. Later appends write outside those ranges and may
publish a newer descriptor, but cannot move the generation storage or enlarge the old snapshot.

The operator itself is thread-affine and adds no synchronization. Future scheduler publication of
the owning source/chunk needs its own release/acquire edge; query resource atomics are control state
only.

## Failure behavior

Invalid tablet, source/destination schema, ordinal, duplicate projection, or zero limits fail
before source adoption. A query budget too small for the complete head pin fails creation. Output
budget and logical/retained chunk limits fail before canonical buffer allocation. Unexpected head
shape contradictions return `INTERNAL`; allocation/container failures return
`RESOURCE_EXHAUSTED`.

An exact request validates the caller projection first, then checks the effective projection with a
possible helper against `maximum_columns`. A missing helper slot fails before query reservation.

All paths are in-memory and side-effect free. No failure changes the head, its publication, WAL,
Manifest, schema lineage, or reclamation state.

## Complexity and measurement

For `C` requested columns, `R` visible rows, and chunk width `W`:

- source validation is `O(C)` plus lineage projection construction;
- each pull is `O(C × W + copied variable bytes)` and owns canonical output bytes;
- exact event-time filtering adds `O(S)` comparisons for `S` selected input rows;
- the source retains the complete head generation; and
- at most one output chunk is produced per pull, so downstream demand bounds in-flight output.

Properties compare all frozen logical types and varied row/chunk boundaries with `HeadSnapshot`
cells. Exhaustive allocator injection covers source and pull allocations. The head-scan fuzzer
varies raw/exact factories, projections, limits, bounds, schema tails, cancellation, and pulls.
`materialize_one_head_chunk` measures four-column canonicalization, while
`materialize_and_exact_filter_one_head_chunk` measures label/event-time materialization, a point
predicate, and helper removal at 64, 1,024, and 65,536 rows. Both report allocations and bytes and
are microbenchmarks, not product throughput claims.

## Tradeoffs and next steps

Copying costs bandwidth but makes ownership, endian conversion, chunk sizing, and publication
safety explicit. A sealed-head compact backing could reduce copies only after measurement and a
new immutable-publication/accounting proof.

The common system-column shape and exact append-only aggregate source now exist. Future correction
and delete operations still need accepted base/delta row-version winner rules before they can enter
that source.

## Likely review questions

**Why not use `HeadColumnView` as a `PhysicalColumnView`?** Their bitmap and offset representations
are intentionally different, and a variable slice needs rebased offsets.

**Why charge the whole head when only a few rows are visible?** The snapshot pins fixed-capacity
generation allocations. Reclaiming that generation waits for the final pin, regardless of its row
boundary.

**Why does the output not retain the head?** Every canonical buffer is copied into owned columns;
no returned view points into head storage.

**How is the complete tablet scanned?** `create_snapshot_tablet_scan` combines the ADR 0028 durable
child with every sealed and active head from the same aggregate publication. This remains exact only
for the currently accepted append-only operations.

**Does head filtering prune materialization work?** No. The head has no accepted zone map. Exact
truth runs on bounded canonical chunks after materialization.
