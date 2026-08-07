# Lifetime-Pinned Vector Backing

## Purpose

The seventh Phase 9 increment lets a `VectorChunk` borrow immutable canonical columns without
losing their owner. This is the ownership prerequisite for zero-copy raw CSEG pages and owned
decompression groups: a downstream operator sees the same `PhysicalColumnView` interface while one
shared backing keeps every referenced byte alive.

It does not implement a CSEG scan, mutable-head scan, scheduler publication, or output builder.
Those components can now use one reviewed lifetime boundary instead of copying solely to make a
borrow valid.

## Interfaces

`VectorChunkBacking` is an abstract immutable owner with four facts:

- number of available columns;
- a stable `PhysicalColumnView` pointer for each valid ordinal;
- logical buffer bytes kept alive; and
- retained buffer bytes, including capacity and backing-owned bookkeeping.

`VectorChunk::create_backed` adopts `shared_ptr<const VectorChunkBacking>`, validates every column
against the selection row domain, creates an identity ordinal map, and checks local limits.
`column_count()` and `column()` are now the uniform physical-column API for direct and backed
chunks. The views contain no durable `ColumnId`.

`OwnedPhysicalColumn::view()` returns a stable const reference. Its internal view contains spans to
the column's immutable vector allocations. Moving the owner transfers those allocations without
moving their byte storage; destroying or mutating the owner still invalidates the view.

## Ownership and lifetime

```text
AccountedVectorChunk
  ├── QueryMemoryReservation
  └── VectorChunk
        ├── VectorSelection
        ├── backing ordinal map
        └── shared_ptr<const VectorChunkBacking>
              ├── stable physical views
              └── page/decompression/storage owner(s)
```

Dropping the producer's `shared_ptr` is safe after chunk creation. The last chunk/backing reference
destroys the backing. When the chunk is accounted, that same unwind also returns its query credit.
A cell or physical view still borrows the chunk/backing and must not be retained by itself.

Backings must not reference their owning chunk. `shared_ptr` controls lifetime only; it does not
publish data between threads. The backing is immutable before construction, and a scheduler must
transfer the complete owning chunk through a separately synchronized queue.

## Validation and accounting

Creation rejects a null backing, an absent physical view, a row-count mismatch, a configured limit,
and arithmetic overflow. It sums visible canonical buffers and requires the backing's logical count
to be at least that sum. Retained backing bytes must be at least logical backing bytes. This catches
visible underreporting while allowing hidden system pages or pin costs to be conservatively charged.

The ordinal map's retained vector capacity is included in the chunk retained count. The existing
selection capacity and all reported backing bytes are also included. `AccountedVectorChunk`
therefore refuses a reservation smaller than the complete backed chunk count.

The interface trusts a backing to report hidden allocations honestly. It is an internal producer
boundary, not a sandbox for adversarial C++ subclasses. A CSEG scan must document how decoded,
container, raw-part pin, and transient bytes map to these counters before it can claim admission.

## Projection behavior

Direct ownership and shared ownership have deliberately different reclamation:

- direct projection move-compacts selected `OwnedPhysicalColumn` objects and destroys discarded
  owners, reducing logical and retained column-buffer counts;
- backed projection compacts only its ordinal map. The monolithic backing remains pinned, so
  logical and retained backing counts stay unchanged.

Both paths preserve the same visible column order, selection, rows, cells, and SQL semantics. The
counter difference prevents the resource layer from pretending memory was released when it was
not. Scan projection pushdown should avoid decoding/unpinning unused columns before chunk creation.

## Failure and complexity

Backing validation is `O(C)` for `C` columns. Identity ordinal-map construction is `O(C)` and one
bounded allocation. Column and selected-cell access remain `O(1)`. Backed stable projection is
`O(P)` for `P` output ordinals and allocates nothing; backing destruction cost belongs to its
implementation.

Semantic failures are `INVALID_ARGUMENT`; size, retained-limit, and factory allocation failures are
`RESOURCE_EXHAUSTED`; cell ordinal failures remain `OUT_OF_RANGE`. Failed construction releases the
shared backing normally and changes no external or durable state.

## Evidence

Unit tests use an independently owned backing to prove caller-handle release, selected-cell access,
projection accounting, invalid reports, and coupled credit/pin destruction. The deterministic
vector property compares direct and backed results through 257-row boundaries. The vector fuzzer
now executes Boolean filtering and projection through both storage modes.

`attach_pinned_chunk_backing` measures selection validation, ordinal-map allocation, shape checks,
and shared-owner attachment for 64, 1,024, and 4,096 rows at dense and sparse selection densities.
It is an ownership-overhead measurement, not a CSEG scan benchmark.

## Tradeoffs and next steps

One backing per chunk keeps lifetime auditable and matches a decoded granule, but a subset cannot
release individual pages. Per-column owners could improve that later if profiles justify their
reference-count and object overhead. The immediate next step is a no-allocation CSEG granule-read
plan so query credit can be reserved before page decode, followed by a CSEG source operator whose
backing owns both the projected granule and the encoded part pin.

## Likely interview questions

**Why not return bare spans?** A span carries no owner, so a decompression vector or mapped part can
disappear while downstream still reads it.

**Why does projection keep the old byte charge?** Removing an ordinal does not destroy the shared
backing. Lowering the charge would make accounting claim bytes were released when they remain live.

**Why use a virtual backing?** Query operators need one storage-independent lifetime interface.
CSEG, mutable heads, and future immutable compute arenas should not be variants inside the core
chunk type.

**Does `shared_ptr` make the chunk thread-safe?** No. It keeps immutable storage alive. Pipeline
thread affinity and scheduler publication are separate contracts.
