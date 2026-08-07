# ADR 0024: Lifetime-Pinned Vector Chunk Backing

- **Status:** accepted
- **Date:** 2026-08-06
- **Owners:** ChronosDB query-execution, columnar, and storage-scan maintainers

## Context

ADR 0020 initially makes every `VectorChunk` directly own `OwnedPhysicalColumn` values. That is an
appropriate construction boundary for computed output, but the accepted CSEG projected reader has
a different safe ownership shape: decompressed and synthesized pages belong to one
`ProjectedCsegGranule`, while raw pages borrow an immutable encoded part image that must stay
pinned. Copying every projected page into a second set of owned column buffers would preserve
lifetime but add unavoidable bandwidth and transient memory before the first storage scan exists.

Physical operators only need immutable canonical `PhysicalColumnView` values and one owner that
keeps them valid. Phase 9 therefore needs a storage-independent lifetime contract before wiring a
CSEG scan source. It must not let borrowed views escape their owner, make projection release bytes
that remain pinned, or detach query credit from the backing lifetime.

## Decision

- `VectorChunkBacking` is an immutable polymorphic lifetime owner. It exposes a stable physical
  column count, bounds-checked nullable column pointers, logical buffer bytes, and retained buffer
  bytes. Implementations include every canonical buffer in the logical count and conservatively
  charge retained allocation capacity, non-buffer backing storage, and external pins in the retained
  count. Allocator metadata may be covered conservatively because its exact size is not portable.
  Every returned view remains valid and immutable for the backing lifetime.
- `VectorChunk::create_backed` retains one `shared_ptr<const VectorChunkBacking>`, validates every
  view and equal row shape, verifies that the backing does not underreport visible bytes, and owns
  an ordered ordinal map plus the existing selection. A null backing, missing view, shape mismatch,
  or underreported accounting is `INVALID_ARGUMENT`; configured bounds and allocation failures are
  `RESOURCE_EXHAUSTED`.
- `VectorChunk` remains move-only. Its public column boundary becomes identity-free
  `PhysicalColumnView` through `column_count()` and `column()`, regardless of whether storage is
  direct or backed. `OwnedPhysicalColumn::view()` returns a stable const reference so directly
  owned chunks do not need a second view allocation. Its custom move operations rebind that cached
  view to the destination buffers rather than assuming a container move transfers allocation.
- Stable column-subset projection over direct ownership continues to destroy discarded column
  owners and reduce chunk buffer counts. Projection over a shared backing compacts only the ordinal
  map: the complete backing remains alive, and the chunk's logical/retained backing byte charge does
  not shrink. This conservative distinction is observable only through resource counters, never
  through SQL rows or column order.
- `AccountedVectorChunk` continues to require one reservation at least as large as the chunk's
  retained count. The backing pointer, selection, ordinal map, and reservation move together, so
  releasing the accounted chunk releases the query credit and its backing pin through ordinary
  RAII.
- A backing is fully constructed and immutable before chunk creation. `shared_ptr` lifetime
  management is not a data-publication primitive; an operator pipeline remains thread-affine, and a
  future scheduler must publish the complete owning chunk with its own release/acquire mechanism.
- Backings must not own the `VectorChunk` that references them. This prevents an ownership cycle.
  They may share external immutable storage/schema pins whose reclamation contract is separately
  defined.

This decision refines ADR 0020's direct-ownership restriction; its canonical physical shape,
selection, bounds, and identity-free decisions remain accepted. No durable or network format
changes, and no dependency is added.

## Detailed rationale

One lifetime owner matches how decoded storage naturally groups pages and lets a raw page remain a
zero-copy view into an immutable part. Returning only `PhysicalColumnView` prevents operators from
depending on the concrete owner. Validating reported counters against visible buffers catches the
dangerous undercounting cases at the chunk boundary, while a conservative backing may report more
bytes than its visible projection.

Keeping the full backing charge after projection is less memory-efficient than splitting owners,
but it is truthful: destroying an ordinal cannot reclaim a decompressed page held in the same
granule object or unpin its encoded part. Future scan planning can request only needed columns to
avoid retaining them in the first place.

## Alternatives considered

- **Copy every storage page into `OwnedPhysicalColumn`:** is lifetime-safe and simple, but doubles
  page movement and can require both decoded and copied buffers simultaneously.
- **Store bare `PhysicalColumnView` values in a chunk:** avoids copies but has no enforceable owner;
  raw page or decompression storage could be destroyed before downstream access.
- **One `shared_ptr` per column buffer:** can release projected columns independently, but adds
  reference-count traffic and fragments the natural one-granule/page-group owner.
- **Make CSEG types a special case inside `VectorChunk`:** avoids a virtual interface but couples the
  query currency to one storage format and cannot represent mutable-head or computed backing.
- **Delay the decision until the complete scan operator:** would force that operator either to copy
  or to introduce an unreviewed lifetime exception at the same time as schema, pruning, and memory
  admission logic.

## Consequences

CSEG and later immutable scan sources can retain decoded/raw storage without copying solely for
lifetime. Existing owned chunks retain their move-only behavior. Operators and plans now validate
uniform physical views instead of concrete owned-column objects.

Backed chunks pay one shared-owner reference and one bounded ordinal-map allocation. Projection
cannot release a monolithic backing early, so scan projection pushdown matters. Counter correctness
still depends on trusted backing implementations; factory validation proves visible lower bounds
but cannot discover hidden allocations. Actual CSEG scan admission and snapshot-pin accounting
remain unimplemented.

## Affected invariants

This decision supports invariants [9, 10, 11, 16, and 18](../architecture/invariants.md). Backing
bytes and ordinals remain bounded; every physical view is shape checked; owner and credit lifetimes
are coupled; a chunk cannot outlive its page pin; and shared ownership does not weaken scheduler
publication requirements.

## Validation plan

- Unit tests cover null/missing/mismatched/underreported backings, exact selected-cell access,
  retained accounting, projection without false release, lifetime after the caller drops its
  handle, and coupled reservation/pin release.
- A deterministic property compares direct and backed chunks across row/selection boundaries.
- The vector fuzzer drives filtering, projection, and access through a pinned backing under
  ASan/UBSan. ThreadSanitizer exercises immutable shared lifetime and existing resource ownership.
- Microbenchmarks isolate backing attachment at representative row counts/densities without
  claiming storage-scan throughput. Public-header, installation, and external-consumer checks cover
  the generalized API.
- All columnar, CSEG, head, ingest, Manifest, and query tests must remain passing after the stable
  `OwnedPhysicalColumn` view-reference change.

## Migration or rollback considerations

There is no persisted state. The pre-alpha source API changes from an owning-column span to uniform
physical-column access. Rollback requires storage scans to copy into direct owners. Any replacement
must retain one live owner across all borrowed cells, conservative backing accounting, projection
truthfulness, and coupled query-credit release.

## Unresolved questions

CSEG scan construction, no-allocation granule planning, transient decode admission, encoded-part
pin charging, mutable-head backing, backing granularity, scheduler publication, typed output
builders, and spill ownership remain later Phase 9 decisions.

## References

- [ADR 0008](0008-custom-sql-and-vectorized-execution.md)
- [ADR 0020](0020-bounded-vector-chunk-representation.md)
- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0022](0022-pull-based-physical-operator-lifecycle.md)
- [CSEG projected reader](../learning/cseg-projected-reader.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
