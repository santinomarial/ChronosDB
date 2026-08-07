# Pinned CSEG Scan Source

## Purpose and phase boundary

The pinned CSEG scan source is the first Phase 9 physical operator that reads real ChronosDB
storage bytes. It turns one projected CSEG granule into one lifetime-backed, query-accounted
`VectorChunk` without copying raw pages solely for ownership.

It scans one already owned in-memory part. It does not acquire a database snapshot, load files,
choose parts, apply pruning, merge mutable heads, resolve row versions, lower SQL, schedule parallel
morsels, or spill. Those owners must compose this primitive without weakening its pin and admission
rules.

## Public interfaces

`chronos/query/cseg_scan.hpp` exposes:

- `CsegPartPin`, a copyable trusted immutable owner for exact part bytes and their conservative
  retained/pin charge;
- `CsegScanLimits`, containing projected-reader and output-chunk bounds; and
- `CsegScanOperator`, a thread-affine `PhysicalOperator` source created against one
  `QueryResourceContext`, retained schema lineage, destination schema/tablet, and destination user
  ordinals.

The query library publicly links `chronos::cseg` because the installed header and implementation use
the accepted projected reader directly.

## Ownership and lifetime

```text
CsegScanOperator state
  ├── source QueryMemoryReservation
  ├── CsegPartPin ── opaque immutable part/snapshot owner
  ├── CsegProjectedReaderView (borrows pin bytes; owns metadata/projection)
  └── destination ordinal vector

returned AccountedVectorChunk
  ├── output QueryMemoryReservation
  └── VectorChunk
        └── CSEG backing
              ├── CsegPartPin
              └── ProjectedCsegGranule
                    ├── raw views into pin bytes
                    ├── owned decompressed/synthesized buffers
                    └── mandatory system pages
```

The output does not borrow the operator. On the last successful pull the source state and its credit
are released before return, while the chunk's copied pin keeps raw cells valid. LIMIT may destroy the
source immediately and the returned chunk remains safe. A cell still borrows the unmoved chunk and
must not escape it.

`CsegPartPin` cannot prove arbitrary opaque-owner/span association. Its producer is trusted and must
retain the exact immutable allocation plus any database snapshot token required to stop reclamation.
Its reported bytes must cover that full ownership, not merely the projected pages.

## Admission and accounting

Creation reserves before metadata decode. Its conservative charge includes the part pin, complete
encoded size as an upper bound for descriptor arrays, bounded metadata-validation scratch,
destination projection entries, caller ordinal capacity, source objects, and allocator allowances.
Compile-time size checks tie decoded descriptor objects to their larger-or-equal durable
descriptors.

Each pull calls allocation-free `plan_granule`, then checks:

1. granule row count against the explicit scan chunk limit;
2. decoded canonical bytes plus identity-selection bytes against the logical limit;
3. the conservative retained output estimate against the retained limit; and
4. the query-wide budget through `reserve`.

Only then are requested pages checked/decompressed and nullable tails synthesized. The final backing
reports all requested, synthesized, and hidden system logical buffers. Its retained count adds the
part pin, owned capacities, result containers, and backing overhead. `VectorChunk::create_backed`
and `AccountedVectorChunk::create` repeat the lower-bound and credit checks before ownership escapes.

The same part pin is conservatively charged by the source and every live output. This can overcount
while both are live, but never lets a chunk outlive its pin credit. A future shared-credit mechanism
must preserve that property before reducing the charge.

## Pull, failure, and cancellation behavior

One pull returns one complete granule, error, or explicit end. User columns follow caller ordinal
order; the four system pages are validated and charged but are not query-visible columns. Empty user
projection is valid and preserves row cardinality through the identity selection.

Pre-cancelled pulls return `CANCELLED` before page work. A source used with a different query returns
`INVALID_ARGUMENT` and cancels that caller. Corruption and unsupported page semantics propagate from
the projected reader and request cancellation. Bounds and admission failures are
`RESOURCE_EXHAUSTED`. Allocation and container failures are classified the same way. No failed pull
returns a partial chunk; local reservations unwind immediately, while the live source pin remains
until ordinary source destruction.

Successful end is sticky. A final successful chunk releases source state eagerly because its backing
is now a sufficient owner.

## Complexity

Source open is `O(metadata descriptors + destination schema columns)` and touches no page body. A
pull is `O(projected ordinals + selected/system stored page bytes + physical validation)` with output
memory proportional to decompressed/synthesized buffers, selection, containers, and the pinned part
charge. Raw selected pages are zero-copy. One virtual call occurs per granule, not per row.

The scan chunk row default is 65,536 because that is the frozen CSEG granule maximum. This is an
explicit source limit, not a change to the generic 2,048-row chunk default. Smaller configured row
limits reject a larger granule before decode until a canonical slicing/gather builder exists.

## Verification and measurement

Unit tests exercise raw/Zstandard values, pin survival, source/output credit, cross-query and
cancellation behavior, corruption, pre-decode exhaustion, sticky end, and LIMIT composition. A
fixed-seed property scans varied row counts and granule boundaries under both compression policies.
A dedicated global-allocation seam fails every creation and pull allocation until success.

`chronos_cseg_scan_fuzz` combines hostile bytes/projections and canonical mutated multi-granule
images with cancellation, pulls, and cell access. `scan_one_cseg_granule` measures the pull boundary
for 64, 1,024, and 65,536 rows under raw and Zstandard policies, reporting decoded bytes and observed
allocations. Fixture construction and source open are outside the timed pull.

## Tradeoffs and next steps

The source is deliberately single-part and sequential. Duplicate pin charging is conservative;
granule-sized physical domains can be wider than an eventual execution morsel; and an opaque pin is
only as truthful as its trusted storage provider. These choices keep the first real storage/source
ownership path auditable.

The snapshot-bound adapter now pairs one validated Manifest part image with its exact aggregate
database owner and carries that pin through returned chunks. The next storage integration must
compose several such images with deterministic part/head order and safe pruning, then decide
whether epoch-wide pin credit can be shared without allowing any output to retain uncharged state.

## Likely review questions

**Why does the chunk own another part pin when the source already owns one?** The chunk may outlive
the source through LIMIT, cancellation, or caller retention. A bare view into source state would be
a use-after-free risk.

**Why charge the part more than once?** Each output has an independent sufficient reservation. This
is conservative but safe until shared reservation transfer has a reviewed lifetime model.

**Why are system pages charged when they are not output columns?** They are decoded and retained to
validate row identity and operation semantics before any user row is exposed.

**Does a successful scan validate the complete part?** No. It validates authenticated metadata and
the requested user plus mandatory system pages. Unrequested user-page corruption is outside this
selective result and cannot be used as installation evidence.

**Does `CsegPartPin` establish a database snapshot?** No. It establishes byte lifetime. Its producer
must embed the applicable snapshot/retention owner, and a higher scan plan must compose all parts and
heads from one stable epoch.
