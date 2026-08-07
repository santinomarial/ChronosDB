# Pruned Multi-Part Snapshot CSEG Scan

## Purpose and phase boundary

This Phase 9 layer turns one exact aggregate database snapshot into bounded durable CSEG work for
one tablet. It prunes provably disjoint Manifest parts before file I/O, prunes provably disjoint
CSEG granules before page work, loads only the selected immutable images, and emits their chunks
through one query-accounted sequential source. A planned event-time range is then evaluated exactly
on candidate rows before the caller-visible projection is returned.

It is deliberately a CSEG-only source. A `DatabaseStorageSnapshot` may also expose active or sealed
mutable-head rows; this layer neither reads nor hides them and therefore cannot represent a complete
tablet scan while such rows are visible. It also does not perform general SQL filtering, merge
overlapping parts, resolve row versions, lower bound SQL, schedule parallel work, or spill.

## Public interfaces

`chronos/query/database_cseg_scan.hpp` exposes three stages:

- `plan_snapshot_cseg_part_scan` creates a move-only `SnapshotCsegPartScanPlan` for one tablet and
  exact database/WAL/generation epoch. Caller limits bound inspected parts, selected parts, and
  retained plan configuration.
- `load_snapshot_cseg_part_scan_images` re-proves that the plan still describes the supplied
  snapshot, then asks `ManifestStorage` to load and fully validate exactly the selected part IDs.
  Provider-owned `SnapshotPartImage` values keep the aggregate publication and per-part reclamation
  pins alive.
- `create_snapshot_cseg_part_scan` validates destination/source schemas and exact ordered image
  coverage, reserves query credit, eagerly creates one ordinary or event-time-pruned child per
  image, composes the children sequentially, and applies exact event-time truth. When the requested
  projection omitted event time, it appends that destination ordinal for child scans and removes it
  after filtering.

`CsegScanOperator::create_event_time_pruned` is the page-work boundary. It authenticates metadata,
owns a bounded `CsegEventTimePruningPlan`, and visits only selected physical granule ordinals.

## Two-stage pruning followed by exact truth

An optional event-time predicate is a conjunction of lower and upper bounds. Reversed bounds, or
equal bounds with an open side, are valid empty predicates. At both pruning levels the same range
intersection rule is used:

1. Manifest descriptor extrema select candidate part IDs without opening disjoint files.
2. Authenticated CSEG granule extrema select candidate granule ordinals without validating or
   decoding disjoint page bodies.

Stored minima and maxima are evidence for exclusion only. Every selected granule is decoded in
full, then `TimestampRangeFilterOperator` applies exact row truth using mechanically copied endpoint
values and inclusive bits. This separation prevents false negatives while preserving the existing
selective page-integrity contract. The low-level pruned CSEG source still returns complete candidate
granules; the aggregate factory does not expose those false positives.

## Ownership and lifetime

```text
DatabaseStorageSnapshot
  └── SnapshotCsegPartScanPlan (owned IDs and predicate; no file bytes)

ManifestStorage selected load
  └── shared SnapshotPartImage[]
        ├── immutable validated CSEG bytes
        ├── exact descriptor and epoch provenance
        └── publication plus per-part retention token
                  │
                  ▼
SequentialSnapshotCsegScan
  ├── parent query reservation
  └── eager CsegScanOperator children in Manifest PartId order
        │
        ▼
TimestampRangeFilterOperator (when planned)
  └── ColumnSubsetOperator (only when a final event-time helper must be removed)
        └── output AccountedVectorChunk retains its own image/pin and credit
```

Planning configuration is bounded caller work and is not adopted by a query. Loaded images are
storage-provider owners. Query construction reserves the parent container before allocation; every
child reserves its complete source/image charge before adoption. The current resource API has no
divisible shared credit, so independent children and returned chunks conservatively repeat the
aggregate epoch charge. This can reject safe work early but cannot undercount live providers.

One operator instance is thread-affine. Returned chunks are independent owners and can outlive the
parent source. Child completion releases that child immediately. Final end destroys remaining
container capacity, releases parent credit, and remains sticky.

## Validation and failure behavior

Planning rejects unknown tablets, corrupt tablet part ranges or extrema, overflowed row metrics,
zero limits, and any configured bound exceeded. Loading rejects a plan from another
database/WAL/generation or any selected-ID disagreement before filesystem access. Selected files
still pass full storage validation; missing or corrupt selected authority is never hidden by
granule pruning.

Source construction validates the destination table and projection even for an empty plan. The
image vector must cover every planned ID exactly once and in order; null, reordered, missing,
foreign-epoch, wrong-schema, or wrong-tablet images fail before query reservation. Allocation and
budget failures are `RESOURCE_EXHAUSTED` and unwind every parent/child reservation. Pull corruption
or child failure requests cooperative cancellation and returns no partial step. A foreign resource
context is rejected and cancelled.

When a predicate requires an unrequested event-time helper, the effective projection including that
helper must fit both the projected-reader and output-chunk column limits. The helper is always the
last child output, exact filtering uses that position, and stable prefix projection prevents it
from leaking. If event time was requested, filtering uses its caller-visible output position and no
projection is rewritten.

No failure mutates storage, publication state, reclamation records, or schema state.

## Complexity and measurement

For `P` tablet parts, planning is `O(P)` time and `O(S)` owned identities for `S` selected parts.
Loading costs the locked namespace scan plus selected file bytes and complete selected-file
validation. Eager source creation is the sum of selected metadata/projection opens. A pull skips
ended children and otherwise has the selected single-granule decode plus `O(S)` exact comparisons
for `S` selected input rows; no page body from a pruned granule is touched.

Deterministic properties compare part, granule, and exact row selection with independent range
models. Hostile tests corrupt pruned files and pruned page bodies, reject foreign/reordered images,
exercise successor-schema helper removal, and force budget and allocator failures.
`chronos_cseg_scan_fuzz` varies ordinary and prune-then-exact decoding over hostile bytes.
`benchmark_event_time_pruning` isolates metadata-plan cost, while
`scan_one_selected_granule_among_many` and `scan_one_exact_row_among_many_granules` distinguish
candidate decode from exact filtering under raw and Zstandard policies with source creation
excluded from timing.

## Tradeoffs and next steps

Serial eager construction is easy to audit but repeats snapshot credit and metadata-open work, and
physical `PartId` order is not a key merge. Compaction may change that physical order; SQL without
`ORDER BY` has no result-order guarantee. Owned file images are portable but copy selected files.

A separate source now canonicalizes one exact mutable-head publication. A complete tablet source
still needs shared hidden-system columns, all-head one-snapshot composition, exact head predicate
lowering, row-version and base/delta merge rules, and a scalar differential oracle. Parallelism
requires reviewed task ownership, bounded queues, terminal-error arbitration, and pin/credit
transfer before replacing this serial source.

## Likely review questions

**Why is a corrupted pruned file not opened?** Its checksum-authenticated Manifest descriptor has
already proved disjointness for this snapshot. Selected authority remains fully reread and
validated.

**Does a point predicate return only one row?** It returns every row with that timestamp, which may
be zero, one, or many. The low-level pruned child decodes complete intersecting granules; the
aggregate factory's exact [timestamp-range filter](exact-timestamp-range-filter.md) removes all
nonmatching rows before output.

**Why validate projection on an empty selection?** Invalid query configuration must not become
conditionally valid because current metadata happens to select no work.

**Why not call this a tablet scan?** Snapshot-visible mutable heads are outside this source. The
independent single-head source does not yet provide the hidden metadata or merge semantics needed
to compose them without omissions or duplicates.

**Why not merge parts by event time?** Overlapping base/delta parts need explicit row-version and
tie-break semantics plus differential validation. Concatenation makes no stronger promise.
