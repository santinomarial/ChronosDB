# ADR 0028: Pruned Multi-Part Snapshot CSEG Scan

- **Status:** accepted
- **Date:** 2026-08-06
- **Owners:** ChronosDB query-execution, CSEG, Manifest-storage, and snapshot maintainers

## Context

ADR 0026 supplies a query-accounted source for one immutable in-memory CSEG part. ADR 0027 then
binds one fully validated part image to the aggregate database publication that selected it. A
tablet query still cannot safely enumerate all durable parts from one snapshot, avoid provably
disjoint work, or concatenate several part sources under one operator lifecycle.

Manifest v1 already provides authenticated table/tablet identity, canonical per-tablet `PartId`
order, row counts, and event-time extrema. CSEG v1 provides authenticated per-granule extrema. Both
levels are conservative pruning evidence, but neither applies the exact SQL predicate: a selected
range may contain nonmatching rows. Physical execution therefore needs a bounded planning and
composition layer that cannot turn optional metadata into truth.

The aggregate database snapshot can also contain mutable sealed and active heads. Their current
storage is lifetime safe but does not yet expose one canonical immutable vector backing or a
physical materialization charge. A source that silently ignored those heads while claiming to scan
the complete tablet would violate snapshot visibility.

## Decision

- `SnapshotCsegPartScanPlan` is an owned, immutable, bounded CSEG-only plan for one tablet in one
  exact `DatabaseStorageSnapshot`. It records database/WAL/generation/table/tablet provenance, the
  optional normalized event-time predicate, selected part identities, selected/skipped part and row
  counts, and conservative retained configuration bytes.
- Planning locates the exact durable tablet descriptor, enforces caller part/configuration limits,
  walks only its canonical Manifest part range, and calls
  `cseg_event_time_range_may_match()` for every descriptor. Selected identities therefore remain in
  canonical `PartId` order. No predicate selects every part; a mathematically empty predicate
  selects none.
- `load_snapshot_cseg_part_scan_images()` requires the same aggregate snapshot and exact plan,
  constructs the target tablet's retained schema binding, and delegates to
  `ManifestStorage::load_snapshot_part_images()`. It loads only selected parts and returns shared
  immutable `SnapshotPartImage` providers. Empty plans perform no filesystem read.
- Plan provenance must match the supplied snapshot before file access. Every loaded image must
  match the plan's database, WAL, generation, table, tablet, and ordered part identity before query
  adoption. The storage loader remains responsible for complete selected-file and schema
  validation.
- `CsegScanOperator::create_event_time_pruned()` authenticates metadata, builds a bounded owned
  `CsegEventTimePruningPlan`, and stores its selected granule ordinals after reserving a conservative
  maximum ordinal-vector charge. Pulls plan, reserve, validate, and decode only those ordinals.
  The ordinary `create()` path remains allocation-free with respect to pruning state and scans
  every granule.
- `create_snapshot_cseg_part_scan()` eagerly constructs one validated single-part child for every
  selected image, then returns a thread-affine sequential source. One pull advances through ended
  children and returns at most one accounted chunk. Child order is plan `PartId` order; granules
  retain CSEG physical order. Successful end is sticky, errors request cooperative cancellation,
  and returned chunks independently retain their exact image and publication pin.
- The sequential source reserves its child-pointer container, object storage, and allocator
  allowances before allocation. Every child separately reserves its full image/publication pin and
  source state before adopting that image. Independent images deliberately repeat the aggregate
  epoch charge under ADR 0027; this first correct composition does not invent shared credit.
- Part planning is configuration work bounded by `SnapshotCsegPartScanPlanLimits`, not execution
  memory. Loaded images are storage-provider owners. Query credit is acquired before those providers
  are adopted into returned physical sources or chunks.
- Pruning never filters rows. A selected part or granule is emitted in full, and an exact predicate
  operator remains required for SQL truth. Without SQL `ORDER BY`, the deterministic physical order
  is not a result-order contract and may change after compaction.

This source is explicitly CSEG-only. It does not include active or sealed mutable heads and cannot
be used as a complete tablet scan when the selected `PublishedTabletStorage` has visible head rows.
No CSEG, Manifest, WAL, schema, checksum, filename, dependency, or network format changes.

## Detailed rationale

Two-stage pruning prevents unnecessary file reads first and unnecessary page reads second. Manifest
descriptor extrema are checksum-authenticated and were bound to complete part contents when the
selected generation was loaded. A skipped file therefore need not be reopened merely to prove that
the authenticated descriptor is disjoint. Selected files are nevertheless reread and fully
validated through the snapshot-bound loader before any child source is returned.

Keeping the plan separate from image loading makes ownership and allocation order visible. The
plan can be inspected or rejected under finite configuration limits, storage can materialize the
exact selected providers, and query construction can reserve before adopting them. Eager child
construction validates all selected metadata before the first pull and keeps the initial lifecycle
simple; later asynchronous providers or morsel scheduling may replace it only with equivalent
admission and terminal-error rules.

Canonical `PartId` order is stable for one Manifest and satisfies deterministic testing and serial
execution. It is not physical key merge order: overlapping base/delta parts can interleave by event
time, and compaction can replace identities. SQL already defines unordered results as a multiset,
so no stronger promise is needed before explicit sort/merge operators exist.

## Alternatives considered

- **Treat descriptor overlap as exact filtering:** causes false positive rows to escape and changes
  SQL truth.
- **Open every file before part pruning:** detects irrelevant external damage but defeats the
  authenticated part-level index and performs avoidable I/O.
- **Pass selected granules to the query source without ownership:** lets caller mutation reorder or
  invalidate future pulls.
- **Create children lazily without charging retained images:** allows the aggregate operator to own
  unaccounted snapshot/file providers.
- **Share one aggregate reservation across all images and chunks:** requires divisible lifetime
  credit that the current RAII resource API does not implement.
- **Merge parts by event time now:** overlapping schemas, physical keys, delta roles, and stable
  row-version tie breaks require a real merge operator and scalar differential oracle.
- **Call this a complete tablet scan:** would omit snapshot-visible mutable heads.

## Consequences

ChronosDB can now plan, load, and scan all predicate-relevant durable CSEG parts for one tablet from
one stable database epoch. Disjoint parts are not opened, disjoint granules perform no page work,
and part/granule ordering, memory credit, cancellation, and chunk pin lifetimes remain explicit.

The source is serial and eager, duplicates epoch charges for every image/output, and emits complete
selected granules. Queries must still compose exact vector predicates, and callers must reject or
separately scan visible heads before claiming a complete tablet result. These costs and boundaries
are intentional until mutable-head backing, shared credit, merge/sort operators, and scheduling are
specified.

## Affected invariants

This decision supports invariants [6, 9, 10, 11, and
18](../architecture/invariants.md). Planning uses one exact snapshot; pruning cannot remove a
possibly matching range; every emitted page retains its selected-part and publication owner; query
credit precedes adopted source/output ownership; and composition changes no visibility or checksum
rule.

## Validation plan

- Unit tests prove exact snapshot/tablet provenance, canonical part order, empty/all/partial part
  plans, finite configuration limits, selected-only file loading, and rejection of reordered or
  foreign images before query reservation.
- Corrupt a part pruned by Manifest extrema and pages in a granule pruned by CSEG extrema; neither
  is touched, while corruption in selected authoritative bytes still fails through the existing
  storage/source classifications.
- Deterministic properties compare planned parts and emitted granules with independent range
  intersection models across boundary inclusivity, empty predicates, compression, and chunk
  lifetimes.
- Allocation-failure tests cover pruning-plan and sequential-source construction; the CSEG scan
  fuzzer varies event predicates and the pruned/ordinary path over hostile and authenticated
  mutated images.
- Microbenchmarks separately retain the existing metadata-pruning cost and measure selected
  granule pulls with many disjoint granules. Ordinary, ASan/UBSan, TSan, public-header,
  installation, and external-consumer checks cover the exported API.

## Migration or rollback considerations

There is no persisted state. Rollback removes the plan/loader/composition APIs and the pruned
single-part factory. Any replacement must retain exact snapshot provenance, no-false-negative
range logic, pre-page pruning, conservative provider credit, stable chunk pins, and the explicit
CSEG-only visibility boundary.

## Unresolved questions

Mutable-head vector backing and composition, exact event-time vector predicate lowering,
base/delta merge order, row-version resolution, shared epoch credit, asynchronous/mapped providers,
cross-tablet planning, morsel scheduling, typed expression outputs, explicit sort, and spill remain
later Phase 9 work.

## References

- [ADR 0008](0008-custom-sql-and-vectorized-execution.md)
- [ADR 0019](0019-rebuildable-pruning-delta-planning-and-part-reclamation.md)
- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0026](0026-pinned-in-memory-cseg-scan-source.md)
- [ADR 0027](0027-snapshot-bound-cseg-images-and-part-lifetime-pins.md)
- [CSEG pruning and reclamation](../architecture/cseg-pruning-delta-and-reclamation.md)
- [Snapshot-bound CSEG loading](../learning/snapshot-bound-cseg-loading.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
