# ADR 0027: Snapshot-Bound CSEG Images and Part-Lifetime Pins

- **Status:** accepted
- **Date:** 2026-08-06
- **Owners:** ChronosDB manifest, reclamation, storage-snapshot, and query-scan maintainers

## Context

ADR 0019 requires every consumer that may open a selected CSEG file to retain the snapshot that
authorizes it. ADR 0026 then requires the query scan pin to own exact immutable bytes and the
applicable database lifetime owner. The existing `LoadedPartImage` is intentionally a temporary
compaction input: it owns bytes but has no aggregate-snapshot provenance or reclamation pin.

The existing reclamation proof also tracked one weak pointer to the immediate predecessor
`DatabaseStoragePublication`. A tablet-only refresh creates a new publication object under the same
Manifest generation without retaining every older publication object. Therefore an older reader
could still name a compaction input after the immediate predecessor expired, allowing the file to
be unlinked before that older reader opened it. Generation or publication identity alone is too
coarse; lifetime follows each selected immutable part through every epoch that retains it.

## Decision

- Every part selected into an aggregate database publication has one private in-memory lifetime-pin
  identity. Tablet-only refreshes and Manifest successors carry the same shared pin for every exact
  retained part. Newly selected parts receive new pins.
- Compaction retirement stores one weak pin for every removed input. `is_pinned()` remains true
  while any publication epoch that names any input remains live. The successor excludes those pins,
  so after the final old snapshot/token/image releases them they cannot be reacquired.
- `ManifestStorage::load_snapshot_part_images` accepts one owning `DatabaseStorageSnapshot`, a
  strictly sorted nonempty part-id set, retained schema bindings, and ordinary part-validation
  limits. It does not require that snapshot's generation to remain the namespace maximum.
- Loading scans the locked namespace, requires every requested identity to be selected by the exact
  snapshot and still exist as a regular final part, reads the descriptor's exact length, and repeats
  complete CSEG/Manifest/WAL/schema validation. The input snapshot pins the part throughout that
  operation.
- Each returned move-only `SnapshotPartImage` owns the exact bytes, descriptor, database identity,
  WAL identity, snapshot generation, and a copy of the exact publication retention token. It is
  therefore independently safe after the snapshot, publisher, storage owner, and file descriptor
  used to create it are gone.
- Snapshot, Manifest, decoded-descriptor, mutable-head, and image owners expose conservative
  retained-memory accounting. An image charge includes its complete aggregate publication pin and
  owned image/object allocation. Independent images deliberately duplicate the shared epoch charge
  so no query output can outlive uncharged pin credit. On-disk part lengths are not memory bytes.
- `pin_snapshot_cseg_part` converts only this trusted storage owner into the generic `CsegPartPin`.
  `create_snapshot_cseg_scan` first validates descriptor/tablet/source-schema/destination-table
  agreement, then delegates page integrity, schema evolution, admission, and pull behavior to the
  existing single-part CSEG scan source.

No durable CSEG, Manifest, WAL, schema, checksum, filename, dependency, or network format changes.
This decision does not compose multiple parts, scan mutable heads, apply pruning, resolve versions,
or lower a bound SQL plan.

## Detailed rationale

Per-part pins preserve the real invariant across both dimensions of aggregate publication: mutable
tablet epochs may change without a Manifest change, and Manifest generations may retain most old
parts while adding or replacing others. A weak pointer to one whole publication misses older
same-generation epochs; a weak pointer to a loaded Manifest can be held by non-reader storage
owners and unnecessarily block reclamation. One stable pin per immutable part is exact and remains
an in-memory policy rather than durable metadata.

Loading a predecessor snapshot is required for long queries. Requiring the generation to remain
current would turn a valid pinned snapshot into an availability error as soon as compaction
published. Conversely, accepting only a descriptor without the owning snapshot would permit a
reclamation race between descriptor capture and file open. The loader's snapshot parameter closes
that interval and the returned image extends it.

Full-epoch memory charging is conservative because several selected images share Manifest and head
owners. It is nevertheless auditable and compatible with the existing query budget. A future
shared-credit transfer may reduce duplication only if every surviving chunk still has sufficient
pin credit and cancellation cannot release it early.

## Alternatives considered

- **Keep the immediate-publication weak pointer:** fails for an older tablet-refresh epoch that
  selects the same input part.
- **Use one weak loaded-Manifest owner:** is safe only if every descriptor consumer holds that exact
  owner and can remain pinned by unrelated storage metadata owners; it is not part-granular.
- **Load by descriptor without a snapshot:** permits compaction reclamation between descriptor
  capture and file open.
- **Require the snapshot generation to remain current:** rejects valid stable readers after any
  later publication and defeats the retention contract.
- **Memory-map files immediately:** could avoid the byte copy, but needs mapping lifetime,
  filesystem/provider accounting, eviction, and platform evidence not supplied by the current
  in-memory storage API.
- **Charge the shared epoch only once globally:** needs transferable or divisible query credit that
  the current RAII reservation API does not provide.

## Consequences

Snapshot-bound readers can safely load and scan an old selected part after a newer Manifest is
published. Reclamation now waits for every epoch that actually names each removed input, including
tablet-refresh predecessors and returned query chunks. Publication carries one shared pointer per
selected part and creates one small control block per newly selected part.

The first adapter remains single-part and copies the final file into owned memory. Duplicate
full-epoch charging can lower concurrency for wide snapshots. Aggregate part/head ordering,
pruning, source scheduling, file-mapping providers, and shared snapshot-credit policy remain later
Phase 9 work.

## Affected invariants

This decision directly supports invariants [2, 3, 6, 10, 11, and
18](../architecture/invariants.md). Only snapshot-selected, fully revalidated immutable bytes reach
the reader; every descriptor/file/page view has one transitive owner; reclamation cannot pass a
live per-part pin; and query credit remains at least the retained-memory report.

## Validation plan

- Hold an older tablet-refresh publication across compaction and require the removed input's
  retirement to remain pinned after the immediate predecessor is gone.
- Load an old snapshot's part after a newer generation becomes namespace maximum; reject empty,
  duplicate, unknown, missing-schema, missing-file, corrupt, and length-mismatched requests.
- Move the image into a scan, destroy the original snapshot/publisher/storage source, return the
  final chunk, destroy the operator, and prove cells and reclamation ownership remain valid until
  the chunk releases.
- Reject schema/tablet mismatches without reservation and reject a budget below the complete image
  plus epoch charge before metadata or page decode.
- Run ordinary, ASan/UBSan, and TSan publication/storage/query suites. Existing publication and
  reclamation microbenchmarks cover pin carry/check overhead; the existing CSEG scan benchmark
  covers the delegated pull path, so the thin validation adapter does not receive a fabricated
  throughput claim.

## Migration or rollback considerations

There is no durable migration. Rollback of the loader and query adapter is safe. Rolling back only
the per-part lifetime repair while retaining part reclamation is unsafe because older
same-generation snapshot epochs can again outlive the weak immediate predecessor.

## Unresolved questions

Aggregate snapshot planning, deterministic multi-part/head order, event-time pruning composition,
head vector materialization, portable mapped-file providers, shared pin credit, query-level pin
quotas, and asynchronous storage loading remain later Phase 9 work.

## References

- [ADR 0019](0019-rebuildable-pruning-delta-planning-and-part-reclamation.md)
- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0024](0024-lifetime-pinned-vector-chunk-backing.md)
- [ADR 0026](0026-pinned-in-memory-cseg-scan-source.md)
- [Manifest installation and publication](../architecture/manifest-installation-and-checkpointing.md)
- [CSEG pruning and reclamation](../architecture/cseg-pruning-delta-and-reclamation.md)
- [Snapshot-bound CSEG loading](../learning/snapshot-bound-cseg-loading.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
