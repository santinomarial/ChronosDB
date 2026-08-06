# ADR 0005: Columnar Heads and Immutable CSEG Parts

- **Status:** accepted
- **Date:** 2026-08-01
- **Owners:** ChronosDB storage-engine maintainers

## Context

ChronosDB must absorb columnar batches, serve recent and historical analytical projections, retain correction history, and reorganize data without blocking readers. Its primary storage cannot be an opaque key-value engine if predicate pruning, temporal ordering, column decoding, system-time visibility, and crash-safe installation are to remain first-class contracts.

## Accepted decision

Recent committed data resides in append-only mutable columnar heads. A head is mutated only by its owning shard worker. Once sealed, it is immutable to writers and remains queryable while flush produces persistent parts.

Persistent storage uses ChronosDB's versioned CSEG format: immutable, sorted, compressed columnar parts with checksummed pages and protected interpretation metadata. The physical ordering key begins with workload-relevant dimensions and event time; the precise dimensions and tie-breakers are table/format decisions, not fixed here.

Installed parts are never modified in place. Flush and compaction write new identities, validate and durably install them, then atomically publish a new manifest version. Readers pin the head/manifest generations needed by their snapshots.

The primary access path combines time partitioning, column projection, zone maps, and sparse fence indexes. A generic mutable B+ tree is not the primary event store. Secondary indexes are optional side structures accepted only for demonstrated predicates and benchmarked cost/benefit; their absence or corruption cannot change query truth.

Events arriving outside the in-memory reorder horizon are written into sorted immutable delta parts and later compacted with overlapping base/version data. Compaction resolves logical versions according to snapshot and retention rules while preserving visible rows.

## Detailed rationale

Columnar heads avoid per-row object allocation and allow recent scans to use the same broad vector shape as CSEG scans. Immutable sorted parts make compression, checksums, sparse indexing, concurrent readers, and crash-safe replacement easier to reason about than in-place mutation. Delta parts bound write amplification from late arrivals while allowing the primary sort order to remain useful.

Owning the format and manifest model exposes the exact safety boundaries needed for recovery, row-version visibility, and later Raft snapshot installation. It also permits selective decoding designed around the target workloads rather than encoding relational rows as opaque values.

## Alternatives considered

- **Row store:** favors whole-row point access and mutation but wastes projection bandwidth and complicates compression for scan-heavy workloads.
- **Mutable B+ tree:** supports ordered point/range updates, yet requires fine-grained mutation, page split recovery, and reader synchronization that do not fit append-heavy versioned events as the primary path.
- **LSM tree of opaque key-value records:** provides a known write architecture but hides columns, temporal semantics, and pruning inside values and would turn ChronosDB into a generic KV wrapper.
- **Parquet directly as the hot format:** useful for interchange and cold analytics, but its general-purpose file/row-group lifecycle is not the accepted mutable-head, flush, correction, and manifest contract. Parquet interoperability remains possible.
- **RocksDB as the engine:** would outsource WAL, compaction, manifests, and primary access semantics that define ChronosDB's engineering value, while storing analytical rows as keys/values.

## Consequences

- CSEG, manifest, flush, and compaction become core maintained public contracts.
- Point lookups may need optional indexes or bounded scans rather than a universal tree.
- Sorting, delta management, and compaction introduce write/space amplification that must be measured.
- Readers need generation pinning and reclamation protocols.
- Table design must choose workload-relevant partition/sort dimensions without claiming one global key fits every workload.

## Affected invariants

This decision directly enforces invariants [2, 3, 6, 7, 10, 11, 13, 14, and 16](../architecture/invariants.md): complete manifest installation, part immutability, stable snapshots, compaction equivalence, integrity coverage, safe reclamation, temporal version preservation, format versioning, and complete head-row publication.

## Validation plan

- Round-trip and fuzz every CSEG codec with hostile lengths, corruption, truncation, and unsupported versions.
- Crash-inject each part-write, sync, rename, manifest-edit, flush, and compaction boundary.
- Differentially compare head/base/delta scans and pre/post-compaction results across all retained snapshots.
- Hold readers across sealing, manifest replacement, and reclamation to validate lifetimes.
- Measure projection, pruning, compression, flush, compaction, and late-data amplification on declared financial and observability distributions.

## Deferred decisions

The CSEG byte layout, page/granule sizes, checksum and compression algorithms, per-type encodings, exact sort keys, partition duration, reorder horizon, head representation, secondary index types, compaction policy, and garbage-collection protocol are deferred.

**Retrospective note (2026-08-06):** [ADR 0016](0016-cseg-v1-layout-integrity-and-compression.md)
and the [CSEG v1 specification](../formats/cseg-v1.md) resolve the v1 byte layout, page/granule
bounds, checksums, baseline encoding, compressor, and physical sort order. The other policies above
remain deferred to their roadmap phases.

## Migration or reversal implications

CSEG versions and manifest compatibility rules must permit explicit upgrade or rejection once data exists. Changing the storage model after v1 would require converters or dual readers and a superseding ADR. Adding an evidence-backed optional index or Parquet export does not reverse the primary-store decision.

## References

- [Architecture storage components](../architecture/overview.md)
- [Representative workloads](../product/workloads.md)
- [Roadmap phases 4–7](../roadmap.md)
