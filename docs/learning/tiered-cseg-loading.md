# Tiered CSEG Loading

## Purpose and public interface

`load_tiered_temporal_part_images` is the first read boundary that can obtain one Manifest-v2 CSEG
from either the local parts directory or an immutable object store. The caller supplies an owning
`TieredDatabaseStorageSnapshot`; this is crucial because that one snapshot pins both the exact
logical Manifest and its compatible Cold Location Manifest.

Requested part IDs must be strictly sorted and present in the snapshot. Limits cap the identity
count, total declared bytes, and the existing CSEG validation work. A successful
`TieredTemporalPartImage` reports `kLocal` or `kRemote`, exposes the exact descriptor and borrowed
byte view, and retains its aggregate snapshot for the image lifetime. The image is move-only so its
owned bytes and snapshot pin have one clear lifetime.

## Validation and failure behavior

The source decision is deliberately asymmetric:

1. load and fully validate the local final through `ManifestStorage`;
2. return every local error except `NOT_FOUND` without consulting remote storage;
3. for a missing final, require an exact part/length/SHA-256 route from the pinned cold manifest;
4. require object metadata to repeat that key, length, and digest;
5. read the complete bounded object, recompute SHA-256, and perform full Manifest-v2 CSEG validation.

This ensures remote storage is a location for the same immutable bytes, not a second authority that
can override the Manifest. Corrupt local files remain observable. Missing routes, mismatched
metadata, incomplete reads, digest differences, schema absence, and CSEG/source violations all fail
closed without returning a partial image.

Cold admission uses the complementary rule. `TieredPartManager::upload_and_install` receives an
exact WAL identity, table schema, and validation limits. Before the first object-store operation it
derives the canonical part name and reuses the Manifest-v1 referenced-part validator over the full
candidate image. That proves the page/checksum structure, schema and tablet identities, system-row
WAL source, record and event-time extrema, row count, declared length, and whole-object digest.
Only then may immutable upload, exact remote verification, and the caller's atomic manifest install
run. A failure at the admission boundary performs no remote mutation and invokes no installer.

## Ownership, complexity, and tradeoffs

A local image owns its complete byte buffer through `LoadedTemporalPartImage`; a remote image owns a
`std::vector<std::byte>`. Both also retain the aggregate shared publication epoch. Construction is
single-threaded and performs no mutation. Separate calls may share the immutable snapshot and
thread-safe object-store implementation, subject to the object's documented lifetime.

Preflight is `O(requested parts × snapshot parts)` with no extra index in this first bounded
implementation. Each selected part adds `O(file bytes)` I/O, hashing, and semantic validation. The
remote path currently performs one HEAD-equivalent request plus one complete GET and holds the full
object in memory. A cache or projected-range reader can reduce repeated transfer later, but it must
retain the same exact snapshot, integrity, and schema/source proofs.

`TieredPartManager`'s separate full-object cache supports concurrent post-install readers. One mutex
linearizes its entry map, LRU list, byte counter, and iterator updates. Remote download and digest
verification occur outside that mutex; after a miss, the caller rechecks under lock and either
installs after bounded eviction or reuses the entry won by another reader. Hits copy while locked so
eviction cannot invalidate their source. Upload/catalog mutation remains single-owner and must
quiesce before these reads; manager destruction likewise requires external lifetime exclusion.

The LRU is deliberately volatile and has no durable cache index. At restart, an empty manager can
restore a strictly sorted descriptor catalog supplied by already selected Manifest/cold-manifest
authority. Restoration preflights exact remote key, length, and SHA-256 metadata into a private map
and publishes it only after the complete set succeeds. Cache state remains empty until the first
read repeats exact length and whole-object digest validation and repopulates it on demand. This
keeps durable truth in the existing manifests and makes cache loss a performance event rather than
a recovery decision.

## Current boundary and review questions

Pair recovery now uses the same remote validation primitive for absent local finals. It first
metadata-loads and hashes the exact pair-selected Manifest, binds the exact committed cold
generation, then repeats full Manifest loading with remote validation before creating any
publishable owner. A metadata-only Manifest value is deliberately not a storage snapshot.

Reader-pinned local reclamation now supplies that missing proof for Raft-owned parts. The aggregate
publisher tracks weak historical epochs; only an epoch that names a candidate without its own exact
cold route blocks. After those pins expire, the coordinator reloads the exact pair marker, fully
validates every remote image, and passes a private capability to ManifestStorage. ManifestStorage
then rechecks Manifest bytes, exact referenced descriptors, and all present local SHA-256 values
before unlinking and synchronizing the directory. Retry treats absent files idempotently, while a
present damaged local image remains a corruption error.

The distributed aggregate worker supplies the integrated query path: all dispatch/placement/Raft/
Manifest gates run before a synchronous tiered batch loader exposes image views to the unchanged
temporal resolver. WAL-owned startup and other local-only entry points are not eligible for local
reclamation yet. Remote deletion is separately authorized only after the part and route leave the
selected pair and every route-bearing aggregate reader drains; it never follows from a loader miss.
The loader does not authorize multipart upload, cache eviction, or retry policy. Those remain the
manager and provider carrier's responsibility after admission succeeds.

Likely review questions include why only `NOT_FOUND` permits fallback, why the cold key is not data
authority, why metadata and a recomputed digest are both checked, why the complete CSEG validator is
reused, and which owner keeps location strings alive during a query.
