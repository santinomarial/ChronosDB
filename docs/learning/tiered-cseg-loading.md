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

## Current boundary and review questions

This loader alone does not make local deletion safe: restart recovery and every query entry point
must use equivalent tier-aware authority before reclamation is enabled. The distributed aggregate
worker now supplies one integrated path: all dispatch/placement/Raft/Manifest gates run before a
synchronous tiered batch loader exposes image views to the unchanged temporal resolver. Other query
entry points remain local-only. The loader also does not authorize remote deletion, multipart
upload, cache eviction, or retry policy.

Likely review questions include why only `NOT_FOUND` permits fallback, why the cold key is not data
authority, why metadata and a recomputed digest are both checked, why the complete CSEG validator is
reused, and which owner keeps location strings alive during a query.
