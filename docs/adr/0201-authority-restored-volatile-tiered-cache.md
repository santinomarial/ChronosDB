# ADR 0201: Authority-restored volatile tiered cache

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB storage, recovery, and tiering maintainers
- **Extends:** [ADR 0183](0183-separate-cold-location-manifest.md), [ADR 0200](0200-concurrent-bounded-tiered-part-cache.md)

## Context

The full-object LRU is an in-memory optimization and has no durable index. That is desirable—query
authority belongs to Manifest and Cold Location Manifest generations—but the manager's separate
part catalog was also populated only as a side effect of upload. A restarted process therefore
could not use already-durable cold descriptors through `TieredPartManager`, even though their remote
objects and authority survived.

Persisting the LRU or a second cache index would duplicate durable authority and create crash,
versioning, reconciliation, and deletion problems. Restart instead needs a bounded way to restore
the manager's routing catalog from the exact durable authority already selected by the embedding
recovery owner.

## Decision

`TieredPartManager::restore_catalog` accepts a strictly ascending span of `ColdPartDescriptor`
values from an already selected authoritative Manifest/cold-manifest pair. It is a single-owner
startup operation, must not overlap reads, and requires an empty manager. The count must fit the configured catalog bound;
each descriptor must have a nonempty key, finite nonzero length and row count, valid record/event
extrema, and a unique ascending part identity.

Before publishing any catalog entry, restoration calls authoritative `stat` for every object and
requires exact key, declared length, and SHA-256 metadata. Descriptors accumulate in a private map;
only complete preflight swaps it into the manager. A missing, inaccessible, mismatched, malformed,
or over-budget object leaves the live catalog empty.

The LRU, recency list, and byte counter always start empty and are never persisted. A first read
uses the restored route, reads the exact declared full length, independently checks returned length
and whole-object SHA-256, then admits the bytes through the normal bounded concurrent cache path.
Bucket listings are not consulted.

## Consequences and validation

Cold startup performs `O(part count)` authoritative metadata requests and stores `O(part count)`
routing entries before reader admission. This cost is deliberate: it fails before queries can
observe a partially usable catalog. Complete object transfer remains demand-driven. The API relies
on its caller to construct descriptors from the exact durable pair; it does not make arbitrary
caller input into authority.

Focused tests place two exact CSEGs in the object store, construct a fresh manager, restore its
catalog, prove the cache begins empty, read and verify both objects, and observe demand-driven cache
population. A mismatched second digest proves all-or-nothing restoration and leaves even the first
descriptor unpublished. The installed external consumer references the public method.

Invariants 2, 3, 6, 8, 10, 11, 14, and 18 apply.

## Alternatives considered

- **Persist the LRU/index:** rejected because it duplicates authority and adds a new durable format
  for reconstructible performance state.
- **Trust the restored descriptors without remote preflight:** rejected because a restart could
  publish routes to missing or conflicting immutable content.
- **Insert descriptors as they pass:** rejected because a later failure would expose partial
  startup state and make retries state-dependent.
- **Re-upload every object on restart:** rejected because local source bytes may already be safely
  reclaimed and immutable remote content already exists.

## Migration and rollback

No durable or network format changes. Existing managers can continue using upload-only population.
Rollback removes the restoration API; deployments must then use the snapshot-bound tiered loader or
retain local source files rather than expecting this manager to reopen existing cold routes.

## References

- [Cold Location Manifest v1](../formats/cold-location-manifest-v1.md)
- [Tiered CSEG loading learning guide](../learning/tiered-cseg-loading.md)
- [Architecture invariants](../architecture/invariants.md)
