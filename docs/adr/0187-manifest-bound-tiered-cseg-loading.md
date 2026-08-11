# ADR 0187: Manifest-bound local and remote CSEG loading

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB storage, tiering, query, and recovery maintainers
- **Extends:** [ADR 0152](0152-atomic-temporal-manifest-publication.md), [ADR 0185](0185-atomic-tiered-storage-publication.md)

## Context

A cold object key is routing metadata, not independent data authority. Query loading must preserve
the exact Manifest v2 descriptor, schema, tablet source, and aggregate Manifest/cold publication
that selected a part. Falling back from a damaged local final to a remote copy would also hide local
corruption and make source choice depend on an error rather than durable tiering state.

## Decision

`load_tiered_temporal_part_images` accepts one owning `TieredDatabaseStorageSnapshot`, a local
`ManifestStorage`, an `ObjectStore`, strictly sorted requested part identities, exact schema
bindings, and explicit count/byte/validation limits. It first proves every identity belongs to the
pinned Manifest and preflights their aggregate declared length.

For each selected identity, the loader attempts the existing full local Manifest-v2 part load. A
valid local final wins. Only `NOT_FOUND` may use the cold route pinned by the same aggregate
snapshot; corruption, permission, schema, and all other local failures are returned unchanged.
Remote loading requires an exact cold descriptor, authoritative per-key metadata matching key,
length, and SHA-256, a complete bounded object read, a repeated whole-object SHA-256, and the same
full CSEG/schema/tablet/source validation used for local bytes.

Each returned move-only image owns either the validated local image or remote byte vector and a copy
of the aggregate tiered snapshot. Its byte view, descriptor, Manifest generation, and cold route
therefore remain valid together until the image is destroyed. This boundary does not yet authorize
local deletion, provide a range cache, or integrate the distributed query worker.

## Consequences and validation

Remote fallback currently reads a complete CSEG, so memory and transfer cost are linear in object
length and bounded by the request limit. It intentionally performs both object-level SHA-256 and
full CSEG validation. Future range/caching work must preserve the same authority and lifetime
contract rather than weakening validation.

Focused tests use real CSEG v2, Manifest v2, durable cold-manifest, publication, and object-store
owners. They prove local preference, missing-local remote loading, full byte equality, aggregate
snapshot retention after every other owner is destroyed, fail-closed local corruption, remote
metadata mismatch, identity ordering, and byte limits. The installed public-target consumer covers
the exported API.

Invariants 2, 3, 6, 8, 11, 14, and 18 apply.

## Alternatives considered

- **Fallback after any local error:** rejected because it masks local corruption and authorization
  failures.
- **Resolve the latest cold key per read:** rejected because one query could mix publication epochs.
- **Trust object metadata without reading SHA-256:** rejected because provider metadata is part of
  routing evidence, not a substitute for validating returned bytes.
- **Return unowned spans:** rejected because asynchronous query execution could outlive both remote
  buffers and the location authority.

## Migration and rollback

Local-only databases continue to load valid local finals. A missing local final without an exact
pinned cold descriptor fails closed. Rolling back after local deletion requires restoring the exact
Manifest bytes locally or continuing to use the aggregate tiered authority.

## References

- [Tiered CSEG loading](../learning/tiered-cseg-loading.md)
- [Cold Location Manifest v1](../formats/cold-location-manifest-v1.md)
- [Architecture invariants](../architecture/invariants.md)
