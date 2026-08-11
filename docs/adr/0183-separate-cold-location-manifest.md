# ADR 0183: Separate cold-location manifest authority

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB storage, tiering, query, and recovery maintainers
- **Extends:** [ADR 0082](0082-source-neutral-manifest-v2-layout.md), [ADR 0182](0182-libcurl-sigv4-s3-object-store.md)

## Context

Manifest v2 part descriptors bind exact CSEG identity, length, source, temporal ranges, format, and
SHA-256, but intentionally contain no location or variable object key. Its fixed descriptor layout,
zero reserved bytes, version registry, and strict decoder are accepted and implemented. Reusing
reserved bytes would silently change frozen 2.0 bytes, while putting arbitrary keys in a fixed
descriptor is not possible. Rewriting the entire storage/query stack to a speculative Manifest v3
before the cold lifecycle exists would create unnecessary migration and recovery scope.

## Decision

Adopt [Cold Location Manifest v1](../formats/cold-location-manifest-v1.md) as a separate immutable
full-generation registry. Each generation binds one database UUID, exact Manifest v2 generation,
deployment-assigned object-store UUID, and a sorted set of exact part/length/SHA-256/object-key
descriptors. Header, every descriptor, and the complete file have CRC32C coverage. Key storage is
bounded, contiguous, canonical UTF-8, and independent of endpoint credentials.

Manifest v2 remains the sole logical part/version authority. A cold generation is useful only after
`validate_cold_location_manifest_binding` exact-matches its database/generation and every listed
part's immutable byte identity to one pinned decoded Manifest v2 value. The object-store UUID must
resolve to one deployment configuration; listings, ETags, and an equal part name are never proof.

This decision freezes codec and binding semantics only. It does not allow local deletion merely
because valid bytes can be encoded. [ADR 0184](0184-durable-cold-location-generations.md) now owns
durable installation and no-fallback recovery selection. Atomic publication of a compatible
base/cold pair and its reader lifetime are now owned by
[ADR 0185](0185-atomic-tiered-storage-publication.md). Cross-directory crash commit, reader-pin
retirement proofs, cache/query integration, and remote deletion remain separate.

## Consequences and validation

The separate registry preserves Manifest v2 compatibility and lets cold locations advance without
rewriting logical CSEG descriptors. Readers must acquire a compatible pair rather than treating two
independent latest generations as coherent. Full-generation duplication costs storage but keeps
selection, rollback, and audit behavior explicit.

Focused tests freeze the 256-byte header, 96-byte descriptor, 472-byte two-location fixture,
deterministic encoding, every truncation boundary, exact/suffix behavior, checksum-valid unknown
versions, descriptor damage, configured limits, ordering/key uniqueness, and exact binding to a
real decoded Manifest v2 generation. Durable filesystem and crash tests remain deferred until the
installer exists.

Invariants 2, 3, 6, 10, 11, 14, and 18 apply.

## Alternatives considered

- **Use Manifest v2 reserved bytes:** rejected because those bytes are frozen zero and cannot hold
  bounded variable keys.
- **Immediately define Manifest v3:** rejected because location lifecycle can compose with v2 and a
  full logical-format migration has no current need.
- **Store a mutable key/value side database:** rejected because it hides durability, recovery, and
  snapshot-pairing semantics behind another engine.
- **Use bucket listing or object tags as the registry:** rejected because remote enumeration is not
  transactionally paired with Manifest visibility and may be delayed or incomplete.

## Migration and rollback

Databases without a cold manifest remain local-only. The first durable cold generation will be
created only after every listed object is uploaded and verified. Rollback can stop publishing cold
generations while retaining local files; once local reclamation exists, rollback must restore or
retain a verified local copy before removing the cold authority.

## References

- [Manifest v2 format](../formats/manifest-v2.md)
- [libcurl SigV4 S3 backend](0182-libcurl-sigv4-s3-object-store.md)
- [Architecture invariants](../architecture/invariants.md)
