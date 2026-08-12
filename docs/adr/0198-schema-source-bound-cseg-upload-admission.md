# ADR 0198: Schema/source-bound CSEG upload admission

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB storage and tiering maintainers
- **Extends:** [ADR 0187](0187-manifest-bound-tiered-cseg-loading.md)

## Context

`TieredPartManager` previously checked the supplied byte length and whole-object SHA-256 before
upload, but those checks did not prove that the bytes were a valid CSEG for the claimed part,
schema, tablet, and WAL source. A caller could therefore place structurally or semantically invalid
bytes in remote storage before its manifest callback rejected them. The object would not become
query authority, but leaving invalid unreferenced state weakens recoverability and garbage-control
assumptions.

Manifest-v1 startup recovery already has a complete referenced-part validator. A separate tiering
parser would create two definitions of valid durable bytes and invite validation drift.

## Decision

`upload_and_install` requires a `TieredPartAdmission` containing the exact WAL identity, immutable
table schema, and bounded referenced-part validation limits. It derives the canonical filename from
the cold descriptor and calls `validate_manifest_v1_part_image` before computing the upload digest
or invoking any object-store or manifest callback.

The reused validator checks the complete CSEG framing, pages, checksums, padding, descriptor and
schema/tablet identities, user and system rows, global ordering, event-time extrema, WAL source,
record-sequence extrema, row count, and declared length. The existing whole-object SHA-256 check
then independently binds the admitted bytes to the cold descriptor.

Any validation, allocation, or length failure returns before remote mutation. Upload, exact remote
metadata verification, and the atomic manifest callback retain their existing order after admission
succeeds. This decision changes no CSEG, Manifest, cold-manifest, or object format.

## Consequences and validation

Callers must retain the schema for the synchronous call and provide the authoritative WAL identity;
the admission structure borrows the schema rather than copying it. A full validation pass adds
linear CPU work before upload, which is deliberate because correctness and recoverability outrank
transfer latency and the upload path is not a per-row query path.

Focused tests corrupt a CSEG while recomputing its whole-object SHA-256 and separately supply the
wrong WAL identity. Both are rejected as corruption, with zero remote objects and no manifest
installer invocation. The feature-completion smoke path uploads a real CSEG through the same
admission API.

Invariants 2, 3, 10, 11, 14, and 18 apply.

## Alternatives considered

- **Trust the caller's digest and metadata:** rejected because hashes prove byte identity, not CSEG
  meaning or authority.
- **Validate only headers and page checksums:** rejected because source rows, extrema, ordering, and
  schema semantics are part of the durable contract.
- **Create a tiering-specific parser:** rejected because it would duplicate the recovery validator
  and could accept a different durable language.
- **Upload first and validate before manifest install:** rejected because invalid unreferenced
  objects would still escape into remote storage.

## Migration and rollback

There is no durable migration. Source callers must supply admission authority when compiling against
the new API. A rollback can read all previously completed objects because their durable bytes and
metadata are unchanged.

## References

- [Tiered CSEG loading learning guide](../learning/tiered-cseg-loading.md)
- [Architecture invariants](../architecture/invariants.md)
