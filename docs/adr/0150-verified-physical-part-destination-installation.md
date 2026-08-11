# ADR 0150: Verified physical-part destination installation

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB cluster, storage, and distributed-systems maintainers
- **Extends:** [ADR 0146](0146-raft-tablet-physical-snapshot-projection.md),
  [ADR 0148](0148-durable-physical-part-chunk-receipt.md), and
  [ADR 0149](0149-idempotent-final-temporal-part-adoption.md)

## Context

Durable chunk completion proves one contiguous object and its SHA-256, but it does not make that
object a canonical CSEG or install it in the destination database. Manifest installation accepts
an immutable owned `EncodedCsegPart`; its constructor is deliberately private so unvalidated bytes
cannot acquire that authority. A physical CSEG may approach 64 GiB, while the current installer
performs complete semantic validation and readback in memory.

## Decision

`install_tablet_physical_part()` composes the two existing owners without weakening either. It
first finalizes the locked durable receipt, then binds its table, tablet, part, length, content
SHA-256, Manifest generation, and Raft group to the supplied Manifest v2 part and tablet
descriptors. Both descriptors must be Raft-owned CSEG 2/0 state in the same nonzero group lineage.

The caller supplies a nonzero materialization limit no greater than the frozen CSEG file maximum;
the default is 256 MiB. An object above that cap or the platform container limit returns resource
exhaustion before allocation or destination mutation. The implementation reloads the immutable
chunks in exact contiguous offset order, exact-decodes the resulting CSEG v2 image, copies it into
the private owned representation, and calls `ManifestStorage::install_temporal_part()`. That owner
recomputes the complete schema/source/descriptor semantics and performs synchronized temporary
write, readback, file sync, no-replace rename, and directory sync. Exact orphan-final retry
adoption remains its responsibility.

Success proves only that the destination CSEG final is durable. It does not publish a Manifest
generation, advance movement readiness, delete receipt chunks, or authorize source reclamation.

## Consequences and validation

The present bridge rereads the transfer for SHA-256 completion, materialization, exact CSEG
adoption, and Manifest validation/readback. This is intentionally conservative and bounded by an
explicit admission cap. Supporting larger parts requires a streaming CSEG/Manifest installer with
the same complete validation and durable ordering, not an implicit cap bypass.

Real-filesystem tests receive a Raft CSEG across multiple chunks, install it, verify the final file,
and retry it under a new nonce without duplicate installation. They also prove generation/source
mismatch and materialization exhaustion leave the destination parts namespace untouched. Cluster
and CSEG suites run with warnings as errors and focused ASan/UBSan coverage.

Invariants 1–5, 8, 10, 11, 14, and 18 apply.

## Migration and rollback

No durable or wire format changes. Rollback can retain completed chunk directories and an
unreferenced installed final; neither is query-visible without a durable destination Manifest.
Operators must preserve either object until a later reconciliation path proves it safely
reconstructible or reclaimable.

## References

- [Tablet Physical Part Chunk v1](../formats/tablet-physical-part-chunk-v1.md)
- [Manifest v2](../formats/manifest-v2.md)
- [Manifest installation and checkpointing](../architecture/manifest-installation-and-checkpointing.md)
