# ADR 0153: Restartable tablet physical ownership publication

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB cluster, manifest, and distributed-systems maintainers
- **Extends:** [ADR 0150](0150-verified-physical-part-destination-installation.md),
  [ADR 0151](0151-raft-tablet-destination-manifest-composition.md), and
  [ADR 0152](0152-atomic-temporal-manifest-publication.md)

## Context

The physical receiver, CSEG installer, destination Manifest builder, durable Manifest installer,
and runtime publisher each enforce one boundary. Without one coordinator, callers could publish
before all part files are durable or fail permanently when a process stops after Manifest
directory synchronization but before the in-memory epoch store.

## Decision

`install_and_publish_tablet_physical_snapshot()` runs under the existing external single-writer
serialization. It validates the source projection against full Raft snapshot metadata, acquires one
live destination epoch, and builds the exact local successor. It then classifies the locked durable
Manifest namespace:

1. If disk and live publication select the same predecessor, it invokes the ordinary Manifest v2
   installer. That installer rereads and completely validates every referenced CSEG final before
   writing, synchronizing, renaming, and directory-synchronizing the candidate.
2. If disk is exactly one generation ahead, the coordinator treats this only as a possible
   interrupted publication. It does not reinstall or infer success.
3. Any other disk/live generation relationship fails the live publisher closed.

After either first-time installation or possible resume, the coordinator reloads the highest
generation with caller-supplied exact database, schema-lineage, and source-owner bindings. Its full
encoded bytes must equal the freshly rebuilt candidate. Only then does the temporal publisher
repeat transition validation and release-store the owning epoch. The result reports whether the
Manifest was already durable.

An ordinary pre-durability installation error leaves the old publisher usable. A storage poison,
post-durability reload/allocation failure, different same-generation candidate, or publication
failure poisons the live publisher. Restart recovery then selects durable truth. Success does not
advance movement readiness, remove transfer receipts, install Raft application state, or authorize
source reclamation.

## Consequences and validation

The coordinator deliberately repeats projection, Manifest, CSEG, schema, and source validation.
This costs I/O and decoding but keeps each authority independently auditable. The exact-byte resume
rule prevents an unrelated generation with the expected number from being adopted.

Real-filesystem tests cover first installation and publication, predecessor snapshot retention,
exact already-durable resume, rejection of a missing CSEG final without live publication, and
fail-closed handling of a different already-durable successor. Manifest and cluster suites,
installed-consumer checks, and focused ASan/UBSan cover the composed path.

Invariants 1–6, 8, 10, 11, 14, and 18 apply.

## Migration and rollback

No format change. Completed but unpublished successors are recovered by exact reconstruction and
comparison. Rollback must leave such a successor in place and restart through normal highest-
generation recovery; deleting it could discard the only durable ownership publication.

## References

- [Raft tablet physical snapshot projection](../formats/raft-tablet-physical-snapshot-v1.md)
- [Manifest v2](../formats/manifest-v2.md)
- [Manifest installation and checkpointing](../architecture/manifest-installation-and-checkpointing.md)
