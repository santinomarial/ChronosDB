# ADR 0128: Tablet movement RTAS handoff

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB ingestion, storage, and distributed-systems maintainers
- **Extends:** [ADR 0085](0085-raft-tablet-application-snapshot-v1.md),
  [ADR 0086](0086-durable-raft-tablet-snapshot-installation.md), and
  [ADR 0127](0127-composed-tablet-movement-checkpoint-recovery.md)
- **Extended by:** [ADR 0129](0129-tablet-movement-raft-snapshot-completion.md)

## Context

A completed movement prefix is checksummed transfer authority, but it is not yet an installed tablet
application snapshot. The target must not accept arbitrary bytes, install an RTAS for another
table/tablet/group, reinterpret transfer metadata, or allow a later promotion checkpoint to
retroactively create the snapshot that should have preceded catch-up and promotion.

## Decision

The application payload transferred by this handoff is exactly one canonical Raft Tablet
Application Snapshot v1 value. `install_recovered_tablet_movement_snapshot` accepts an authoritative
recovered movement generation, the expected table, and one locked group-owned
`RaftTabletSnapshotStorage`.

Before first installation, the handoff requires movement phase `kCatchingUp`, exact-decodes the
complete transferred prefix under caller limits, and requires canonical re-encoding to
match every transferred byte. The RTAS table and tablet must match the expected table and movement
tablet. Its Manifest generation and included Raft index/term must equal the movement transfer
metadata, its voters must equal the pre-promotion source voter set, and the RTAS storage owner must
accept its group. The existing RTAS installation protocol then exact-readbacks, file-synchronizes,
no-replace renames, and directory-synchronizes the immutable file.

For a recovered `kReady`, `kTargetPromoted`, or `kComplete` movement, handoff is verification-only.
The exact RTAS index and bytes must already be present; absence or difference is corruption. This
enforces the ordering that durable application-snapshot installation precedes target catch-up
evidence and promotion. The report returns the complete decoded `SnapshotMetadata` so a later owner
can perform the separate Raft snapshot-installation transition without reconstructing metadata from
the movement's compact transfer fields.

This decision does not call `complete_snapshot_install`, acknowledge the source, install physical
Manifest/CSEG files, or reclaim transfer chunks. RTAS v1 contains exact application commands and
Manifest identity, not a physical part bundle. Those authority transitions remain explicit
follow-up work.

## Rationale and alternatives

Reusing RTAS v1 preserves the existing state-machine recovery contract and avoids a second tablet
snapshot representation. Trusting only transfer size and CRC was rejected because those fields do
not bind group, table, tablet, membership, or canonical application contents. Installing after
promotion was rejected because it would make local movement state claim a prerequisite that was
never durable.

## Consequences and validation

The handoff performs bounded linear decode and canonical encode work before the existing durable
installation. It may temporarily own decoded entries and one canonical encoded copy in addition to
the recovered movement prefix; caller codec limits bound each object. The movement and RTAS storage
owners remain externally serialized and retain their existing lifetimes.

Invariants 1–5, 8, 10, 11, 14, and 18 apply. Real-filesystem tests cover installation from reopened
reference/chunk owners, byte-identical RTAS adoption, retries after ready and promotion, incomplete
transfer rejection, ready/promoted-without-install corruption, table/tablet/metadata/voter/group
mismatch, and non-RTAS bytes. Raft metadata completion, source acknowledgment, physical part
transfer, fault injection, process crashes, reclamation, and allocation-failure sweeps remain
deferred.

**Retrospective update (ADR 0129):** that decision subsequently composes this RTAS boundary with
exact pending-request validation and the durable Raft metadata transition. Response transport and
physical part installation remain separate.

## Migration and rollback

No durable format changes. Existing RTAS files remain valid. A rollback may ignore the handoff API,
but must not promote a target unless an equivalent exact durable application-snapshot installation
has already succeeded.

## References

- [Raft Tablet Application Snapshot v1 format](../formats/raft-tablet-application-snapshot-v1.md)
- [Tablet Movement External-Prefix Reference v1 format](../formats/tablet-movement-checkpoint-reference-v1.md)
- [Committed Raft tablet application learning guide](../learning/raft-tablet-application.md)
