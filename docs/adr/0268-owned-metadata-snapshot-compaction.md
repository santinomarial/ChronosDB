# ADR 0268: Owned Metadata Snapshot Compaction and Recovery

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB metadata, Raft, and recoverability maintainers
- **Extends:** [ADR 0266](0266-metadata-application-snapshot-v1.md) and
  [ADR 0267](0267-durable-metadata-snapshot-installation.md)

## Context

Canonical metadata snapshot bytes and durable local installation exist, but independent callers
could still compact Raft first, install mismatched application bytes, omit a committed metadata
command, or reopen only one side. Metadata state must remain unpublished until the application
snapshot, durable Raft boundary, and retained committed suffix form one exact history.

## Decision

`DurableMetadataStateMachine` may own `MetadataSnapshotStorage`. Its compaction operation accepts a
newer applied index only under stable membership. It copies application entries from the exact
current installed snapshot, appends every metadata/schema entry through the requested retained-log
boundary, derives the membership checkpoint, and computes the application identity. Metadata
snapshot generation equals the included index in v1. The identity is SHA-256 over the domain
`CHRMASN\x01` followed by each entry's little-endian index, term, one-byte type, little-endian
64-bit payload length, and exact payload.

Membership checkpoint derivation is delegated to the Raft core's read-only prefix preparation
before application bytes are installed. It replays only through the requested boundary, rejects a
prefix ending in joint state, and can therefore checkpoint older stable voters while retaining a
later joint/final change. Copying the node's later live voters is not a valid substitute.

The owner durably installs the complete application snapshot first and only then submits Raft's
`CompactSnapshotOperation`. It verifies the synchronized Raft state equals the installed snapshot
metadata before adopting the new live boundary. A crash between the two leaves an unreferenced
immutable application snapshot that an exact retry may adopt; it never leaves compacted Raft
without prior application durability.

Recovery loads the file named by Raft's exact nonzero snapshot index, requires complete
`SnapshotMetadata` equality, recomputes the application digest, decodes every nested command before
publishing an owner, applies verified internal gaps efficiently, and then replays only the committed
retained suffix. Recovery without the required snapshot fails closed. Live application rejects a
snapshot boundary that changes outside this owner.

## Consequences and validation

Compacted metadata groups now reopen without a second catalog authority. Snapshot v1 intentionally
retains exact command payloads; compaction reduces the shared physical Raft history requirement but
does not yet reduce logical metadata snapshot size. The OpenSSL SHA-256 provider already required by
ChronosDB is a private implementation dependency; no cryptographic type enters the public Raft API.

A real-filesystem test applies a complete schema and table policy, installs the application
snapshot, compacts Raft, commits a suffix node command, closes both owners, proves recovery without
the snapshot is rejected, and reconstructs the exact complete catalog from snapshot plus suffix.
ADR 0269 now provides node-wide shared-log reclamation after every resident group has a fresh full
state record, and ADR 0270 removes every application snapshot except the exact Raft authority. Crash
injection now includes a ten-cut real-process `SIGKILL` matrix spanning application temporary
creation, write, readback, file sync, close, rename, and directory sync followed by the Raft record
write, Raft sync, and successful return. Pre-Raft-authority images replay the retained log and an
exact retry either installs the snapshot or adopts the immutable orphan left after rename;
post-Raft-record images require and exact-match that application snapshot. Every schedule proves
catalog reconstruction, retry convergence, and a second reopen. This is process-restart evidence,
not physical power-loss qualification. A separate four-schedule injected-I/O matrix arms both
owners, crosses application partial-write and post-rename directory-sync failures with either a
pre-write or partial Raft record on the next attempt, and proves that the first owner failure
prevents the second mutation. Reopen removes or adopts the exact application state; strict Raft
recovery rejects a partial record, explicitly authorized tail repair restores the retained log, and
an exact retry plus second reopen converge. Snapshot transfer, fuzzing, and large catalogs remain
deferred. A repeated-failure test now interrupts two consecutive application temporaries and proves
each following open removes the exact partial file, then interrupts two consecutive Raft compaction
records and requires strict rejection plus explicit tail repair after each. The final retry adopts
the immutable application orphan, compacts Raft, reconstructs the catalog, and survives another
reopen. An eight-schedule owner-reopen matrix now leaves an interrupted application temporary,
fails cleanup before unlink or after unlink at directory sync, then leaves a partial Raft compaction
record and fails each authorized repair stage: size inspection, truncation, repaired-file sync, and
repair-directory sync. Every failed open releases its lock; the next open accepts the observed
temporary and tail state, adopts the exact application orphan, finishes compaction, and survives a
second reopen. Simultaneous faults and other non-cleanup cross-stage combinations remain deferred.
A membership-boundary filesystem test compacts an application entry before retained joint/final
commands, requires the installed metadata snapshot to carry the older voter set, and keeps the live
group on the newer stable set.

Invariants 1–6, 8, 10, 11, 14, and 18 apply.

## References

- [Metadata Application Snapshot v1](../formats/metadata-application-snapshot-v1.md)
- [ADR 0088](0088-owned-raft-tablet-snapshot-compaction.md)
