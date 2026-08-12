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
injection, snapshot transfer, reclamation fault injection, fuzzing, and large catalogs remain
deferred.

Invariants 1–6, 8, 10, 11, 14, and 18 apply.

## References

- [Metadata Application Snapshot v1](../formats/metadata-application-snapshot-v1.md)
- [ADR 0088](0088-owned-raft-tablet-snapshot-compaction.md)
