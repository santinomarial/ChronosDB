# ADR 0270: Raft-Authoritative Application Snapshot Reclamation

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB ingestion, metadata, storage, and recoverability maintainers
- **Extends:** [ADR 0088](0088-owned-raft-tablet-snapshot-compaction.md) and
  [ADR 0268](0268-owned-metadata-snapshot-compaction.md)

## Context

Tablet and metadata compaction install immutable application snapshots before advancing durable
Raft snapshot metadata. Repeated compaction therefore leaves older files, and a crash between
application installation and Raft compaction can leave a higher-index orphan. Selecting the highest
application file is not safe because only durable Raft state decides which snapshot and retained
suffix form the authoritative history.

## Decision

Both snapshot-storage owners expose explicit `reclaim_obsolete` operations. A nonzero authoritative
index is exact-loaded and fully revalidated before mutation. The owner then removes every other
canonical final snapshot, including older generations and higher-index crash orphans, and
synchronizes the directory. If durable Raft state has no snapshot, a null authority removes every
canonical final as an orphan.

`RaftTabletStateMachine` and `DurableMetadataStateMachine` expose the operation only through their
owned snapshot storage. They first require that the live durable Raft `SnapshotMetadata` exactly
matches the application snapshot adopted during recovery or compaction. The zero-snapshot case
requires that no application snapshot was adopted. A cleanup error is returned for retry but does
not change Raft or application authority and does not fail the state machine closed.

The operation ignores unrelated non-prefixed files consistently with existing storage discovery,
but rejects malformed or nonregular entries in its recognized snapshot namespace. It never deletes
the authoritative filename. Partial deletion is safe and retryable because every removal targets a
non-authoritative immutable file; directory synchronization determines only whether cleanup itself
survives a crash.

Both storage owners expose process-local saturating cleanup metrics. They count temporary files and
their cleanup directory synchronizations only after that synchronization succeeds. Reclamation
attempts and failures are counted independently, while reclaimed files and reclamation directory
synchronizations advance only after the successful durability boundary. An unlink or directory-sync
error may already have changed the observed namespace, so failure counters intentionally do not
claim those removals; the next attempt restarts discovery from current bytes. The composed tablet
and metadata state machines forward the metrics only when they own snapshot storage.

## Consequences and alternatives

Reclamation is caller-triggered rather than part of compaction's success result. This avoids an
ambiguous error after application installation and Raft compaction have already become durable.
Automatic highest-file retention was rejected because a pre-Raft crash orphan may have the greatest
index. Reader pins are unnecessary for v1 because snapshot loads own and decode all bytes before the
file handle is released; no live state retains mapped or borrowed file storage.

Scheduling and device qualification remain hardening work.

## Validation and invariants

Invariants 1–5, 8, 10, 11, 14, and 18 apply. Real-filesystem tests retain an exact middle authority
while deleting older and future files, reclaim all files for a zero-snapshot boundary, and exercise
the tablet and metadata state-machine wrappers after repeated compaction. Both storage owners'
one-shot injection covers authoritative-file open, validation stat, size stat, and read before any
mutation; directory enumeration; failure at each ordered obsolete-file unlink; and final directory
sync. Each owner's eight nonzero-authority cases always preserve and revalidate the middle
authority. Each owner's five zero-authority cases expose only the exact partial orphan deletion
already completed. Every failure keeps the owner usable, and exact retry plus reopen converges. Each
storage owner's eleven-schedule `SIGKILL` matrix stops after enumeration, each individual unlink,
final directory sync, and success release for both nonzero and zero authority. Reopen observes
exactly the completed deletion prefix, then exact retry and a second reopen converge without
deleting the middle authority or retaining a zero-authority orphan. Storage tests pin successful
temporary-cleanup counters for both owners and
reclamation attempt/failure/synchronized-success counters across every injected failure for both
owners; state-machine tests prove ownership-aware forwarding.
