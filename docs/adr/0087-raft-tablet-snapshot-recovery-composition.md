# ADR 0087: Raft tablet application-snapshot recovery composition

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB ingestion and distributed-systems maintainers
- **Extends:** [ADR 0073](0073-committed-raft-tablet-application.md),
  [ADR 0085](0085-raft-tablet-application-snapshot-v1.md), and
  [ADR 0086](0086-durable-raft-tablet-snapshot-installation.md)

## Context

The deterministic Raft core can reopen a compacted prefix and the application-snapshot owner can
durably load exact command bytes, but the tablet state machine still rejected every nonzero snapshot
boundary. Recovery must not trust persisted `applied_index` as row state, confuse absolute log
indexes with suffix offsets, publish a partial prefix before discovering a bad suffix, or lose the
application frontier when the snapshot ends on a membership-only index.

## Accepted decision

`RaftTabletStateMachine::recover` retains its complete-log overload and adds an overload that takes
move-only ownership of one locked `RaftTabletSnapshotStorage`. A nonzero persistent Raft snapshot
requires the latter. Recovery exact-loads the file named by `last_included_index` and requires its
group, table, tablet, and complete `SnapshotMetadata` to equal the reopened Raft state.

Snapshot commands and the complete committed retained-log suffix are schema/tablet preflighted
before application. Recovery then replays snapshot commands at their original Raft indexes into
fresh tablet/retry owners, publishes a no-row frontier through the included index when membership
gaps require it, replays every committed suffix entry in absolute order, and durably advances Raft's
applied index only after the rebuilt tablet succeeds. Any failure destroys the unpublished owners
and releases snapshot storage ownership.

Membership-only committed batches now use the same no-row frontier publication. Tablet rows,
generations, and retries remain exactly unchanged while the outer Raft group/index position advances.
This keeps the tablet application frontier aligned with durable Raft application even when no row
command occurs at the boundary.

The recovered state machine retains the snapshot lock and rejects a later, different Raft snapshot
boundary. Local snapshot creation/compaction must therefore be integrated through this owner rather
than performed behind it.

## Consequences and alternatives

Compacted-prefix restart no longer depends on reclaimed Raft entries. Exact retry commands in the
suffix rebuild without duplicate rows and still advance the tablet frontier. Membership gaps remain
explicit instead of manufacturing row operations.

Replaying only `committed_unapplied()` was rejected because process-memory state must be rebuilt even
when persisted `applied_index` already reached commit. Treating `commit_index` as a vector length was
rejected because indexes are absolute after compaction. Loading the numerically latest application
snapshot was rejected because only the exact persistent Raft boundary is authoritative.

## Affected invariants and validation

Invariants 1, 3–8, 10, 11, 14, and 18 apply. A real-filesystem test applies index 1, installs its
application snapshot, compacts Raft, commits an exact-retry suffix at index 2, reopens both owners,
proves missing snapshot ownership fails, and reconstructs two rows, one retry, tablet frontier 2,
and durable applied index 2. Existing membership tests now prove no-row frontier advancement.
Mismatch/corruption matrices, allocation-failure sweeps, process crashes, follower installation,
snapshot creation orchestration, and physical-log reclamation remain deferred.
