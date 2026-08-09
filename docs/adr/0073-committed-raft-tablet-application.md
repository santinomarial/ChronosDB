# ADR 0073: Committed Raft tablet application and retained-log recovery

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** ChronosDB ingestion and distributed-systems maintainers
- **Extended by:** [ADR 0085](0085-raft-tablet-application-snapshot-v1.md)

## Context

The deterministic Raft core, shared durable runtime, and tablet ingestion machinery existed as
separate owners. An appended entry could become committed, but there was no boundary that decoded it,
published rows under a Raft group/index identity, and then durably advanced `applied_index`. Simply
restoring a persisted applied index after process restart would also be unsafe because mutable tablet
state is process memory and no Raft application snapshot exists yet.

## Accepted decision

`RaftTabletStateMachine` owns one fresh tablet and the database retry directory for one configured
Raft group. It borrows the single-thread-affine durable runtime and retains the accepted schema
lineage. Logical Raft entry type 1 contains one exact `COLUMNAR_APPEND v1` application payload.

Live application reads only `committed_unapplied()`, preflights the complete available batch, applies
entries in ascending index order through the same committed-command function used by WAL recovery,
and publishes each command at `(RAFT, group_id, log_index)`. After all entries succeed, one durable
runtime batch persists the final applied index. Unknown, corrupt, misrouted, conflicting, or
unrepresentable commands fail the tablet state machine closed.

Recovery accepts only fresh unpublished tablet and retry owners. Because the v1 Raft log currently
retains complete entry history, recovery replays every entry through `commit_index`, regardless of
the persisted `applied_index`, before returning the owner. If the persistent applied index trails the
commit index, recovery advances it only after the rebuilt application state succeeds. A nonzero Raft
snapshot boundary is rejected until a corresponding application snapshot is implemented.

The WAL recovery path and Raft application path share `apply_committed_columnar_append`, including
exact retry behavior, batch ownership, tablet publication, and global retry publication. Legacy
`mark_wal_started` API names denote crossing the external durable commit gate in this shared path;
they perform no WAL I/O for Raft application.

## Consequences and alternatives

Uncommitted entries are never query-visible, applied-index persistence cannot precede tablet
publication, exact retries do not duplicate rows, and a process restart can reconstruct application
memory from authoritative retained log bytes. Replaying the full committed prefix costs time and
requires retaining all entries; this is accepted only until application snapshots and safe log
reclamation are implemented.

Trusting the recovered applied index without tablet bytes was rejected because it loses acknowledged
rows after restart. Marking applied before publication was rejected because a crash could skip an
unpublished command forever. Maintaining separate WAL and Raft mutation implementations was rejected
because their retry and row-publication semantics could diverge.

## Affected invariants and validation

Invariants 1, 3, 4, 5, 6, 8, 11, 14, and 18 apply. Focused tests prove uncommitted invisibility,
ordered committed publication, exact duplicate suppression, durable applied-index advancement,
fail-closed corrupt committed bytes, and complete reconstruction after durable runtime reopen. Crash
injection at application/applied-index boundaries, application snapshots, log reclamation, schema
catalog recovery, multi-group scheduling, and true quorum acknowledgment remain required.
