# ADR 0069: Deterministic Raft transitions and multiplexed state records

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** ChronosDB distributed-systems maintainers

## Context

Raft must be testable without clocks, sockets, or disks, while many tablet groups must share future
persistence without conflating logical indexes.

## Accepted decision

`RaftNode` owns only deterministic role, term, vote, log, replication, commit, apply, and snapshot
metadata. Runtime election timeouts call `start_election`; messages are value types. A transition
containing persistent state must be durably installed before its outbound messages. Majority commit
uses the current-term restriction, and committed entries remain unavailable to application until
`mark_applied`.

`MultiRaftRuntime` multiplexes bounded groups on one owner and assigns node-global physical
sequences. It returns per-group persistence batches and group-tagged outbound messages. The durable
record boundary is [`multiplexed-raft-log-v1.md`](../formats/multiplexed-raft-log-v1.md). A dedicated
metadata state machine consumes only consecutive committed metadata-group indexes.

## Consequences and alternatives

Timers, transport, fsync batching, and application snapshot bytes remain outside the pure core.
ADR 0078 adds deterministic snapshot request, completion, and compaction transitions without
claiming external application installation.
[ADR 0071](0071-segmented-multi-raft-persistence.md) now owns segmented installation, append/sync
frontiers, and recovery around these records. An external Raft library and one physical fsync stream
per tablet were rejected under ADR 0010. Full-state records favor recoverability and auditability
over space; delta records may be a compatible future record type, not an unversioned reinterpretation.

## Affected invariants and validation

Invariants 1, 4–6, 8, 10–12, 14, and 17 apply. Focused deterministic tests cover 3-node election,
replication/commit, failover, stale-term rejection, restart catch-up, independent groups with
different leaders, node loss, reopen, metadata order, record round trip, and corruption. Focused disk
tests additionally cover rotation, reopen, explicit incomplete-tail repair, and corruption
rejection. Randomized simulation, partitions, application snapshot codecs, coordinated fsync
batching/crash testing, and production transport remain deferred.

**Retrospective note (2026-08-12):** [ADR 0252](0252-replayable-deterministic-raft-fault-simulator.md)
now supplies bounded explicit and seeded schedules for partitions, delay/reordering, duplication,
loss, crash/restart, atomic full-state persistence faults, membership, snapshots, safety checking,
replay, and deletion shrinking. Long/exhaustive campaigns, clock changes, and physical log syscall
faults remain deferred.
