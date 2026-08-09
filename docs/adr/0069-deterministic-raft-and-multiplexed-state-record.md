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

Timers, transport, segmented file installation, fsync batching, snapshots, and application wiring
remain outside the pure core. An external Raft library and one physical fsync stream per tablet were
rejected under ADRs 0010–0011. Full-state records favor recoverability and auditability over space;
delta records may be a compatible future record type, not an unversioned reinterpretation.

## Affected invariants and validation

Invariants 1, 4–6, 8, 10–12, 14, and 17 apply. Focused deterministic tests cover 3-node election,
replication/commit, failover, stale-term rejection, restart catch-up, independent groups with
different leaders, node loss, reopen, metadata order, record round trip, and corruption. Disk,
randomized simulation, partitions, snapshots, membership changes, and QUORUM_SYNC are deferred.

