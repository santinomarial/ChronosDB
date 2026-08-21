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
`mark_applied`. Recovery rejects retained log entries or installed snapshot metadata whose term is
newer than the checkpoint's current term. Term zero cannot carry a vote, and an index-zero snapshot
cannot carry term, manifest/checksum identity, configuration index, or voters. Logical index
`UINT64_MAX` remains reserved for exhaustion detection and cannot be recovered or installed.
Remote candidates cannot advertise that reserved value as an existing last-log index, and remote
leaders cannot name it as an AppendEntries predecessor or committed index.
AppendEntries responses cannot report it as the follower's actual last known match index.
InstallSnapshot responses cannot report it as the follower's installed snapshot boundary.
An AppendEntries sender outside the recipient's active configuration must supply a predecessor-
matching, semantically valid candidate suffix whose derived active voters include that sender.
Otherwise the request is rejected before its term can change local state. This permits a lagging
node to learn a valid joint/final configuration from a new-only leader without treating an
unproven nonvoter heartbeat as leadership authority.
The core also owns the exact aggregate byte budget of the version-1 persistent-state payload:
fixed state, snapshot voters, retained entry framing, and entry payloads. Recovery and every log- or
snapshot-changing transition must fit that budget before any persistent or volatile core state
changes. A capacity rejection is therefore retryable after compaction and cannot create state that
the full-state codec will reject only after mutation.

Every canonical higher-term response prepares an exact copy of its post-term persistent state
before changing term, vote, role, leader identity, replication state, or pending work. Allocation
or container-limit failure returns `RESOURCE_EXHAUSTED` with the complete node unchanged. Success
then demotes the node and returns that already owned state, so the durable runtime cannot lose the
persistence transition to a later allocation failure.

An admitted RequestVote request similarly computes its grant against the prospective term and vote
state, reserves the exact response, and owns any changed persistent state before publication. This
includes a higher-term grant or stale-log rejection and a same-term first vote. Allocation failure
therefore cannot advance the term, record a vote, change role, or omit the response required for an
exact retry.

Starting a local election is prepare-before-publish as well. The core first owns the prospective
term/vote state, self-vote set, complete outbound vote batch, and returned persistent-state copy.
If the self vote is already a quorum, it also prepares complete leader replication maps and the
initial heartbeat batch. Only non-throwing moves and scalar changes may then publish candidacy or
leadership. Allocation failure preserves the prior role, leader, pending work, and durable state.

Candidate vote-response handling follows the same rule. Before counting an admitted same-term
grant, the core owns the complete replacement vote set and computes its stable or joint quorum. If
the grant completes a quorum, it also owns the complete leader replication maps and initial
heartbeat batch. Allocation failure leaves that voter uncounted and the candidate unchanged, so an
exact retry cannot inherit partial leadership state.

Leader AppendEntries-response handling is also prepare-before-publish. A successful response owns
replacement match/next progress before deciding commit; if it advances commit, the core additionally
owns the derived membership, exact post-commit persistent state, adjusted replication maps, and
complete broadcast batch. A rejection owns its replacement next index and retry message together.
Allocation failure therefore publishes neither follower progress nor a durable commit, membership
change, leader removal, or retry rewind without the matching complete transition.

InstallSnapshot responses use the same progress boundary. A successful response prepares complete
replacement match/next maps and the retained-log follow-up before advancing the follower beyond the
snapshot. A rejected response owns its snapshot retry before returning. Resource exhaustion leaves
the old follower position intact in both cases and is returned as an explicit status rather than an
escaping allocation exception.

AppendEntries requests retain their validated candidate log, prospective commit, and derived
membership. Before publication, the core owns the exact stale or conflict feedback and any changed
post-term/post-suffix persistent state; an accepted request also owns its success response and commit
notification. Validation and preparation allocation failures return `RESOURCE_EXHAUSTED` without
changing term, role, leader identity, pending work, log, commit, or membership.

InstallSnapshot requests likewise own stale, already-installed, or competing-install feedback and
any changed post-term persistent state before publication. A newly admitted installation owns both
the core's pending request identity and the externally returned installation task before demotion or
pending-work publication. Allocation failure therefore preserves the exact role, leader identity,
pending work, and persistent state for retry.

Snapshot completion is prepare-before-publish on the follower side as well. Rejection owns its
negative response before releasing the pending identity. Success owns the retained suffix, derived
membership, exact in-memory and returned persistent states, commit notification, and acknowledgement
before installing any of them. Resource exhaustion preserves the pending identity and exact node
state so the same external completion remains retryable.

Local snapshot compaction prepares the canonical voter checkpoint, retained suffix, replacement
snapshot base, and both copies of the compacted persistent state before erasing any live log entry.
Allocation failure leaves the installed snapshot, retained log, membership base, and durable state
unchanged for exact retry.

Applied-index advancement also owns both the node's replacement persistent state and the returned
durable transition before changing application progress. Allocation failure cannot consume an
applied entry without the state required to persist that fact and remains exactly retryable.

`MultiRaftRuntime` multiplexes bounded groups on one owner and assigns node-global physical
sequences. It returns per-group persistence batches and group-tagged outbound messages. The durable
record boundary is [`multiplexed-raft-log-v1.md`](../formats/multiplexed-raft-log-v1.md). A dedicated
metadata state machine consumes only consecutive committed metadata-group indexes.
Once the recovered or emitted physical sequence reaches `UINT64_MAX`, the runtime fails closed
before invoking another group transition so no unpersistable in-memory state can be installed.
Its configured outbound batch must hold `max(1, maximum_voters - 1)` messages, the exact worst case
for one legal core transition; smaller configurations are rejected before any group exists.

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
rejection. An entry that individually meets the entry limit but would exceed the aggregate record
budget is rejected with exact state preservation. Recovery validation also rejects an installed
snapshot term above current term before a node can emit messages from impossible history. Focused
coverage rejects term-zero votes and every
nonzero external identity tested on an empty snapshot. Randomized simulation, partitions,
application snapshot codecs, coordinated fsync batching/crash testing, and production transport
remain deferred.

Dedicated allocation sweeps cover vote, append, snapshot, and read-barrier higher-term responses.
Every observed persistent-state-copy failure preserves exact leadership and durable state, while an
exact retry returns one follower demotion with the matching persistence transition.
RequestVote request sweeps separately cover higher-term grant/rejection and same-term first-vote
paths, requiring response and persistence preparation to fail before any node mutation.
Election-start sweeps cover a multi-voter leader with pending read work and a single-voter follower
that becomes leader immediately. Every observed allocation failure preserves exact state and retry
publishes the complete expected transition.
A five-voter vote-response sweep distinguishes a failed acknowledgement from a leaked one: a
different single grant remains below quorum after failure, while retrying the original grant
publishes complete leadership and all initial heartbeats.
Five-voter AppendEntries-response sweeps cover successful progress through commit and rejected
progress through rewind. Heartbeat output proves every failed allocation preserves the prior
per-follower position; commit state remains byte-for-byte unchanged until its persistent state and
complete broadcast are owned.
InstallSnapshot-response sweeps use a compacted leader with one retained suffix entry. A heartbeat
must continue selecting the snapshot after every failed success or rejection allocation; exact
retry then produces either the retained suffix or the same snapshot request.
AppendEntries-request sweeps separately cover stale rejection, higher-term predecessor conflict,
and higher-term replacement plus commit. Every observed validation, membership, persistent-state,
and response allocation preserves exact node state; retry publishes the complete expected outcome.
InstallSnapshot-request sweeps cover stale rejection, a higher-term already-installed
acknowledgement, and a higher-term new pending installation. Every observed response, persistent-
state, pending-identity, and returned-task allocation preserves exact leadership and pending work;
retry publishes the complete expected response or installation task.
Snapshot-completion sweeps separately cover explicit rejection and successful installation with a
retained suffix. Every observed response, suffix, membership, and persistent-state allocation
preserves the pending completion identity and exact node state; retry publishes the complete
negative response or installed-state transition.
The local-compaction sweep retains a nonempty suffix and fails every voter, state, and returned-
transition allocation. Each failure preserves the exact uncompacted state, and retry publishes the
complete compacted checkpoint and suffix together.
An applied-index sweep fails both post-apply state copies while two committed entries remain
available. Each failure preserves the original applied boundary and complete unapplied range; retry
returns the exact advanced state.

**Retrospective note (2026-08-12):** [ADR 0252](0252-replayable-deterministic-raft-fault-simulator.md)
now supplies bounded explicit and seeded schedules for partitions, delay/reordering, duplication,
loss, crash/restart, atomic full-state persistence faults, membership, snapshots, safety checking,
replay, and deletion shrinking. Long/exhaustive campaigns, clock changes, and physical log syscall
faults remain deferred.
