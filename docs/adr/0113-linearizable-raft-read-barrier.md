# ADR 0113: Linearizable Raft read barrier

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB distributed-systems maintainers

## Context

The deterministic Raft core prevented uncommitted application visibility but had no mechanism for a
leader to prove that it still held authority before serving a linearizable read. Local leadership,
a prior election result, or a leader's applied index alone cannot exclude a newer leader elected by
a quorum. Replication responses also drive follower catch-up state, so reusing synthetic
AppendEntries failures for read confirmation would couple read latency to log repair.

## Accepted decision

`RaftNode` supports one in-memory read barrier at a time. A leader may begin a barrier only after an
entry from its current term is committed. The barrier freezes the current committed index and the
stable voter set, or both old and new voter sets during joint consensus. It sends an explicit
`ReadBarrierRequest` with a nonzero term-local context to every other active voter. Valid recipients
adopt the request's current or newer leader term using the normal persist-before-response boundary
and return an accepted response. The recipient rejects a source outside its active voter union
before observing the request term; the learner exception for log and snapshot replication does not
grant leadership-probe authority. Before observing an admitted request term, the recipient owns the
response slot and, for a newer term, an exact copy of the post-observation persistent state.
Allocation or container-limit failure returns `RESOURCE_EXHAUSTED` without changing term, vote,
role, leader identity, or persistent state. A response from a higher term demotes the sender.

The leader counts only accepted responses in its current term from the frozen configuration and for
the exact pending context. Completion requires a stable majority or separate old and new
majorities. The returned `ReadBarrier` names the term, context, and committed index captured at
issuance. The caller must wait until `applied_index >= read_index` before acquiring or serving the
query snapshot; barrier completion never exposes committed-but-unapplied state.

Pending barriers are bounded to one and are abandoned on term, election, leadership, or leader
removal transitions. Starting or finalizing a membership change is rejected while a barrier is
pending, so a stable frozen quorum cannot become obsolete before completion. Contexts never wrap.
They need not be durable because a restarted node must win a later term before leading again, and
only an exact current pending term/context can complete.

Issuance prepares the frozen voter ownership, acknowledgement set, and complete outbound probe
batch before publishing either the pending barrier or the next context. Allocation or container-
limit failure returns `RESOURCE_EXHAUSTED` with no pending operation and no consumed context, so an
exact retry cannot be mistaken for a concurrent barrier and reuses the same context.
Recording an accepted response is also atomic: failure to allocate its acknowledgement returns
`RESOURCE_EXHAUSTED` without counting that voter or completing the barrier. Retrying the same exact
response is safe under the set's duplicate suppression.

## Consequences

- A successful barrier establishes a linearization point between issuance and quorum completion.
- Lagging voters can confirm leadership without modifying replication progress or first receiving a
  snapshot.
- Joint membership preserves its two-quorum safety rule for reads as well as election and commit.
- Allocation failure during issuance or acknowledgement is retryable and cannot leave a hidden
  pending barrier, partial quorum observation, or gap in the term-local context sequence.
- Recipient response preparation fails before term observation, preserving the durable owner's
  ability to synchronize every admitted higher-term transition before sending its response.
- Production transport framing and binding the returned index to a tablet snapshot remain separate
  integration work; the in-memory value messages are not a released wire format.

## Alternatives considered

- **Serve from leader/applied state without a quorum round:** cannot exclude a newer leader.
- **Use a time-based leader lease:** requires bounded clock-drift and timer assumptions absent from
  the deterministic core.
- **Attach contexts to AppendEntries:** valid, but unnecessarily mixes leadership confirmation with
  next/match-index repair and snapshot catch-up.
- **Append one log entry per read:** safe after commit but adds log, synchronization, and replication
  work to every read.

## Affected invariants and validation

Invariants 4–6, 8, 14, and 18 apply. Focused tests cover the current-term commit prerequisite,
context matching, apply gating, stable and frozen joint quorums, higher-term demotion, stale response
rejection after reelection, recipient persistence, nonvoter request rejection before higher-term
observation, single-voter completion, and allocation failure at every observed issuance allocation.
The allocation sweep requires exact role, term, leader, persistent-state, and retry-context
preservation until the complete transition can be published. A joint-quorum response sweep further
requires failed acknowledgement allocation to leave the voter uncounted, then proves exact retry
completion only after both frozen majorities exist. A recipient sweep fails every response and
persistent-state-copy allocation before higher-term observation, requires exact node-state
preservation, and then validates the complete retry transition. Production wire
versioning, tablet snapshot acquisition, exhaustive schedules, partitions, duplication, restart,
and long randomized simulation remain Phase 14 integration and hardening work.
