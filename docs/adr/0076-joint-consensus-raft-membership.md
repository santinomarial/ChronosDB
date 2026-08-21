# ADR 0076: Joint-consensus Raft membership

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** ChronosDB distributed-systems maintainers
- **Extended by:** [ADR 0136](0136-idempotent-retained-reconfiguration-action-replay.md) and
  [ADR 0137](0137-current-term-raft-progress-noop.md)

## Context

Raft groups previously used a fixed bootstrap voter list. Placement metadata and the learner-first
tablet movement state machine could describe desired replicas, but directly replacing the voter
list would allow disjoint old and new majorities to commit conflicting histories. Membership also
needed canonical durable identity, restart reconstruction, and explicit application behavior.

## Accepted decision

Membership changes use two versioned, checksummed Raft-internal entries. The joint entry contains
the exact committed old voter set and desired new set. Accepting that entry activates the union and
requires separate old-majority and new-majority votes and replication acknowledgments. The final
entry may be proposed only after the joint entry commits, repeats its new set and joint index, and
also commits under both majorities. After final commit only the new set remains active. One change
may be in flight per group.

The bootstrap voter list remains external group configuration. A node need not initially be a
voter, which permits a learner target to accept log replication, but a nonvoter cannot campaign or
grant a vote. That learner exception covers AppendEntries and snapshot installation only; a
nonvoter cannot issue a leadership read-barrier probe. An AppendEntries sender not already active
must establish its authority through a predecessor-matching, valid candidate suffix that derives
the sender into the active voter union before the recipient observes its term. Recovery
deterministically derives
membership from the complete retained log and rejects corrupt or impossible transitions. Generic
application proposals cannot use the reserved
entry types. Metadata and tablet application owners advance over membership entries as internal
no-ops. A removed leader propagates its final commit transition and then steps down.
The configured voter bound must fit Membership Command v1's unsigned 16-bit count fields. Values
above 65,535 fail node construction rather than creating a runtime whose membership operations can
never be encoded.

Both membership append operations are prepare-before-publish transitions. The leader constructs a
prospective complete node and owns the encoded command, derived active configuration, expanded
replication maps, complete outbound batch, and returned persistent state before replacing its live
state. Resource exhaustion therefore cannot expose a joint configuration without its replication
messages or expose a pending final entry without its complete durable transition.

## Consequences and alternatives

No old-only or new-only quorum can finish the transition, including elections during joint state.
Adding and removing replicas therefore remains safe across restart and leader replacement when the
durable log and authenticated transport assumptions hold. Availability temporarily requires both
majorities, which is the intended cost of avoiding split-brain configuration changes.

Direct voter-list replacement and one-entry membership were rejected because they permit disjoint
quorums. Encoding membership only in placement metadata was rejected because catalog intent does
not alter Raft's commit rule. Multiple concurrent changes were rejected because they make quorum
identity and recovery substantially harder to audit.

Snapshot installation must later carry the membership checkpoint that precedes a compacted log.
The existing learner movement state machine is not automatically coupled to these commands; its
owner must durably coordinate catch-up, joint promotion, finalization, placement epoch, and source
cleanup. Protocol exposure, authenticated transport, exhaustive schedule exploration, and crash
matrices remain required before the Phase 14/15 exit gates can be claimed.

## Affected invariants and validation

Invariants 4, 8, 9, 10, 13, 14, and 18 apply. Focused tests cover canonical encoding and damage,
old/new commit and election quorums, premature-final rejection, leader removal, learner election
rejection, invalid-history preflight, durable reopen, Multi-Raft operations, and ordered tablet and
metadata application across internal entries. Limit coverage rejects an unencodable configured
voter bound at node construction and accepts the exact format maximum. A higher-term read-barrier
request and an unproven higher-term AppendEntries heartbeat from a nonvoter are rejected before
term, role, leader identity, or persistent state changes. A suffix carrying the valid joint/final
transition from a new-only leader remains accepted. Allocation sweeps for joint and final proposal
publication require every resource-exhaustion result to preserve the exact stable or joint leader;
the identical retry then appends once at the same index and returns the complete active-peer batch.
