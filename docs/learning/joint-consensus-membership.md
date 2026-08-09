# Joint-consensus membership

## Purpose and public interface

`RaftNode::begin_membership_change` starts one transition from the committed voter set to a desired
set. `finalize_membership_change` is available only after the joint entry commits. The Multi-Raft
and durable runtimes expose matching operations, so membership state crosses the same
persist-before-send boundary as terms, votes, log entries, and commit indexes.

## Data structures and invariants

The bootstrap configuration is immutable input. Log replay derives a committed voter set plus an
optional joint configuration containing old voters, new voters, joint index, and whether a final
entry is pending. All arrays are sorted, unique, nonzero, bounded, and encoded by
[Raft Membership Command v1](../formats/raft-membership-command-v1.md).

During joint state, the active peer set is `old ∪ new`, but a quorum is not a majority of that
union. Elections and commit advancement each require `majority(old) AND majority(new)`. This is the
central safety property: any decision intersects the decision quorums on both sides of the change.
Only the final entry switches the committed configuration to `new`.

## Ownership, recovery, and failure behavior

`RaftNode` owns the derived membership and its leader replication indexes. The caller owns clocks,
transport, and persistence. A returned persistent transition must be synchronized before outbound
messages are released. Followers preflight the complete candidate log and prospective commit before
mutating persistent state, so damaged or impossible membership histories fail without a partial
installation. Reopen derives the same active set from the retained log.

A bootstrap learner can receive replication but cannot start an election or grant a vote. New peers
receive replication as soon as the joint entry is appended. A leader excluded from the final set
sends the final commit update to new peers, clears leader-only state, and becomes a follower.

Application owners never interpret membership payloads as row or catalog commands. They validate
application entries, treat membership entries as ordered internal no-ops, and persist the final
applied index only after the whole batch succeeds.

## Complexity and tradeoffs

Quorum checks are linear in the bounded voter count. Membership derivation is linear in retained
log entries and decodes only reserved membership entries. Full replay is intentionally simple and
auditable; snapshot membership checkpoints will be necessary before shared-log reclamation.
Joint consensus temporarily reduces availability because both configurations must form majorities.
ChronosDB accepts that cost in preference to unsafe direct replacement.

## Likely interview questions

- Why is a majority of the union not equivalent to majorities of both configurations?
- Why does the joint configuration become active when appended rather than only when committed?
- Why must the final entry itself commit under joint rules?
- How can a new learner receive entries without being allowed to campaign?
- What membership state must a future Raft snapshot preserve?
