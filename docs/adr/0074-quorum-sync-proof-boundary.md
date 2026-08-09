# ADR 0074: Raft quorum-synchronization proof boundary

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** ChronosDB durability and distributed-systems maintainers

## Context

ADR 0006 defines `QUORUM_SYNC` as acknowledgment after a voting majority meets the documented
persistence condition. The deterministic Raft core advances commit after majority replication, and
`DurableMultiRaftRuntime` withholds every outbound response until its state-changing transition is
covered by local synchronization. The missing boundary was a checked proof tying one committed log
index to those facts and, separately, to tablet state-machine application.

## Accepted decision

For the current active committed or joint voting configuration and non-Byzantine authenticated
transport model, a
successful `AppendEntriesResponse` released by `DurableMultiRaftRuntime` proves that the follower's
full persistent state containing that entry crossed its local synchronization frontier. A leader
that advances and locally synchronizes `commit_index` after enough such responses may issue a
`QuorumSyncReceipt` for any retained committed entry.

`prove_quorum_sync(group, index)` succeeds only on the current leader, for a nonzero index at or
below its committed frontier, while the durable runtime is healthy and can identify the retained
entry term. The receipt names the group, leader node and term, log index and entry term, and the
leader's covering local physical durability sequence. It is immutable evidence, not a lease and not
permission for a stale leader to acknowledge later writes.

Storage commitment alone is not query visibility. `RaftTabletStateMachine::prove_applied_quorum_sync`
additionally requires the Raft applied index and the tablet's Raft group/index publication frontier
to cover the requested entry. A replicated write path may acknowledge `QUORUM_SYNC` only after this
composed proof succeeds and must report the requested and effective mode without downgrade.

## Assumptions and exclusions

The proof assumes correctly configured voter identities; the joint-consensus transition from
[ADR 0076](0076-joint-consensus-raft-membership.md); crash-fault, not Byzantine, behavior;
authenticated message source identity; and the local persistence contract of each runtime. Forged
responses, correlated storage loss, a lying device/controller, bypassing the membership protocol,
or loss of a required old/new majority are outside the guarantee.

The receipt is currently an internal API. Native protocol negotiation, response fields, metrics,
timeouts, cancellation, and end-to-end client exposure remain separate integration work and must
not advertise `QUORUM_SYNC` before they preserve this exact boundary.

## Consequences and alternatives

The implementation can distinguish locally durable append from majority-durable commit and can
prevent acknowledgment before application. It does not need a second follower-ack tracker outside
Raft because commit advancement already embodies the majority calculation. Requiring all replicas
was rejected because it weakens availability without strengthening the declared minority-failure
contract. Treating message receipt as durable was rejected because it bypasses follower sync.

## Affected invariants and validation

Invariants 1, 4, 5, 8, 9, 10, 14, and 18 apply. A focused three-runtime test proves that the leader
cannot produce a receipt after only local append, that a follower response is released after its
durability frontier advances, that the synchronized leader commit unlocks the receipt, and that a
follower cannot issue it. Tablet tests prove that the composed application receipt is unavailable
before publication. Authenticated production transport, explicit receipt configuration identity,
crash matrices, minority-loss recovery reconciliation, and client protocol exposure remain required.
