# Tablet Reconfiguration Across Raft and Metadata

## Purpose and interface

`TabletReconfigurationCoordinator` turns one snapshot-complete, caught-up `TabletMovement` into
exact durable operations for the tablet and metadata Raft groups. `reconcile` observes a
`RaftNode` plus applied `MetadataStateMachine` and returns either one action, no action while a
commit is in flight, or an explicit inconsistency.

Construction is restartable from every post-catch-up durable phase. Ready resumes target promotion,
target-promoted resumes source removal without replaying promotion, and complete reconstructs a
terminal coordinator that emits no action.

## State and invariants

There are two authoritative views. Tablet Raft membership decides which replicas can commit data.
Metadata placement decides where clients and coordinators route. Local movement phase is only a
checkpoint of those authorities and never evidence by itself.

Promotion goes old stable -> old/new joint -> promoted stable -> promoted placement epoch. Removal
goes promoted stable -> promoted/final joint -> final stable -> final placement epoch. The local
phase advances after each corresponding placement is applied. Every voter vector is sorted and
exact-compared, including separate joint old/new sets.

## Ownership and failure behavior

The coordinator uniquely owns `TabletMovement`; it borrows Raft and metadata only for one call. The
membership spans are node-owned and cannot be retained across a transition. Returned requests own
their vectors and encoded metadata bytes.

One action must be executed at a time. A joint entry that is uncommitted or a final entry already
pending yields no action. A follower cannot start/finalize membership. Divergent epochs, tables,
replicas, or joint intent fail as corruption and do not mutate movement. Metadata publication may
be routed to a different node that leads the metadata group.

Every action also owns a stable ID containing tablet, current movement epoch, and kind. Because the
ID is derived from checkpointed movement state, restart reconstructs the same pending ID without a
separate allocation record. The ID identifies retry intent; a routing owner must still persist
in-flight/completion evidence and exact-match the request before suppressing a duplicate.

`encode_tablet_reconfiguration_action_v1` turns that intent into bounded canonical bytes containing
the destination group and exact supported operation. Decode revalidates the stable identity,
operation-kind agreement, canonical voter set, and nested placement command before a retry owner can
dispatch it.

`TabletReconfigurationActionLedger::prepare` installs those exact bytes under the tablet's exclusive
lock before dispatch. A same-ID retry succeeds only for byte-identical content. Reopen discards only
canonical temporaries, while immutable final actions remain evidence; the reconciler's observed
Raft/metadata state decides whether any prepared action still needs execution.

## Complexity and tradeoffs

Reconciliation is linear in the bounded replica count. The explicit two-group handoff adds control-
plane latency but avoids treating routing intent as consensus. Durable movement generations and
deterministic action identities now make restart reconstruction exact; production leader routing,
retry-ledger consumption, and failure injection remain separate work.

## Likely interview questions

- Why must placement follow, rather than precede, Raft finalization?
- Why compare joint old/new sets instead of only their union?
- Why does local movement phase not prove membership?
- What happens when the source is the leader being removed?
- What must be durable before automatic reconciliation can resume after restart?
- Why is a stable action identity not itself proof that the action committed?
