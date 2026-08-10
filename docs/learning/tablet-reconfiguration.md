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

`reconcile_durable_tablet_reconfiguration` runs that reconciler against a private candidate copied
from a recovered movement generation. Emitting an action, waiting, or observing terminal state does
not write a generation. When committed authorities advance ready to target-promoted or
target-promoted to complete, it installs generation +1 before replacing the live movement. The
adapter preserves self-contained versus external-prefix representation; an external phase change
therefore requires and exact-revalidates the original chunk owner. Failed installation leaves the
live phase and generation unchanged.

Production routing uses `reconcile_and_prepare_durable_tablet_reconfiguration`, which releases no
bare action. It returns a sealed, move-only capability bundling each action with the matching durable
ledger preparation receipt. When a call both checkpoints target promotion and emits source-removal
work, checkpoint installation precedes ledger preparation. A ledger failure therefore leaves the
new phase durable but returns no dispatch; retry reconstructs the same action identity and prepares
it idempotently.

`execute_local_prepared_tablet_reconfiguration` accepts only that capability and submits its exact
request to the synchronous durable Multi-Raft owner. The result and any outbound messages are
released after local log synchronization. The per-operation status still must be checked, and local
durability is not quorum commit or state-machine application; transport and another reconciliation
remain mandatory.

`try_submit_local_prepared_tablet_reconfiguration` provides the nonblocking production admission
path to the asynchronous single-owner runtime. Capacity or shutdown rejection leaves the capability
valid. Successful admission returns an owning completion; reactor threads poll or hand it off, and
only the completed durable result may release outbound messages.

The same owner now accepts FIFO-ordered bounded group observations. Reconciliation code can inspect
an owning role/term/commit/apply/membership snapshot after admitted work without borrowing the
worker-owned `RaftNode`. The observation is not a leader lease or application proof; authoritative
metadata and movement reconciliation still decide the next action or phase.

Production reconciliation accepts that owning observation after validating its tablet-group
identity, ordered frontiers, canonical voters, and exact stable/joint relationships. Uncommitted
membership remains a no-dispatch wait. Committed membership plus applied metadata feeds the same
checkpoint-first, ledger-prepare-second transition used by the node-local path, so asynchronous
completion never needs a second mutable truth.

Placement execution uses an explicit exact-retained proposal operation. If the identical canonical
metadata command is already committed or retained in the current term, retry returns an empty
successful transition without another append or synchronization. An uncommitted prior-term match
adds or reuses an empty current-term Raft progress entry, allowing the retained action to commit
without duplicating the placement epoch. Membership begin and finalize retries use the same
retained-intent rule.

## Complexity and tradeoffs

Reconciliation is linear in the bounded replica count. The explicit two-group handoff adds control-
plane latency but avoids treating routing intent as consensus. Durable checkpoints before every
post-catch-up live phase adoption and deterministic action identities make restart reconstruction
exact; production leader routing, retry-ledger consumption, and failure injection remain separate
work.

## Likely interview questions

- Why must placement follow, rather than precede, Raft finalization?
- Why compare joint old/new sets instead of only their union?
- Why does local movement phase not prove membership?
- What happens when the source is the leader being removed?
- What must be durable before automatic reconciliation can resume after restart?
- Why is a stable action identity not itself proof that the action committed?
