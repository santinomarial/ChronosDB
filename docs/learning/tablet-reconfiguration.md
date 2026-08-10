# Tablet Reconfiguration Across Raft and Metadata

## Purpose and interface

`TabletReconfigurationCoordinator` turns one snapshot-complete, caught-up `TabletMovement` into
exact durable operations for the tablet and metadata Raft groups. `reconcile` observes a
`RaftNode` plus applied `MetadataStateMachine` and returns either one action, no action while a
commit is in flight, or an explicit inconsistency.

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

## Complexity and tradeoffs

Reconciliation is linear in the bounded replica count. The explicit two-group handoff adds control-
plane latency but avoids treating routing intent as consensus. The current coordinator state is
in-memory; durable intent and idempotent action identities are required for process-restart
automation.

## Likely interview questions

- Why must placement follow, rather than precede, Raft finalization?
- Why compare joint old/new sets instead of only their union?
- Why does local movement phase not prove membership?
- What happens when the source is the leader being removed?
- What must be durable before automatic reconciliation can resume after restart?
