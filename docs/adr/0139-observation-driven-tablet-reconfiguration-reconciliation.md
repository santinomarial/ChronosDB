# ADR 0139: Observation-driven tablet reconfiguration reconciliation

- **Status:** accepted
- **Date:** 2026-08-10
- **Owners:** ChronosDB distributed-systems and metadata maintainers
- **Extends:** [ADR 0133](0133-prepared-tablet-reconfiguration-dispatch.md),
  [ADR 0135](0135-bounded-asynchronous-prepared-reconfiguration-admission.md), and
  [ADR 0138](0138-fifo-ordered-raft-group-observation.md)

## Context

Prepared reconfiguration actions can enter the asynchronous durable owner, and that owner can now
return a FIFO-ordered owning group observation. Production reconciliation still accepted only a
borrowed `RaftNode`, which belongs to the worker and cannot cross threads. Consequently a producer
could execute safely but could not feed the resulting committed membership state back into the
durable checkpoint-and-prepare path without violating owner affinity.

Completion cannot be inferred from successful admission, local synchronization, or a retained log
entry. Membership completion is authoritative only in committed Raft configuration state. Placement
completion is authoritative only when the applied metadata state machine exposes the expected epoch
and replicas.

## Decision

`TabletReconfigurationCoordinator::reconcile` accepts either its existing node-local `RaftNode`
view or an owning `RaftGroupObservation`. Both paths reduce to the same internal immutable view and
the same deterministic state machine.

An observation is accepted only when it names the configured tablet group and has coherent bounded
semantics: nonzero local identity; recognized role; ordered `applied <= commit <= last` indexes;
canonical nonzero stable and active voters; leader self-identity in leader role; and exact stable-
versus-joint voter/flag relationships. A foreign or
fabricated inconsistent value returns `INVALID_ARGUMENT` before movement, checkpoint, or ledger
state changes.

The production `reconcile_and_prepare_durable_tablet_reconfiguration` overloads accept this
observation for both self-contained and external-prefix movement generations. They preserve the
existing order: reconcile authoritative committed membership plus applied metadata, install any
representation-preserving phase checkpoint, then durably prepare the exact next action in the
ledger before releasing its sealed dispatch.

If an observed membership entry is still uncommitted, reconciliation emits no next action. Once the
committed observation makes joint finalization legal, retry deterministically prepares the finalize
action. After stable membership and applied placement become visible, the existing coordinator
advances the durable movement phase or emits the next placement action. No mutable completion bit
is introduced.

## Detailed rationale

The existing coordinator already encodes the correct postconditions for every begin, finalize, and
placement action. Reusing it prevents a second completion state machine and keeps the metadata
application value—not a Raft applied index alone—as placement authority. Semantic validation at the
owning-value boundary prevents public aggregate construction from bypassing node invariants.

An observation remains a point-in-order fact, not a lease. A later leader change may make a newly
prepared local action unavailable; exact retained retry and future authenticated routing handle
that independently. Reconciliation is therefore explicitly retry-driven.

## Alternatives considered

- Borrow the worker's `RaftNode` after completion was rejected because it violates thread affinity
  and lifetime ownership.
- Mark the ledger action complete after durable execution was rejected because local persistence is
  not commit or application.
- Trust only commit/applied indexes was rejected because they do not identify the expected
  membership or metadata placement value.
- Add a mutable completion record was rejected because authoritative Raft and metadata state would
  become one of two competing truths.

## Consequences

Local asynchronous reconfiguration can now close the loop from prepared admission through observed
commit/application state into the next durable checkpoint and sealed action. Pending work remains a
clean no-dispatch result; wrong-group or incoherent observations fail without side effects.

This decision does not authenticate or transmit remote actions, keep an observation current, apply
metadata on the worker, or reclaim completed ledger and checkpoint evidence.

## Affected invariants

Invariants 1, 4, 5, 8, 9, 11, 14, and 18 apply. No execution boundary is mistaken for application,
authoritative state drives phase changes, retry remains idempotent, and worker-owned memory never
escapes as a borrowed reference.

## Validation plan

A real-filesystem async test elects a single-owner group, obtains its owning observation, prepares
and admits the begin-joint action, observes no next action while it is uncommitted, submits the
follower replication response, observes committed joint state, rejects the same value under a
foreign group identity, and prepares the exact finalize dispatch. Reopen proves the committed log
boundary. Existing node-based coordinator and full Raft suites remain required.

## Migration or rollback considerations

No durable or wire bytes change. Existing node-local callers retain their overload. Older binaries
can recover every checkpoint, action, and Raft record produced by the observation-driven path.
Rollback must preserve worker affinity and may use an equivalent owning snapshot adapter.

## Unresolved questions

Authenticated current-leader transport, remote duplicate delivery and retry/backoff, automatic
metadata apply scheduling, completion-to-reactor continuations, and safe evidence reclamation remain
Phase 16 work.

## References

- [FIFO-ordered observation ADR](0138-fifo-ordered-raft-group-observation.md)
- [Tablet reconfiguration learning guide](../learning/tablet-reconfiguration.md)
- [Tablet Reconfiguration Action v1](../formats/tablet-reconfiguration-action-v1.md)
- [Phase 16 roadmap](../roadmap.md#phase-16--distributed-query-execution-and-rebalancing)
