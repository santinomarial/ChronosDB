# ADR 0132: Durable tablet reconfiguration phase checkpoints

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB distributed-systems and metadata maintainers
- **Extends:** [ADR 0116](0116-raft-metadata-tablet-reconfiguration.md),
  [ADR 0130](0130-durable-tablet-movement-ready-reconciliation.md), and
  [ADR 0131](0131-restartable-tablet-reconfiguration-phases.md)
- **Extended by:** [ADR 0133](0133-prepared-tablet-reconfiguration-dispatch.md)

## Context

The reconfiguration reconciler advances a ready movement to target-promoted only after committed
tablet membership and metadata placement prove promotion, and advances target-promoted to complete
only after those authorities prove source removal. Those transitions originally changed the live
`TabletMovement` without first installing a new movement generation. A crash could therefore reopen
the preceding phase and repeat already-completed orchestration. Conversely, reconciliation calls
that merely emit an action or wait for a commit do not change durable movement state and must not
require access to snapshot chunks.

## Decision

`reconcile_durable_tablet_reconfiguration` evaluates reconciliation against a private candidate
movement copied from the recovered generation. If the candidate remains in the same phase, the
adapter returns the action or wait result without installing a checkpoint, requiring a chunk owner,
or consuming generation headroom.

Only ready to target-promoted and target-promoted to complete are valid candidate phase changes.
Before either change reaches the supplied live movement, the adapter installs generation
`current + 1` containing the candidate record. A self-contained generation remains self-contained.
An external-prefix generation remains an external reference and exact-revalidates its tablet,
session epoch, boundaries, chunk CRCs, and whole-snapshot CRC against the supplied durable chunk
owner. Generation exhaustion and a missing external chunk owner fail before installation and leave
the live movement unchanged.

After a successful durable install, the adapter transfers ownership of the candidate movement into
the recovered generation through an rvalue-only, `noexcept` operation and updates its generation
number. This avoids replaying reconciliation or allocating after the durable boundary. The returned
action, including a source-removal action returned with a newly durable target-promoted phase, is
still only intent: a routing owner must prepare its exact bytes in the durable action ledger before
dispatch.

## Detailed rationale

Installing before live adoption makes an I/O failure retryable without memory getting ahead of
disk. Candidate reconciliation isolates all earlier validation and mutation from the authoritative
owner. Preserving the existing checkpoint representation avoids silently adding or removing a
durable chunk-lifetime dependency. Deferring the chunk and generation checks until a phase actually
changes keeps read-only reconciliation usable at terminal, waiting, and action-emission states.

## Alternatives considered

- Checkpoint after changing the live movement was rejected because installation failure would leave
  memory ahead of recoverable state.
- Always require the chunk owner for external movements was rejected because unchanged
  reconciliation neither reads nor rewrites the prefix.
- Rewrite every generation as self-contained was rejected because it duplicates the durable prefix
  and changes ownership semantics.
- Re-run reconciliation after checkpoint installation was rejected because the authorities can
  change and because post-durability allocation or validation failure would complicate recovery.

## Consequences

Every authoritative promotion or completion observation becomes a recoverable movement boundary
before subsequent work is exposed. Candidate construction temporarily copies the bounded received
prefix. The caller must serialize access to the movement, checkpoint owner, chunk owner, Raft node,
and metadata state for the duration of a call.

This adapter does not prepare or dispatch actions, consume completed ledger entries, route to
leaders, transfer physical Manifest/CSEG files, or reclaim checkpoint generations and chunks.

## Affected invariants

Invariants 1, 4, 5, 8, 10, 11, 14, and 18 apply. Durable generations remain monotonic and
checksummed; exact authoritative state precedes orchestration advancement; failed installation
cannot expose partially adopted live state; and external bytes retain their original owner.

## Validation plan

Real-filesystem tests cover external ready-to-promoted-to-complete generations and reopen, action
emission without a chunk owner when no phase changes, refusal of an external phase change without
that owner, representation preservation for self-contained checkpoints, and an immutable generation
conflict that leaves the live movement unchanged. Broader process-kill and filesystem fault
injection remains part of Phase 16 validation.

## Migration or rollback considerations

No durable or wire format changes. Older binaries can read every generation installed by this
adapter, but do not provide the checkpoint-before-live orchestration guarantee. Rollback must
therefore stop automatic reconfiguration or retain an equivalent durable adapter.

## Unresolved questions

The production action-ledger/transport owner, physical data handoff, and safe generation/chunk
reclamation remain unresolved Phase 16 integration work.

## References

- [Tablet reconfiguration learning guide](../learning/tablet-reconfiguration.md)
- [Tablet movement checkpoint learning guide](../learning/tablet-movement-checkpoint.md)
- [Phase 16 roadmap](../roadmap.md#phase-16--distributed-query-execution-and-rebalancing)
