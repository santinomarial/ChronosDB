# ADR 0133: Prepared tablet reconfiguration dispatch boundary

- **Status:** accepted
- **Date:** 2026-08-10
- **Owners:** ChronosDB distributed-systems and storage maintainers
- **Extends:** [ADR 0121](0121-durable-tablet-reconfiguration-action-ledger.md) and
  [ADR 0132](0132-durable-tablet-reconfiguration-phase-checkpoints.md)
- **Extended by:** [ADR 0134](0134-sealed-local-tablet-reconfiguration-execution.md) and
  [ADR 0139](0139-observation-driven-tablet-reconfiguration-reconciliation.md)

## Context

The durable reconfiguration adapter can reconstruct one exact action, and the tablet-bound action
ledger can durably prepare that action before dispatch. Leaving those as unrelated calls permits a
caller to accidentally send the in-memory action first. Production orchestration needs an interface
whose successful action result itself proves that the exact deterministic identity and request bytes
crossed the ledger directory-sync boundary.

A single reconciliation may also observe target promotion, install its next movement generation,
and emit the first source-removal action. These are two ordered durable boundaries rather than one
atomic filesystem transaction.

## Decision

`reconcile_and_prepare_durable_tablet_reconfiguration` composes authoritative reconciliation,
movement phase checkpointing, and action-ledger preparation. It returns no bare action. An emitted
action is available only as `PreparedTabletReconfigurationDispatch`, bundled with the matching
`PreparedTabletReconfigurationAction` receipt from the tablet-bound ledger. Callers may dispatch
only that bundled action.

The function first performs durable movement reconciliation. If reconciliation returns no action,
the result contains only any installed phase checkpoint and the ledger is untouched. If an action
is returned, the function calls `prepare` and releases the bundle only after exact encoding,
readback, file sync, no-replace rename, and directory sync succeed. Same-identity/same-bytes retry is
successful and reports `already_present`; same-identity/different-bytes fails as corruption.

When a call both advances movement phase and emits the next action, the movement generation is
installed and adopted before ledger preparation. If ledger preparation then fails, the call returns
that error while the new durable and live movement phase remains authoritative. Retry reconciles
from that phase, reconstructs the same next action identity, and retries preparation. No action is
returned on the failed attempt.

## Detailed rationale

The wrapper makes prepare-before-dispatch the easiest usable interface without coupling filesystem
owners or inventing a cross-directory transaction. Checkpoint-first ordering follows the logical
dependency: the next action identity derives from the newly proven phase and epoch. Deterministic
identity makes recovery across the two durability boundaries idempotent.

## Alternatives considered

- Documenting two caller-side calls was rejected because the type system would still allow the raw
  action to escape before preparation.
- Preparing the next action before installing its phase checkpoint was rejected because the action
  identity derives from state not yet durably adopted.
- Combining movement generations and actions in one file was rejected because it would replace two
  established tablet-bound formats and owners with a new transaction format.
- Rolling back the phase after ledger failure was rejected because durable immutable generations
  cannot be rolled back and the authoritative Raft/metadata transition already occurred.

## Consequences

Production routing can accept only `PreparedTabletReconfigurationDispatch` and need not infer
whether preparation happened. A preparation failure after phase installation is intentionally
visible as an error plus the advanced recovered owner; callers must retry reconciliation rather than
reuse a saved bare action.

This boundary does not dispatch, authenticate, route to the current leader, record mutable
completion, delete ledger evidence, or prove Raft/metadata application.

## Affected invariants

Invariants 1, 4, 8, 9, 10, 11, 14, and 18 apply. Exact action identity and bytes become durable
before release to transport, immutable conflicts remain detectable, and an already durable movement
phase is never reversed after a later preparation failure.

## Validation plan

Real-filesystem tests prove that an unchanged-phase action is returned only with a new ledger
receipt, exact retry reports the existing receipt, a phase-advancing call installs generation 2 and
then prepares its epoch-8 action, and a same-ID conflict returns no dispatch while retaining the
installed phase checkpoint. Existing ledger and full Raft suites remain required.

## Migration or rollback considerations

No durable or wire bytes change. Existing raw reconciliation remains available for tests and lower-
level recovery composition, but production routing must use the prepared wrapper. Rollback must
preserve an equivalent explicit prepare-before-dispatch call sequence.

## Unresolved questions

Authenticated leader transport, duplicate-delivery application reconciliation, completion-evidence
reclamation, and multi-owner process-kill fault injection remain Phase 16 work.

## References

- [Tablet Reconfiguration Action v1 format](../formats/tablet-reconfiguration-action-v1.md)
- [Tablet reconfiguration learning guide](../learning/tablet-reconfiguration.md)
- [Phase 16 roadmap](../roadmap.md#phase-16--distributed-query-execution-and-rebalancing)
