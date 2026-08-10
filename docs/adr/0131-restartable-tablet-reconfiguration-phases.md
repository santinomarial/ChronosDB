# ADR 0131: Restartable tablet reconfiguration phases

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB distributed-systems and metadata maintainers
- **Extends:** [ADR 0116](0116-raft-metadata-tablet-reconfiguration.md) and
  [ADR 0130](0130-durable-tablet-movement-ready-reconciliation.md)

## Context

Movement checkpoints can durably record `kTargetPromoted` after the target membership and placement
epoch commit. The original reconfiguration constructor accepted only `kReady`, so restart at that
valid durable boundary could not resume source removal. A complete checkpoint likewise could not be
reconstructed as a terminal coordinator even though reconciliation already defined its behavior.

## Decision

`TabletReconfigurationCoordinator::create` accepts every post-catch-up orchestration phase:
`kReady`, `kTargetPromoted`, and `kComplete`. All existing group/table/epoch/leader-hint validation
still applies. Reconciliation from target-promoted exact-compares the promoted stable membership and
placement before emitting or continuing the source-removal joint-consensus sequence. Reconciliation
from complete returns no action after confirming the matching tablet placement exists.

Epoch headroom is phase-specific: ready reserves two increments, target-promoted reserves the one
remaining removal increment, and complete may occupy the maximum epoch because it cannot advance.

Earlier phases remain invalid because snapshot transfer and ready checkpoint reconciliation have
different owners. No action identity, Raft request, metadata command, or durable format changes.

## Rationale and alternatives

Rewinding a promoted checkpoint to ready was rejected because it would repeat target promotion
against a newer placement epoch. Requiring one long-lived coordinator across process lifetime was
rejected because all orchestration state is explicitly designed for durable reconstruction.

## Consequences and validation

A restarted process can continue source removal from the exact durable movement phase, and a
complete movement is observably terminal. Focused tests reconstruct promoted membership/placement,
elect a valid leader, and prove the exact removal action identity and final voters; they also reopen
complete state and prove no action. Crash injection during the later action sequence and durable
checkpoint composition for promoted/complete phase changes remain follow-up work.

Invariants 4, 5, 8, 11, 14, and 18 apply.

## Migration and rollback

No durable bytes change. Older binaries continue to fail closed when asked to create a coordinator
from a later phase; upgraded binaries resume it without migration.

## References

- [Tablet reconfiguration learning guide](../learning/tablet-reconfiguration.md)
- [Phase 16 roadmap](../roadmap.md#phase-16--distributed-query-execution-and-rebalancing)
