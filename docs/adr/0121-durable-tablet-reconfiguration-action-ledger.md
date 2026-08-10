# ADR 0121: Durable tablet reconfiguration action ledger

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB distributed-systems and storage maintainers
- **Extends:** [ADR 0120](0120-tablet-reconfiguration-action-v1.md)

## Context

An action envelope is not safe retry authority until the exact bytes cross a durable boundary before
dispatch. A restart must distinguish an exact retry from conflicting reuse of the same stable action
identity without trusting process memory or overwriting prior evidence.

## Decision

`TabletReconfigurationActionLedger` owns one existing directory for one exact tablet under a
nonblocking exclusive advisory `LOCK`. Every prepared action is an immutable file named
`action-<20-digit-movement-epoch>-<3-digit-kind>.ract`. Its embedded tablet, epoch, and kind must
match the configured owner and filename.

`prepare` canonical-encodes the supported action, exact-loads an existing final for byte-identical
idempotent retry, and rejects same-ID/different-byte reuse as corruption. For a new identity it
removes and directory-synchronizes only the canonical prior temporary, creates without replacement,
writes all bytes, exact-reads and decodes them, file-syncs, closes, no-replace renames, and
directory-syncs. Only the final directory sync authorizes dispatch. A failure after rename but
before directory sync poisons the live owner because crash durability is uncertain.

Reopen removes only canonical regular temporaries and synchronizes cleanup. Final files are never
overwritten or automatically reclaimed. Reconciliation against authoritative Raft membership and
metadata placement determines whether a prepared identity still needs dispatch; the ledger does not
use a mutable latest pointer or mistake preparation for execution.

## Rationale and alternatives

The deterministic action identity already supplies the retry coordinate, so another generation
counter would add an unnecessary atomicity problem. Retaining completed actions makes conflicting
reuse detectable across restart and preserves diagnostic evidence.

Writing after dispatch was rejected because a crash could lose retry identity. Rename-over-existing
was rejected because it permits conflict erasure. A mutable completion bit was rejected because the
authoritative state machines already prove completion and a second truth could diverge.

## Consequences and validation

Callers can durably prepare before local or remote dispatch, then repeat the same prepare after
restart without duplicating identity allocation. Storage grows by at most the bounded action
envelope per retained step until a later reclamation policy proves old evidence unnecessary.

Invariants 1, 4, 8, 9, 10, 11, 14, and 18 apply. Real-filesystem tests cover exclusive ownership,
prepare/retry, same-ID conflict, close/reopen/load, canonical naming, interrupted temporary cleanup,
and installed corruption. Syscall fault injection, process-kill cut points, power-loss qualification,
reclamation, authenticated leader routing, and duplicate-delivery simulations remain deferred.

## References

- [Tablet Reconfiguration Action v1 format](../formats/tablet-reconfiguration-action-v1.md)
- [ADR 0119](0119-deterministic-tablet-reconfiguration-action-identities.md)
- [POSIX I/O learning guide](../learning/posix-io.md)
