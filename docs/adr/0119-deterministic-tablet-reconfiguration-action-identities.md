# ADR 0119: Deterministic tablet reconfiguration action identities

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB distributed-systems and metadata maintainers
- **Extends:** [ADR 0116](0116-raft-metadata-tablet-reconfiguration.md) and
  [ADR 0118](0118-durable-tablet-movement-checkpoint-generations.md)
- **Extended by:** [ADR 0120](0120-tablet-reconfiguration-action-v1.md)

## Context

Reconciliation can be interrupted after an action leaves the process but before its commit or
application becomes observable. Restarting from the durable movement checkpoint then emits the same
logical work, but the action previously had no stable identity with which a leader router or retry
ledger could recognize that retry.

## Decision

Every `TabletReconfigurationAction` carries an exact `Id` composed of tablet ID, current movement
placement epoch, and action kind. The kind distinguishes begin-joint, finalize-joint, and
publish-placement steps. The epoch distinguishes promotion from removal and subsequent movements;
metadata placement epochs are strictly increasing for a tablet.

The ID is deterministic, not randomly allocated. Reconstructing `TabletMovement` from a durable
checkpoint and reconciling the same authoritative Raft/metadata observations produces the same ID.
When an authoritative transition advances the movement epoch or the next step becomes eligible, the
ID changes. The emitted request remains exactly the existing versioned Raft membership or metadata
command.

This ID identifies intent; it does not alone prove execution, commit, or application. A production
leader-routing owner must key its retry/in-flight ledger by the ID and exact request bytes, reject
same-ID/different-request conflicts, and retain completion evidence until the corresponding
authoritative state is observed.

## Rationale and alternatives

Deriving identity from the durable state avoids a second counter whose installation would need to
be atomic with the movement checkpoint. Tablet plus epoch plus kind is collision-free under the
metadata epoch invariant and directly auditable in diagnostics.

Random UUIDs were rejected because restart would need another persisted allocation boundary.
Hash-only identities were rejected because they add collision reasoning without reducing the exact
fixed identity. Treating request bytes alone as identity was rejected because membership and
metadata groups have different execution domains and diagnostics need the tablet/step coordinate.

## Consequences and validation

The same pending action is recognizable across process restart and leader rerouting. ADR 0120 adds
a canonical envelope without changing the nested Raft commands. Focused tests assert all six
promotion/removal IDs and reconstruct the same pending ID from recovered movement bytes.

Invariants 4, 8, 9, 10, 14, and 18 apply. Production retry-ledger persistence, duplicate delivery
before metadata application, leader changes, and same-ID request conflict tests remain deferred to
the routing owner.

## References

- [ADR 0116](0116-raft-metadata-tablet-reconfiguration.md)
- [ADR 0118](0118-durable-tablet-movement-checkpoint-generations.md)
- [Tablet reconfiguration learning guide](../learning/tablet-reconfiguration.md)
