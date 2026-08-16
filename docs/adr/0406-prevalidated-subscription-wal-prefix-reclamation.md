# ADR 0406: Prevalidated subscription WAL prefix reclamation

- **Status:** accepted
- **Date:** 2026-08-15
- **Owners:** ChronosDB live-query and WAL maintainers
- **Extends:** [ADR 0106](0106-topology-bound-subscription-retention.md) and
  [ADR 0405](0405-verified-logical-to-physical-wal-prefix-resolution.md)

## Context

The topology-bound subscription authority emits one logical reclamation request per tablet. A
production WAL owner must bind those requests to the exact open writers, map every safe logical
frontier to a verified record-end coordinate, and avoid deleting from one WAL before discovering
that another source in the same authorization batch is invalid.

Several tablets may share one database WAL. Their record sequences use the same global history, but
each tablet's durable/subscription frontier independently limits deletion. Treating them as separate
physical logs or choosing their maximum could remove a record another tablet still needs.

## Accepted decision

`WalSubscriptionSourceReclaimer` is a thread-affine `SubscriptionSourceReclaimer` implementation.
Its fixed configuration borrows open `WalWriter` owners and binds each canonical tablet to one
placement epoch. Writers must outlive the reclaimer and all operations remain serialized by the
same caller.

Each reclamation call requires the complete configured tablet set, one nonzero committed metadata
index, exact tablet/WAL identity, and exact placement epochs. Requests that share a writer collapse
to their minimum record sequence. The reclaimer calls `resolve_replay_checkpoint` for every distinct
WAL before the first mutation. Only after all mappings succeed does it invoke
`reclaim_checkpointed_segments` for the resulting physical checkpoints. An already-absent logical
prefix is an idempotent no-op.

Deletion across independent WAL directories is not filesystem-atomic. If a later writer operation
fails after an earlier one succeeds, the batch reports failure and a retry converges because both
resolution and whole-segment cleanup are idempotent. The subscription authority advances its
in-memory authorization only after the complete call succeeds.

## Consequences and alternatives

The conservative minimum may retain unrelated records longer when one tablet sharing a WAL is idle.
Recovering that space requires a richer durable proof that every intervening global WAL record is
covered, not a maximum or fabricated coordinate. One WAL per tablet remains naturally independent.

Keeping only a callback from logical sequence to offset was rejected because it would separate the
mapping from the writer's locked namespace proof. Mutating each writer immediately after its own
mapping was rejected because a later invalid source could have been found before any deletion.

## Affected invariants and validation

Invariants 8, 10, 11, 12, 15, and 17 apply. Focused tests prove minimum-frontier aggregation for two
tablets sharing one real WAL, idempotent repeat, complete prevalidation across two WALs before the
first unlink, and direct integration from the topology authority through physical segment removal.
Raft-source mapping, dynamic plan-owner retirement, crash cuts across multiple WAL directories, and
the broader Phase 11 exit evidence remain incomplete.

## Retrospective note (2026-08-15)

[ADR 0410](0410-raft-subscription-snapshot-and-prefix-reclamation.md) completed the separate
Raft-source path with application-snapshot coverage and node-wide shared-log checkpointing. The WAL
batch semantics accepted here are unchanged.
