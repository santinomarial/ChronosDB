# ADR 0106: Topology-bound subscription retention authority

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB live-query, metadata, WAL, and Raft maintainers
- **Extends:** [ADR 0075](0075-durable-metadata-raft-commands.md) and
  [ADR 0101](0101-durable-multi-tablet-subscription-owner.md)

## Context

A durable subscription checkpoint exposes logical per-tablet expiry positions, but ADR 0101
explicitly forbids treating them alone as permission to delete an upstream log. Storage/Raft
snapshots may lag, several durable plans may promise different retained suffixes, tablet placement
may move, and a logical tablet sequence does not identify a physical WAL segment and byte offset.

## Accepted decision

`SubscriptionRetentionCoordinator` is the thread-affine deletion authority for one fixed database,
table, tablet/WAL set, local replica, and committed placement-epoch vector. Configuration registers
the complete durable subscription-owner set that may promise resume for those sources. Every
advance exact-validates owner identities and canonical lineages, requires a committed metadata
placement for each tablet at the configured epoch, and proves the local node remains a replica.

The caller supplies the storage/Raft-safe logical vector. The coordinator takes the component-wise
minimum with every owner's last durably installed subscription expiry. If any subscription owner has
no checkpoint, deletion is blocked. Candidate positions cannot regress behind a prior successful
authorization. The complete candidate vector, placement epochs, and committed metadata index are
passed once to a `SubscriptionSourceReclaimer`; the coordinator advances its remembered authority
only after that source owner reports success.

The reclaimer is source-specific by necessity. A WAL implementation must resolve the logical
position to a verified `WalReplayCheckpoint` including segment and byte offset; a Raft
implementation must apply its snapshot/log-prefix rules. The interface requires complete-batch
prevalidation and idempotent cleanup. It does not pretend that a logical sequence can be cast into a
physical coordinate, and successful authorization need not split an active physical segment.

## Consequences and alternatives

Placement movement or epoch advancement fails closed before any deletion call and requires a newly
bound authority. Multiple plan coordinators constrain one source by their minimum durable frontier.
Removing a plan owner or changing a source set is intentionally a higher-level lifecycle operation;
it cannot be done silently through this fixed authority.

**Retrospective update (ADR 0412):** the authority now exposes bounded explicit plan-owner
registration and retirement while retaining its fixed source topology and monotonic authorized
frontier. Service activation must follow registration, and service drain must precede retirement.

Calling `WalWriter::reclaim_checkpointed_segments` with a fabricated byte offset was rejected.
Using only the fastest subscription frontier was rejected because a slower plan could still resume.
Ignoring metadata placement was rejected because a removed replica could delete state during
handoff. Advancing local authority before the physical owner succeeds was rejected because retry and
restart would overstate completed cleanup.

## Affected invariants and validation

Invariants 8, 11, 12, 15, and 17 apply. Focused tests use a real durable subscription generation,
intersect a storage-safe vector with evicted subscription suffixes, verify the exact batch and
metadata index delivered to the reclaimer, then advance a committed placement epoch and prove no
second deletion call occurs. Multi-plan aggregation, physical WAL coordinate resolution, Raft
prefix cleanup, owner retirement, crash cuts, movement races, and fault injection remain with their
source-specific Phase 15/18 validation.
