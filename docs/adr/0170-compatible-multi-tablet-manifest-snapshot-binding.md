# ADR 0170: Compatible multi-tablet Manifest snapshot binding

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB query, manifest, and distributed-systems maintainers
- **Extends:** [ADR 0166](0166-authority-bound-distributed-fragment-construction.md)

## Context

Binding one worker fragment at a time allows a caller to accidentally use different acquire-loaded
Manifest generations across tablets. Every individual request could be valid while the distributed
query no longer names one coherent database publication epoch. A plain vector of dispatches also
does not retain the Manifest owner that supplied their database and generation identity.

## Decision

`bind_compatible_distributed_aggregate_snapshot` accepts one planned aggregate, one copyable owning
`TemporalDatabaseStorageSnapshot`, and one authority binding per planned fragment. It requires a
nonempty bounded plan, exact binding count and plan order, unique nonzero tablet identities, and a
bounded aggregate projection width. Every entry passes the existing admission, group, committed
placement, schema, durable-position, and projection binder against that same Manifest v2 snapshot.

The returned move-only `CompatibleDistributedAggregateSnapshot` owns both the acquire-loaded
Manifest epoch and the plan-ordered dispatch vector. Every dispatch must repeat the owner's exact
database identity and generation. Keeping this object alive pins the immutable loaded Manifest even
if the publisher advances to a later generation.

Compatibility here means one database publication generation plus each tablet's explicitly proved
per-group durable/applied position. It does not invent one comparable Raft index across groups or a
cross-tablet transaction timestamp. Different groups may legitimately have different admitted
positions, each governed by the declared read policy.

## Consequences and validation

Binding is `O(fragments log fragments + total projection ordinals)` because tablet uniqueness uses
an ordered set and each existing binder validates bounded placement/projection data. Retained memory
is the one shared Manifest owner plus the bounded dispatch vector. The default total projection
limit is 65,536 ordinals; callers may raise it only to the format-derived hard product of maximum
fragments and maximum schema columns.

Tests bind two different Raft groups and positions from one Manifest generation, verify exact plan
order/database/generation in both dispatches, and prove the loaded generation remains pinned after
the input snapshot is moved. Reordered bindings, duplicate tablet plans, and aggregate projection
limit exhaustion fail before a compatible owner is returned. Existing single-fragment mismatch
tests continue to cover placement, group, schema, and durable-boundary conflicts.

Remote generation refresh, leader-loss rebinding, general vector fragments, and cross-tablet write
transactions remain separate work.

Invariants 4, 5, 6, 14, 15, and 18 apply.

## Migration and rollback

This is an in-memory source API with no durable or wire change. Callers that need multi-tablet
execution should retain the compatible owner through sender/coordinator completion. Rolling back to
independent per-tablet binding loses the structural guarantee against mixed Manifest generations.

## References

- [Authority-bound distributed fragment construction](0166-authority-bound-distributed-fragment-construction.md)
- [Manifest v2](../formats/manifest-v2.md)
- [Distributed read admission](../learning/distributed-read-admission.md)
