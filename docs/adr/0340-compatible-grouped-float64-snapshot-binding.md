# ADR 0340: Compatible grouped FLOAT64 snapshot binding

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and distributed-systems maintainers
- **Extends:** [ADR 0300](0300-group-keyed-distributed-query-proof-binding.md),
  [ADR 0326](0326-authority-bound-grouped-float64-fragment.md),
  [ADR 0329](0329-packaged-authority-bound-grouped-dispatch.md)

## Context

The single-fragment grouped binder proved group-key type and authority, while the aggregate batch
binder retained one compatible Manifest epoch for every plan-ordered tablet. A multi-tablet grouped
execution could still accept a caller-assembled grouped dispatch vector, losing proof that every
nested fragment came from that same epoch and every key index was validated under its exact schema.

## Decision

`bind_compatible_distributed_grouped_float64_snapshot` first delegates the complete plan/order,
admission, placement, Raft-group, proof, projection, schema, and Manifest validation to
`bind_compatible_distributed_aggregate_snapshot`. It accepts one shared projected group-key input
index and then, in exact plan order, resolves that projected ordinal under the same destination
schema reference used for each aggregate binding.

Every key must be in bounds and have logical type FLOAT64. Only then does the binder construct one
`DistributedGroupedFloat64FragmentDispatch` from the exact bound group and nested aggregate
fragment. No caller joins those values. Any count, authority, key-bound, type, or allocation failure
returns no grouped owner.

`CompatibleDistributedGroupedFloat64Snapshot` is move-only. It owns the complete compatible
aggregate snapshot, including its acquire-pinned Manifest generation, plus the plan-ordered grouped
dispatch vector. It exposes the pinned storage snapshot and a borrowed immutable dispatch span. No
durable or network format changes.

## Consequences and validation

All grouped tablets now retain one database/generation epoch and the same structural authority as
the proven aggregate batch. Binding work and owned dispatch storage are linear in fragments plus
projected ordinals and preserve the aggregate binder's hard limits. The owner is immutable after
construction, so no synchronization or memory-ordering argument is required.

The existing focused two-tablet compatible-snapshot case now proves both grouped dispatches retain
the exact groups, plan order, tablets, group-key index, database generation, and live epoch pin. It
also rejects an out-of-bounds key and a TIMESTAMP key before publication. The installed-consumer
gate references the public binder.

Portable coordinator execution, TCP scheduling, packaged grouped query construction,
multi-key/non-FLOAT64 grouping, and broad fault/measurement evidence remain incomplete. No Phase 16
exit gate is claimed.

Invariants 5, 6, 10, 11, 14, and 18 apply.

## References

- [Group-keyed distributed query proof binding](0300-group-keyed-distributed-query-proof-binding.md)
- [Authority-bound grouped FLOAT64 fragment](0326-authority-bound-grouped-float64-fragment.md)
- [Packaged authority-bound grouped dispatch](0329-packaged-authority-bound-grouped-dispatch.md)
- [Grouped FLOAT64 Fragment Dispatch v1](../formats/distributed-grouped-float64-fragment-dispatch-v1.md)
