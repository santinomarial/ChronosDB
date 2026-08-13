# ADR 0301: Bound-snapshot distributed query execution

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB cluster and distributed-query maintainers
- **Extends:** [ADR 0171](0171-fail-closed-distributed-query-execution-owner.md),
  [ADR 0300](0300-group-keyed-distributed-query-proof-binding.md)

## Context

The compatible distributed snapshot owns plan-ordered dispatches containing the exact serving node,
applied position, observed leader-commit position, and optional linearizable barrier accepted by
binding. `DistributedQueryExecution::create` nevertheless required its caller to rebuild a second
plan-ordered admission vector. Although creation exact-compared the two representations, this was
an unnecessary allocation and correlation responsibility at the coordinator composition boundary.

## Decision

`DistributedQueryExecution::create_from_bound_snapshot` accepts only the source node, logical plan,
compatible snapshot, and limits. It reconstructs each admission directly from the immutable
dispatch in snapshot order and delegates to the existing exact-validating creation path. Callers
cannot substitute or reorder external admission authority through this entry point.

The lower-level `create` remains available for tests and embeddings that construct compatible
snapshots explicitly. Both paths retain the same validation, sender/coordinator ownership, retry
limits, and complete-result boundary.

## Consequences

Metadata- and group-backed coordinators now hand one owning bound authority object to execution.
The temporary admission vector is still required by the aggregate coordinator's current owned
model, but its contents have a single source of truth. Construction adds `O(fragments)` work and one
bounded allocation already present in the lower-level path. Allocation failure is explicit resource
exhaustion. No durable or wire format changes.

## Validation

The two-tablet execution test uses `create_from_bound_snapshot`, accepts both terminal partials
exactly once, and produces the complete aggregate. The delegated creation checks continue to cover
query, tablet order, policy, serving node, positions, barrier, generation, and sender limits.

Invariants 5, 6, 11, 14, 15, and 18 apply.

## Migration and rollback

Coordinator composition should prefer `create_from_bound_snapshot` after metadata-backed binding.
Rolling back requires reconstructing and retaining a parallel admission vector without changing
the transport or exchange protocol.

## References

- [Fail-closed distributed query execution owner](0171-fail-closed-distributed-query-execution-owner.md)
- [Group-keyed distributed query proof binding](0300-group-keyed-distributed-query-proof-binding.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
