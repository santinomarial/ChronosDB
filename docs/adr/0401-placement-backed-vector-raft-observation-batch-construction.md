# ADR 0401: Placement-backed vector Raft observation batch construction

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB cluster, metadata, query, and replicated-runtime maintainers
- **Extends:** [ADR 0316](0316-placement-backed-raft-observation-batch-construction.md),
  [ADR 0400](0400-packaged-bounded-stale-vector-aggregate-v2-query.md)

## Context

Placement-backed Raft observation batch construction accepted only
`DistributedAggregatePlan`. Schema-bound vector plans carry the same query identity, read policy,
and ordered tablet descriptors, but a remote vector follower owner could not acquire authority
without fabricating an unrelated legacy aggregate plan.

## Decision

`construct_raft_observation_tcp_batch` gains a type-safe overload for
`DistributedVectorQueryPlan`. Both public overloads delegate to one implementation over the shared
plan authority surface: nonnil query identity, follower-bounded-stale policy, canonical unique
tablets, and each tablet's committed leader target.

Placement/group lookup, deterministic follower selection, repeated-group consistency, unique
target route resolution, explicit node TLS contexts, finite retry and carrier limits, consecutive
correlation assignment, overflow handling, ordering, ownership, and failure classification are
identical. Vector result intent is not interpreted here; the later schema-bound binder remains the
authority for row/aggregate semantics and rejects an incompatible plan before execution.

## Alternatives considered

- **Convert the vector plan to a legacy aggregate plan:** rejected because it invents an aggregate
  operation unrelated to observation authority.
- **Accept naked tablet/leader pairs:** rejected because it would bypass query policy and canonical
  plan validation.
- **Duplicate the constructor:** rejected because placement and correlation guarantees must not
  drift by result shape.

## Consequences

Remote vector lifecycle owners can retain the original plan from authority acquisition through
schema-bound execution. Complexity and finite ownership are unchanged:
`O(fragments log metadata + targets + bounded DNS)`. Construction is synchronous and opens no
socket. One caller thread owns construction, so no synchronization or memory-ordering argument
applies. No durable or network bytes change.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): observations are requested only for the committed
  leader/follower targets selected for planned tablets.
- [Invariant 6](../architecture/invariants.md): one original vector plan identifies every group
  whose complete authority must publish together.
- [Invariant 11](../architecture/invariants.md): returned routes and pair configuration retain
  owned bounded storage with explicit borrowed TLS lifetimes.
- [Invariant 14](../architecture/invariants.md): the existing Raft Observation Transport v1 remains
  unchanged.
- [Invariant 18](../architecture/invariants.md): shared construction preserves placement, retry,
  TLS, and correlation guarantees.

## Validation plan

Construct legacy and vector follower plans over the same two tablets whose group order differs from
tablet order. Require identical canonical group order, leader/follower targets, route identities,
and correlation IDs. Retain invalid plan, placement, route, limit, correlation-overflow, header
self-containment, installed-consumer, formatter, static-analysis, sanitizer, and full-suite
coverage.

## Migration or rollback considerations

Vector follower lifecycle owners should pass their original plan directly to the new overload.
Rollback is wire- and durable-format compatible but requires disabling that process composition;
callers must not recreate legacy plans as an adapter.

## References

- [Placement-backed Raft observation batch construction](0316-placement-backed-raft-observation-batch-construction.md)
- [Packaged bounded-stale vector aggregate v2 query](0400-packaged-bounded-stale-vector-aggregate-v2-query.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
