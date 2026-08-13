# ADR 0304: Packaged bounded-stale query construction

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, query, cluster, and replicated-runtime maintainers
- **Extends:** [ADR 0302](0302-packaged-replicated-distributed-query-construction.md),
  [ADR 0303](0303-correlated-follower-read-proof-binding.md)

## Context

The correlated follower binder removed the naked leader-commit scalar, but an embedding still had
to join it manually with metadata-barrier coverage, route resolution, execution construction, and
the TCP lifecycle owner. The leader-linearizable packaged constructor could not accept follower
authority without misrepresenting its policy.

## Decision

`create_replicated_follower_distributed_aggregate_query` is the packaged construction boundary for
a follower-bounded-stale plan. It accepts the owning plan and Manifest snapshot, a canonical span of
already-correlated leader/follower group observations, and the same replicated-query configuration
used by the leader path.

The constructor acquires and verifies the metadata-group barrier, requires the catalog publication
to cover it, invokes the follower group-backed binder, resolves the selected follower routes from
that catalog, creates execution directly from the compatible snapshot, and returns the move-only
TCP poll/cancel/result owner. It rejects every policy other than bounded stale with an explicit
numeric lag bound.

Remote observation transport remains caller-owned. The supplied pair proves the group, term,
leader, follower, membership, and commit frontier; the binder and Manifest snapshot prove follower
application and durable visibility. Authentication, node authorization, and TLS policy are borrowed
for the returned owner's lifetime.

## Consequences

Leader and follower policies now have distinct packaged entry points and share the same catalog,
route, execution, deadline, retry, cancellation, and complete-result gates. No intermediate
admission or route correlation escapes to the embedding. Construction opens no socket until the
returned owner is polled and changes no durable or wire format.

Observation acquisition still needs a bounded authenticated protocol before bounded-stale queries
can be fully self-contained across independent nodes. Until then, callers must not fabricate or
infer remote observations.

## Validation

The service composition test supplies a stable same-term two-replica leader/follower pair, a
metadata-only applied barrier, matching committed placement, an exact follower Manifest position,
and a node-specific mTLS route. Construction returns a running lifecycle owner whose immutable
dispatch targets the follower. The full service and installed-consumer tests cover the public API.

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## Migration and rollback

Bounded-stale embeddings should replace manual binding and scheduler construction with this entry
point while retaining their observation acquisition owner. Rolling back restores manual
correlation without changing compatibility.

## References

- [Packaged replicated distributed query construction](0302-packaged-replicated-distributed-query-construction.md)
- [Correlated follower read proof binding](0303-correlated-follower-read-proof-binding.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
