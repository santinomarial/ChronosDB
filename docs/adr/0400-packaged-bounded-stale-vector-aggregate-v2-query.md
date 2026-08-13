# ADR 0400: Packaged bounded-stale vector aggregate v2 query

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB service, query, cluster, and replicated-runtime maintainers
- **Extends:** [ADR 0304](0304-packaged-bounded-stale-query-construction.md),
  [ADR 0399](0399-packaged-leader-linearizable-vector-aggregate-v2-query.md)

## Context

The packaged aggregate-v2 service boundary accepted only leader-linearizable authority. The v2
follower binder already proved a canonical same-group, same-term leader/follower pair against
committed placement, follower application, schema, and Manifest state. Entering it manually would
again expose the schema/definition/route/execution correlation to an embedding.

## Decision

`create_replicated_follower_distributed_vector_aggregate_query_v2` is the distinct synchronous
construction boundary for an ungrouped follower-bounded-stale vector aggregate. It accepts one
canonical group-sorted span of already-correlated follower authorities and requires an explicit
numeric staleness bound.

The constructor verifies the same metadata-group barrier and committed catalog coverage as the
leader path, then calls `bind_follower_group_backed_distributed_vector_snapshot_v2`. That binder
retains responsibility for stable membership, same-term leader/follower correlation, commit
frontier derivation, follower lag/application, committed placement, exact projection, result
schema, Manifest coverage, and cross-tablet aggregate definitions. Only the resulting compatible
owner enters the shared committed route resolver, portable aggregate execution, TCP scheduler, and
Native result finalizer. The immutable dispatch continues to target the proved follower.

Remote observation acquisition remains caller-owned. The constructor neither fabricates authority
nor silently downgrades leader policy. Catalog, barrier, plan, projection, and authority views need
only outlive the synchronous call. Authentication, authorization, and node TLS policy must outlive
the returned owner.

## Alternatives considered

- **Reuse the leader constructor:** rejected because leader barriers cannot prove follower lag or
  application.
- **Convert follower observations into leader barriers:** rejected because it destroys the exact
  correlated proof and misstates the serving node.
- **Bundle remote acquisition now:** rejected because its multi-phase cancellation, metrics, and
  retained result schema need a separate move-only owner.

## Consequences

Leader and follower aggregate-v2 paths now have distinct policy entry points and share the exact
post-binding route/execution/finalization gate. Work and memory retain all existing finite limits.
Construction may block on the metadata barrier and bounded DNS only; it opens no socket until the
returned owner is polled. One thread serializes construction and execution. No durable or network
format changes.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): follower results cannot exceed the proved applied
  and leader-commit boundary.
- [Invariant 6](../architecture/invariants.md): the correlated proof, Manifest snapshot, schema,
  definitions, and route describe one compatible follower read.
- [Invariant 11](../architecture/invariants.md): the compatible owner pins referenced Manifest and
  query resources through execution.
- [Invariant 14](../architecture/invariants.md): no existing durable or network format changes.
- [Invariant 18](../architecture/invariants.md): sharing only the post-binding lifecycle preserves
  follower-specific authority guarantees.

## Validation plan

Supply a same-term stable leader/follower pair, metadata-only barrier, two-replica placement,
follower-specific TLS route, and exact follower Manifest position. Require a running aggregate-v2
owner whose immutable dispatch targets the follower and retains the exact AVG definition. Retain
policy rejection, missing metadata barrier, header self-containment, installed-consumer,
formatting, static analysis, ASan/UBSan, and full serialized-suite coverage.

## Migration or rollback considerations

Bounded-stale aggregate-v2 embeddings with an existing authenticated observation owner should use
this constructor and retain authentication/TLS policy for the returned scheduler lifetime.
Rollback restores manual post-acquisition correlation without changing persisted or wire state.

## References

- [Packaged bounded-stale query construction](0304-packaged-bounded-stale-query-construction.md)
- [Packaged leader-linearizable vector aggregate v2 query](0399-packaged-leader-linearizable-vector-aggregate-v2-query.md)
- [Metadata-backed schema-bound vector v2 snapshots](0397-metadata-backed-schema-bound-vector-v2-snapshots.md)
- [Distributed Vector Aggregate Query Transport v2](../formats/distributed-vector-aggregate-query-transport-v2.md)
