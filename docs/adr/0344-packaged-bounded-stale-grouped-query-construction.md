# ADR 0344: Packaged bounded-stale grouped-query construction

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB service, query, cluster, and replicated-runtime maintainers
- **Extends:** [ADR 0304](0304-packaged-bounded-stale-query-construction.md),
  [ADR 0343](0343-packaged-leader-linearizable-grouped-query-construction.md)

## Context

The packaged grouped constructor accepted only leader-linearizable barriers. Reusing that entry
point for follower reads would misstate authority, while manually entering the correlated follower
binder would again expose the catalog/Manifest/schema/route join to the embedding.

## Decision

`create_replicated_follower_distributed_grouped_float64_query` is the distinct synchronous
construction boundary for a bounded-stale grouped plan. It requires a numeric staleness bound and a
canonical group-sorted span of already-correlated leader/follower observations. It separately
acquires and verifies the metadata-group barrier, then enters the existing follower group-backed
aggregate binder.

The follower binder remains responsible for same-group, same-term, stable-membership correlation,
leader commit derivation, selected follower application/lag, committed placement, schema, and
Manifest coverage. Only its move-only compatible aggregate result enters the same exact active-
schema FLOAT64 specialization, route resolution, grouped execution, and TCP scheduler used by the
leader path. The resulting immutable dispatch targets the proved follower.

Observation acquisition remains caller-owned. The constructor neither fabricates observations nor
silently downgrades policy. No durable or network format changes.

## Consequences and validation

Leader and follower grouped policies now have distinct entry points and share the same post-binding
specialization and lifecycle helper. Work and memory retain the bounds of the existing follower
binder, grouped specialization, route resolver, and scheduler. Borrowed metadata, authentication,
projection, and TLS policy must outlive the returned owner.

The focused replicated service test supplies a stable same-term leader/follower pair, metadata-only
applied barrier, two-replica committed placement, matching follower Manifest position, and
follower-specific TLS route. Construction returns a running grouped scheduler whose dispatch
targets node 12 and retains key input one. Header self-containment and the installed-consumer gate
cover the public constructor.

Authenticated remote observation acquisition composition for grouped queries, explicit grouped
authority rebinding, general vector fragments, multi-key/non-FLOAT64 grouping, and broad
fault/measurement evidence remain incomplete. No Phase 16 exit gate is claimed.

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## References

- [Packaged bounded-stale query construction](0304-packaged-bounded-stale-query-construction.md)
- [Packaged leader-linearizable grouped-query construction](0343-packaged-leader-linearizable-grouped-query-construction.md)
- [Correlated follower read proof binding](0303-correlated-follower-read-proof-binding.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
