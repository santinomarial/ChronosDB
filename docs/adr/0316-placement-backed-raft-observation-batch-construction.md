# ADR 0316: Placement-backed Raft observation batch construction

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB cluster, metadata, and query maintainers
- **Extends:** [ADR 0315](0315-canonical-multi-pair-raft-observation-acquisition.md),
  [ADR 0314](0314-catalog-backed-raft-observation-route-resolution.md)

## Context

The batch owner still accepted caller-assembled group pairs, routes, and correlation identities. An
embedding could omit a planned group, choose a node outside committed placement, reuse a
correlation, or construct different selections for tablets sharing one group.

## Decision

`construct_raft_observation_tcp_batch` accepts a follower-bounded-stale plan, one canonical
committed catalog, and bounded carrier policy. For every planned tablet it resolves exact placement
and immutable group binding, requires the plan leader to match the committed leader hint and voter
set, and selects one follower deterministically: the coordinator source node when it is a nonleader
replica, otherwise the lowest nonleader replica.

Selections are sorted and deduplicated by group; repeated tablets in one group must select the same
leader and follower. Every unique target is resolved once through committed endpoint metadata and
node-specific TLS contexts. Consecutive leader/follower correlation IDs are assigned from one
nonzero base with an exact overflow check. The returned group-sorted batch config retains the
configured pair, route, retry, TLS, and timeout bounds and opens no socket.

## Consequences and validation

Construction is `O(fragments log metadata + targets + DNS)` and retains bounded owning route copies
per selected group. The deterministic follower policy favors local serving without treating
reachability or a DNS answer as authority. If its selected follower is stale or changes term, the
later authenticated pair and query binders fail closed; this constructor does not fabricate
freshness or silently switch replicas mid-acquisition.

A focused test uses two tablets whose group order differs from tablet order. It proves canonical
group output, coordinator-preferred and lowest-follower selection, exact target routes, consecutive
correlations, and pre-I/O correlation-overflow rejection.

Composition that owns this batch through completion and then constructs the packaged bounded-stale
query, alternate-follower retry policy, process integration, and broader failure matrices remain
incomplete.

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## References

- [Canonical multi-pair Raft observation acquisition](0315-canonical-multi-pair-raft-observation-acquisition.md)
- [Catalog-backed Raft observation route resolution](0314-catalog-backed-raft-observation-route-resolution.md)
- [Packaged bounded-stale query construction](0304-packaged-bounded-stale-query-construction.md)
