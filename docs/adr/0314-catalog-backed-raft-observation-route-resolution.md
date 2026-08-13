# ADR 0314: Catalog-backed Raft observation route resolution

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB cluster, metadata, and networking maintainers
- **Extends:** [ADR 0312](0312-finite-multi-address-raft-observation-acquisition.md),
  [ADR 0305](0305-bounded-dns-multi-address-query-routing.md)

## Context

Finite observation acquisitions accepted already-resolved routes, leaving embeddings to join target
node IDs with committed endpoint metadata, TLS contexts, DNS answers, and address bounds. Ad hoc
joins could use an endpoint for another node, accept duplicate targets, run DNS inside a poll loop,
or treat reachability as authority to change the observation target.

## Decision

`resolve_raft_observation_tcp_routes` accepts one committed metadata catalog, a canonical unique
node-ID selection, a canonical TLS-context set, and explicit route/address limits. It validates
catalog node ordering and identity, then resolves each selected node to its exact committed generic
endpoint and node-specific TLS context.

Numeric canonical IPv4 endpoints bypass DNS. Strict lowercase DNS endpoints use the existing
bounded IPv4/TCP resolver, retain system answer order, remove duplicates, preserve the committed
port, and reject answer sets beyond the caller limit. Unsupported endpoint grammar is
`UNAVAILABLE`, malformed selection/limits are `INVALID_ARGUMENT`, noncanonical committed node
metadata is `CORRUPTION`, and route, allocation, or answer bounds are explicit resource exhaustion.

Resolution is a blocking pre-acquisition operation. The returned vector and every candidate vector
are owning, canonical, finite snapshots. A route carries the selected node ID and its exact TLS
context; DNS answers cannot change request target, correlation, certificate identity, or principal
authorization.

## Consequences and validation

Resolution retains `O(selected nodes + bounded answers)` memory and performs at most one system
lookup per selected DNS node. An embedding invokes it before entering or re-entering the
single-threaded observation poll owners. Fresh construction reruns resolution; ChronosDB adds no
hidden cache or TTL policy.

A focused test resolves one committed numeric node and one committed `localhost` node, proves route
and port order plus exact TLS-context binding, and rejects duplicate target selection and missing
TLS authority while enforcing the selected-route limit. Existing network tests cover strict
hostname grammar, answer uniqueness, and configured bounds.

Follower selection, catalog-wide multi-pair construction, asynchronous resolver integration, DNS
cache/TTL policy, resolver latency qualification, IPv6, and live DNS-failure matrices remain
incomplete.

Invariants 5, 6, 10, 14, 15, and 18 apply.

## References

- [Finite multi-address Raft observation acquisition](0312-finite-multi-address-raft-observation-acquisition.md)
- [Bounded DNS and multi-address distributed query routing](0305-bounded-dns-multi-address-query-routing.md)
- [Committed distributed query route resolution](0298-committed-distributed-query-route-resolution.md)
