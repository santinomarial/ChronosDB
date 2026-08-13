# ADR 0398: Committed vector v2 query route resolution

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB cluster, query, metadata, and networking maintainers
- **Extends:** [ADR 0298](0298-committed-distributed-query-route-resolution.md),
  [ADR 0305](0305-bounded-dns-multi-address-query-routing.md),
  [ADR 0397](0397-metadata-backed-schema-bound-vector-v2-snapshots.md)

## Context

Committed route resolution accepted only legacy aggregate fragment dispatches. The schema-bound
vector-v2 owner retains immutable `DistributedVectorFragmentDispatch` views, so packaged vector row
and aggregate execution either needed a parallel resolver or had to reconstruct route targets after
authority binding.

## Decision

`resolve_distributed_query_node_routes` gains a type-safe overload for a span of immutable vector
fragment dispatches. Both public overloads delegate to one implementation and differ only in how
they read the proof-bound serving node. Canonical committed catalog validation, explicit node-to-TLS
authority, finite DNS/address ownership, selected-node deduplication, ordering, limits, and failure
classification are identical.

The resolver borrows dispatch and TLS views only for the synchronous pre-execution call. Returned
routes own their finite address vectors and borrow only caller-owned TLS contexts under the existing
lifetime contract. Result schema, aggregate definitions, read proofs, and Manifest ownership remain
in the compatible vector-v2 owner and are neither copied nor interpreted by route resolution.

## Alternatives considered

- **Reconstruct legacy aggregate dispatches:** rejected because it would invent irrelevant fields
  and create a second representation of already-bound authority.
- **Add a vector-only resolver name and implementation:** rejected because route validation and
  failure semantics must not drift by query result shape.
- **Resolve from naked node IDs:** rejected because the bound dispatch set is the authority for the
  exact selected targets.

## Consequences

Packaged vector-v2 execution can join its exact bound target set to committed endpoints and explicit
TLS identities without weakening ownership. Complexity and allocation bounds are unchanged:
`O(fragments log unique targets + committed authority lookups + bounded DNS answers)`, with one
owned route per selected node. The synchronous resolver performs no internal concurrent work, so no
memory-ordering argument applies.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): route selection cannot change the proof-bound
  serving node.
- [Invariant 6](../architecture/invariants.md): routes are derived from the exact immutable bound
  dispatch set and one committed catalog snapshot.
- [Invariant 11](../architecture/invariants.md): returned address storage is owned, and borrowed TLS
  lifetime remains explicit.
- [Invariant 14](../architecture/invariants.md): no durable or network format changes.
- [Invariant 18](../architecture/invariants.md): shared implementation preserves existing route and
  authentication guarantees.

## Validation plan

Resolve a real two-tablet compatible vector-v2 owner through the same committed catalog and TLS map
as the legacy aggregate owner, then require identical node ordering, endpoints, and TLS identities.
Retain canonical metadata, missing TLS, unsupported endpoint, DNS, limit, allocation-failure,
header self-containment, installed-consumer, sanitizer, and full-suite coverage.

## Migration or rollback considerations

No current caller changes behavior. New packaged vector-v2 constructors should pass their owning
snapshot's dispatch view directly to the overload. Rollback is wire- and durable-format compatible
but restores the process-integration gap.

## References

- [Committed distributed query route resolution](0298-committed-distributed-query-route-resolution.md)
- [Bounded DNS multi-address query routing](0305-bounded-dns-multi-address-query-routing.md)
- [Metadata-backed schema-bound vector v2 snapshots](0397-metadata-backed-schema-bound-vector-v2-snapshots.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
