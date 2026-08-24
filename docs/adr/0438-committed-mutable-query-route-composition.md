# ADR 0438: Committed mutable query route composition

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB service, cluster, metadata, networking, and query maintainers
- **Extends:** [ADR 0398](0398-committed-vector-v2-query-route-resolution.md),
  [ADR 0437](0437-correlated-replicated-mutable-fragment-binding.md)

## Context

Correlated replicated binding produced the exact owning mutable fragment set, and the mutable TCP
scheduler accepted the established `DistributedQueryNodeRoute` value. Route resolution still
accepted only durable aggregate and vector dispatches. Native composition would therefore have to
extract serving nodes from mutable fragments, independently reach back into committed metadata,
join TLS contexts, and keep that route set paired with the fragments.

## Decision

`resolve_distributed_query_node_routes` gains a type-safe overload for
`DistributedMutableVectorFragment`. All three overloads use one implementation and differ only in
the serving-node projection. Canonical committed node metadata, selected-node deduplication,
node-specific TLS context authority, strict IPv4/lowercase-DNS parsing, bounded fresh DNS answers,
ordering, allocation failure, and error classification remain identical.

`ReplicatedQuerySnapshot::bind_and_resolve_linearizable_mutable_vector_query` packages the
coordinator boundary. It first invokes the complete correlated mutable binder. Only after that
succeeds does it resolve exactly those fragments through the same immutable committed metadata
publication retained by the snapshot. The returned value owns the fragments and finite endpoint
vectors together; TLS contexts remain explicitly borrowed and must outlive TCP execution.

Route resolution is synchronous and may perform blocking system DNS lookup, so composition must
run before the nonblocking scheduler starts. DNS answers are reachability candidates under a
committed node identity, never authority to change a fragment target.

## Consequences

The output transfers directly into `DistributedMutableVectorQueryExecution::create` and
`DistributedMutableVectorQueryTcpExecutionConfig::routes` without reconstructing identities.
Fragment failure still requires full authority rebinding and fresh route resolution. The package
does not yet lower SQL, acquire barriers, own a Native request lifecycle, or start the scheduler.

Complexity and bounds match the established resolver:
`O(fragments log unique nodes + node/TLS lookups + bounded DNS answers)`. No new thread,
synchronization algorithm, durable format, or network frame is introduced.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): route selection cannot rewrite the fragment's
  proof-bound serving node.
- [Invariant 6](../architecture/invariants.md): fragments and routes derive from one retained
  committed metadata publication.
- [Invariant 11](../architecture/invariants.md): address vectors are owned; TLS borrowing is
  explicit.
- [Invariant 15](../architecture/invariants.md): routes, endpoint bytes, and DNS answers retain
  existing positive hard bounds.
- [Invariant 18](../architecture/invariants.md): no routed result is returned after fragment or
  route failure.

## Validation

The shared cluster route test resolves durable aggregate, durable vector, and mutable fragments to
identical ordered endpoints/TLS contexts and rejects a zero mutable serving node. The replicated
two-tablet recovery test binds two fragments and resolves their one deduplicated numeric node route
from the same committed snapshot. Header self-containment, installed external consumption,
sanitizers, and existing route allocation sweeps cover the shared public implementation.

## Migration and rollback

Both APIs are additive and not yet called by native request handling. Rollback removes the mutable
overload and packaged service method without changing durable or wire bytes.

## References

- [Bounded DNS and multi-address distributed query routing](0305-bounded-dns-multi-address-query-routing.md)
- [Bounded mutable vector query TCP scheduling](0435-bounded-mutable-vector-query-tcp-scheduling.md)
- [Correlated replicated mutable fragment binding](0437-correlated-replicated-mutable-fragment-binding.md)
