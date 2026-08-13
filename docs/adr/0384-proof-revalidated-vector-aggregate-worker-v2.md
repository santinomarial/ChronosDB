# ADR 0384: Proof-revalidated vector aggregate worker v2

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, Manifest, and distributed-query maintainers
- **Extends:** [ADR 0375](0375-proof-revalidated-schema-bound-vector-row-worker-v2.md),
  [ADR 0383](0383-owned-cross-tablet-vector-aggregate-definitions.md)

## Context

The all-type merge kernel, canonical nested state, correlated ungrouped exchange, and pinned
cross-tablet definition authority existed, but the real-CSEG Fragment-v2 worker still accepted rows
only. Executing a local aggregate through the row result path would finalize AVG, variance, and
exact sums too early. It would also apply the final result schema where the worker needs the exact
projected input shapes.

The older Float64 aggregate worker cannot be generalized invisibly: it has a different fragment,
fixed state, transport, and single-input contract. Vector-v2 needs an explicit all-type worker while
preserving all existing proof gates and leaving grouped semantics distinct.

## Decision

`execute_distributed_vector_aggregate_fragment_v2` accepts only Fragment-v2
`UNGROUPED_AGGREGATE` plans. It canonically re-encodes the in-memory dispatch, validates finite
query/scan/projection/state limits, and reuses the row worker's exact local node, placement,
leadership, read-barrier, Manifest generation, database/table/tablet, recovery schema, Raft source,
durable position, and part-range validation before part I/O.

Under the freshly proved local schema, the worker derives projected physical shapes and binds the
exact operation/input definition vector against the Fragment-v2 result schema. It loads validated
generation-pinned temporal CSEGs, resolves current winners/tombstones, applies the event-time
predicate, and materializes the full fragment projection in plan order. Every selected row then
accumulates into the shared all-type mergeable kernel. Query credit owns scan/projection chunks and
variable extrema; fixed state capacity is independently bounded.

The result owns the exact local definition vector, one correlated Aggregate Exchange v1 message per
definition, and the selected input-row count. Sequence is `ordinal + 1`, only the final definition
is terminal, and empty tablets emit the complete vector of sufficient empty states. Worker-local
ORDER BY, LIMIT, and finalization are never applied.

The worker is synchronous and thread-affine. The validated loader invokes its borrowed part
consumer exactly once; incomplete or repeated callback behavior fails without publishing a result.
The overload accepting an embedding-owned loader permits the existing exact tiered-storage seam
without weakening snapshot authority.

## Consequences and validation

One tablet can now compute every supported COUNT/SUM/AVG/MIN/MAX/variance partial over real temporal
CSEGs without losing sufficient state. Work is O(selected rows × aggregate count) after temporal
resolution. Memory is bounded by resolution limits, query credit, projection chunk limits,
aggregate width, fixed configuration bytes, and variable-extremum bytes. No cross-thread state is
shared, so no memory-ordering argument applies.

The real-CSEG test filters two visible rows to one and proves canonical COUNT, SUM, AVG, and MAX
messages, exact identities/positions, successful exchange encoding, and final state values. It also
rejects the row API crossover, incomplete loader callbacks, stale placement, and lower width limits
before publication. An allocation-failure matrix exercises an empty tablet through every owned
allocation, requiring resource exhaustion until one complete two-state vector succeeds. Header
self-containment and installed consumption cover the public API.

The authenticated service, v2 transport carrier, retry coordinator, cross-tablet merge/finalization,
grouped all-type exchange, and process integration remain separate tasks. No Phase 16 exit gate is
claimed.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Proof-revalidated schema-bound vector row worker v2](0375-proof-revalidated-schema-bound-vector-row-worker-v2.md)
- [Owned cross-tablet vector aggregate definitions](0383-owned-cross-tablet-vector-aggregate-definitions.md)
- [Distributed Vector Aggregate Exchange v1](../formats/distributed-vector-aggregate-exchange-v1.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Architecture invariants](../architecture/invariants.md)

