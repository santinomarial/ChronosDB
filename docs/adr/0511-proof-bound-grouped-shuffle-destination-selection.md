# ADR 0511: Proof-bound grouped shuffle destination selection

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB catalog, cluster, and distributed-query maintainers
- **Extends:** [ADR 0488](0488-coherent-replicated-grouped-sql-preparation.md),
  [ADR 0502](0502-complete-node-bound-grouped-shuffle-authority.md)

## Context

The complete shuffle authority accepted caller-assembled source and destination vectors. Packaged
mutable grouped execution already owns one plan-ordered vector of proof-bound fragments whose
serving nodes were selected from the same pinned committed placement and correlated leader
authority. Reconstructing source placement or choosing destinations independently at every worker
would permit topology drift and leave no canonical partition policy.

## Decision

Add a shuffle-authority constructor over the complete proof-bound mutable fragment vector and the
exact grouped key/state definitions. Fragment order becomes source order without sorting. Every
fragment must share one nonnil query identity and carry a nonnil tablet plus nonzero serving node;
the existing authority constructor rejects duplicate tablets and validates grouped shape and all
hard/configured bounds.

The destination set is the sorted unique set of those exact serving nodes. Each distinct node owns
one partition, with contiguous partition IDs assigned in ascending node order. Repeated source
placement therefore does not create repeated partitions. Selection is deterministic for the same
proof-bound fragment product, does not depend on route/DNS order, and guarantees that every
destination already participates in the query's committed placement. The constructor copies all
sources, destinations, keys, and aggregate definitions and borrows no fragment or catalog storage.

This baseline is placement-aware, not load-aware. It does not infer CPU, memory, network locality,
or current utilization, and it does not add extra nodes that hold no selected tablet. Skew remains
a bounded query failure under the existing partitioner policy. A future measured policy requires a
new explicit authority decision rather than changing this mapping silently.

## Detailed rationale

Choosing one partition per distinct serving node maximizes a simple invariant: every selected
destination is already an authenticated, addressable participant in the exact query authority.
Sorting numeric node identity makes the mapping independent of source order while source order
continues to define deterministic reducer merge. Keeping numeric node identity separate from route
resolution preserves address rotation and TLS context policy.

## Alternatives considered

- **Let each source hash over its local route list.** Rejected because route order and visibility
  can differ and addresses are not placement authority.
- **Use one partition per source tablet.** Rejected because colocated tablets would create needless
  partitions and empty-edge amplification.
- **Always reduce on the Native coordinator.** Rejected because it preserves the existing central
  merge bottleneck and is not destination-partitioned shuffle.
- **Select by live load.** Deferred until a versioned observation and measurement-backed rebinding
  contract exists.

## Consequences

Packaged mutable grouped execution can now derive one complete source/destination/hash authority
from its existing proof-bound fragment product without caller-chosen destinations. The mapping is
finite, deterministic, and allocation-atomic. It still needs an owner that partitions every source
stream, dispatches local and remote edges, closes reducers, and gathers partition results.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): source and destination nodes come from the exact
  proof-bound serving-node observations rather than an independent topology lookup.
- [Invariant 6](../architecture/invariants.md): query identity, plan-order tablets, serving nodes,
  grouped shape, partition count, and destination order become one immutable authority.
- [Invariant 11](../architecture/invariants.md): the result owns all copied authority fields and
  borrows no fragment or catalog lifetime.
- [Invariant 15](../architecture/invariants.md): source, unique destination, grouped width, and
  retained configuration bounds are checked before publication.
- [Invariant 18](../architecture/invariants.md): destination selection does not alter canonical hash
  or exact reducer equality.

## Validation plan

Focused tests preserve reverse plan-order sources while sorting and deduplicating serving nodes,
and reject empty inputs, query drift, zero nodes, and a lower partition limit. Allocation injection
sweeps source copies, node sorting/deduplication, destination construction, grouped-definition
copies, and the existing authority index. Header self-containment, changed-file formatting,
the warning-as-error ASan/UBSan build, all 287 cluster tests, and all 49 cluster
allocation-failure tests pass. Changed C++ files pass LLVM 18 formatting. The repository-wide
format check reaches one unchanged pre-existing grouped-query TLS header violation. Changed-source
clang-tidy reaches only the known LLVM 18/macOS 26 libc++ builtin incompatibility without a
ChronosDB-source finding. Final whitespace and scope review pass.

## Migration or rollback considerations

No durable or wire bytes change. Rollback requires packaged callers to stop selecting the shuffle;
the lower-level explicit authority constructor remains for tests and non-packaged embeddings.

## Unresolved questions

- Own complete all-edge partitioning, local delivery, remote scheduling, and cancellation.
- Return disjoint partition results for global final projection, ordering, and limit.
- Measure skew and resource balance before considering multiple partitions per node.

## References

- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
