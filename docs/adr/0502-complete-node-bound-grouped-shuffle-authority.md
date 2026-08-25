# ADR 0502: Complete node-bound grouped shuffle authority

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB cluster and distributed-query maintainers
- **Extends:** [ADR 0501](0501-canonical-bounded-grouped-partition-splitting.md) and
  [ADR 0469](0469-split-leader-native-read-authority-coordination.md)

## Context

Canonical source partitioning can identify a numeric destination but cannot authorize where bytes
may go or which sources a reducer must close. Deriving destinations independently at each source
would permit catalog drift, while authorizing only destination nodes would leave reducers unable to
distinguish a missing source from a source that never belonged to the query. Hash version, key/state
shape, source merge order, and partition count must be one immutable query decision before a
network carrier is introduced.

## Decision

`DistributedVectorGroupedAggregateShuffleAuthority` owns one nonzero query ID, the complete
plan-ordered source tablet/node vector, a canonical contiguous partition/node vector, the exact
group key and aggregate definitions, and hash version one. Source tablet IDs are unique and every
node ID is nonzero. Partition IDs must equal their vector position, beginning at zero with no gap;
the vector length is the exact partition count. Multiple partitions may intentionally share a node.

An immutable tablet index resolves each authorized source node. `validate_edge` accepts only the
exact source tablet/node, partition/destination node, and hash version tuple. Unknown sources and
partitions are distinct `NOT_FOUND` failures; drift within a known edge is invalid. A source and
destination may name the same node because local shuffle is valid, but that edge must use an
in-process path rather than fabricate a self-network route.

Source vector order is frozen as the later reducer's deterministic sufficient-state merge order.
Partition destination order is routing authority, not SQL result order. Source count, partition
count, grouped key/state width, and a conservative retained-configuration estimate are bounded;
caller limits cannot exceed the hard 65,536-source, 4,096-partition, or 64-MiB configuration
ceilings. Construction publishes no partial owner and classifies allocation failure as resource
exhaustion.

This decision does not derive the authority from a catalog, resolve endpoints or TLS contexts,
encode a partition frame, authenticate a peer, retry a stream, or reduce a partition. Those owners
must consume this exact authority instead of recreating any part of it.

## Detailed rationale

One whole-query authority makes absence provable: a reducer can eventually require a terminal from
every ordered source for its partition. Binding numeric nodes before address resolution preserves
the existing separation between committed placement and replaceable bounded address candidates.
Keeping local edges valid avoids changing query semantics merely because a source and reducer are
co-located.

## Alternatives considered

- **Derive `hash % node_count` independently at workers.** Rejected because node ordering and
  membership can drift and because a node is not a stable partition identity.
- **Carry only partition count and let receivers accept any source.** Rejected because terminal
  closure and peer authorization would be incomplete.
- **Bind resolved socket addresses in query authority.** Rejected because committed node identity
  is authoritative while DNS/address candidates are bounded transport policy.
- **Reject local edges.** Rejected because co-location is valid; only the self-network carrier is
  invalid.

## Consequences

Every future shuffle frame can be checked against one exact source/destination edge, and reducer
closure has a complete ordered source set. The owner retains a bounded ordered vector plus a source
lookup map. It does not yet prove that the supplied mapping came from one catalog/read-authority
observation; the eventual constructor must establish that composition.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): future source execution still requires committed and
  applied Raft authority; this owner records the selected node without replacing that proof.
- [Invariant 6](../architecture/invariants.md): query, source tablets, destinations, hash version,
  and grouped shape are frozen together rather than reacquired per edge.
- [Invariant 11](../architecture/invariants.md): the complete vectors and lookup index have one
  move-only owner and no borrowed catalog lifetime.
- [Invariant 15](../architecture/invariants.md): source, partition, grouped width, and retained
  configuration influence are finite.
- [Invariant 18](../architecture/invariants.md): routing authority does not change key hashing,
  reducer equality, or deterministic source merge order.

## Validation plan

Focused tests preserve source order and repeated destination nodes, resolve exact source and
partition owners, and validate a complete edge. Negative cases cover nil identity, zero nodes,
duplicate tablets, noncanonical partitions, lower count/configuration limits, unknown sources and
partitions, node drift, and hash-version drift. Allocation injection covers every retained source
index allocation. The warning-as-error cluster build, 267 cluster tests, and 40 cluster
allocation-failure tests pass; focused authority cases pass under ASan/UBSan. Formatting,
static-analysis, and diff-review evidence is recorded with the implementing change.

## Migration or rollback considerations

This is additive in-memory pre-alpha authority with no durable or network bytes. Rollback removes
the owner while retaining canonical source partitioning and the existing coordinator-routed grouped
path.

## Unresolved questions

- Derive source and destination nodes from one correlated committed catalog/read-authority product.
- Freeze the distinct checksummed partition carrier and authenticated peer checks.
- Define whole-stream retry acknowledgment, reducer closure, and destination loss behavior.

## References

- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Distributed Vector Grouped Aggregate Exchange v1](../formats/distributed-vector-grouped-aggregate-exchange-v1.md)
- [Implementation roadmap](../roadmap.md)
