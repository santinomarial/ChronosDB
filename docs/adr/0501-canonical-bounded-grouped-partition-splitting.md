# ADR 0501: Canonical bounded grouped partition splitting

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB query and distributed-query maintainers
- **Extends:** [ADR 0470](0470-canonical-multi-key-grouped-sufficient-state-exchange.md),
  [ADR 0471](0471-shared-mergeable-grouped-state-owner.md), and
  [ADR 0500](0500-packaged-mutable-grouped-native-execution.md)

## Context

The sufficient-state grouped path sends every tablet-local group to one coordinator. A future
destination-partitioned shuffle needs one exact routing function shared with local grouping, a
finite skew policy, and explicit stream closure for every source-to-destination edge. Defining only
`std::hash` or omitting empty edges would make routing process-dependent or force reducers to infer
completion from timeouts. A second key-normalization implementation could also separate partition
identity from the grouped table's signed-zero, NaN, NULL, type, and payload equivalence.

## Decision

Expose the grouped table's canonical scalar-key hash as `canonical_vector_group_key_hash_v1` and
use it for partition selection. Hash v1 is FNV-1a over each key's fixed-width logical type code and
parameters, presence byte, and canonical value bytes. Variable values include their little-endian
64-bit byte length. FLOAT32 and FLOAT64 normalize every NaN to one quiet-NaN bit pattern and both
signed zeros to positive zero. NULL contributes no payload. Declared nullability is validated but
is not a hash input. Exact key equality at the reducer remains authoritative for collisions.

`DistributedVectorGroupedAggregatePartitioner` accepts exactly one complete canonical
tablet-local `CHDVGEX1` stream and a fixed nonzero partition count. It exact-decodes and validates
identity, contiguous ordinal/sequence, stable group count, and the distinct empty terminal before
routing `hash-v1(key) % partition_count`. Local first-seen order is preserved within each
destination. Every destination receives a complete re-ordinalized source stream; a destination
with no groups receives one canonical empty terminal. Thus downstream closure can require one
terminal from every planned source without timeout inference.

Partition count, input group count, groups per destination, decode limits, input bytes,
per-destination bytes, and total output bytes are all checked. Configurable byte bounds cannot
exceed the one-GiB hard ceiling. Skew above the per-destination group bound is resource exhaustion;
the implementation does not silently repartition or spill. Construction and partitioning are
single-thread-affine. The input remains caller-owned and the complete output is constructed
privately, so decode, allocation, skew, or byte failure exposes no partial partition vector.

This decision creates only an in-memory source-side split. It does not select destination nodes,
add a partition identifier to a network frame, transport streams, arbitrate retries, merge a
partition, or replace the existing all-tablet coordinator.

## Detailed rationale

Reusing the grouped table hash prevents a third semantic oracle beside physical-key grouping and
scalar sufficient-state merge. Explicit empty streams turn all-to-all completion into finite
message accounting. A fixed modulo rule is simple to test and sufficient for the first bounded
shuffle; skew is surfaced as an error until measured evidence justifies spill, adaptive splitting,
or another policy.

Hash v1 is named because changing its normalization or byte order can move groups. A future
distributed carrier must bind the hash version and partition count in authenticated query
authority before mixed-version deployment; this in-memory increment has no compatibility
negotiation to infer.

## Alternatives considered

- **Use `std::hash` or process-seeded hashing.** Rejected because results and destinations can vary
  by standard library, process, or restart and because SQL floating equivalence is not guaranteed.
- **Re-encode each key and hash the full exchange bytes.** Rejected because group position,
  sufficient state, and framing are unrelated to key identity and would route equal keys
  differently.
- **Emit only nonempty partitions.** Rejected because a reducer could not distinguish an empty edge
  from loss without additional inference.
- **Add network routing immediately.** Deferred until partition destination authority, retry
  identity, and reducer closure have their own bounded ownership contract.

## Consequences

Equal canonical keys from different sources now have a deterministic destination function, and
every source can generate an explicit terminal for every partition. Empty-edge amplification is
bounded but costs one 132-byte frame per empty destination. FNV-1a is not a load-balancing or
adversarial-skew guarantee; hard skew limits deliberately fail the query. The current splitter
decodes then re-encodes the complete source stream, so it is a correctness boundary rather than a
performance claim.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): the split retains exact query/tablet identity and
  key/state authority; future destination binding remains separate.
- [Invariant 10](../architecture/invariants.md): only exact-decoded integrity-checked group frames
  influence routing and output.
- [Invariant 11](../architecture/invariants.md): decoded query-accounted storage, caller-owned input,
  and privately constructed output have nonoverlapping lifetimes.
- [Invariant 14](../architecture/invariants.md): existing `CHDVGEX1` bytes are unchanged, and the
  in-memory hash contract has an explicit version before it enters a protocol.
- [Invariant 15](../architecture/invariants.md): partition, group, input, per-partition, total-output,
  and query-memory influence are finite.
- [Invariant 18](../architecture/invariants.md): partitioning reuses the exact grouped hash
  normalization and does not weaken collision equality or complete-stream validation.

## Validation plan

Focused tests route nullable multi-key STRING/Boolean groups across eight partitions, exact-decode
every emitted stream, verify `hash % partition_count`, require contiguous terminal positions, and
prove each input group appears once. They cover explicit empty destinations, signed-zero/NaN/NULL
hash equivalence, incomplete input, one-partition skew, total-byte failure, post-failure retry, and
allocation failure at every observed partitioning allocation. The warning-as-error build passes;
the full query suite passes 427 of 427 tests and the allocation-failure suite passes 63 of 63.
Sanitizer, formatting, static-analysis, and diff-review evidence is recorded with the implementing
change.

## Migration or rollback considerations

This is additive in-memory pre-alpha behavior with no durable or network migration. Rollback
removes the partitioner and public hash entry point while retaining the unchanged grouped exchange,
worker, coordinator, and packaged Native path.

## Unresolved questions

- Bind partition count, hash version, destination node, and source set to one authenticated query.
- Define complete per-partition retry/duplicate arbitration and reducer merge order.
- Qualify adversarial skew, source/destination loss, split leadership, and measured exchange costs
  before selecting this path in packaged SQL.

## References

- [Distributed Vector Grouped Aggregate Exchange v1](../formats/distributed-vector-grouped-aggregate-exchange-v1.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
