# ADR 0383: Owned cross-tablet vector aggregate definitions

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and distributed-query maintainers
- **Extends:** [ADR 0365](0365-schema-bound-distributed-vector-fragment.md),
  [ADR 0382](0382-schema-bound-ungrouped-vector-aggregate-exchange.md)

## Context

The compatible Fragment-v2 snapshot owner retained one schema proved against every tablet, but it
discarded the projected input shapes used during that proof. Result descriptors do not always
identify aggregate input state: COUNT always returns non-null INT64, and AVG/variance return
FLOAT64 for several input types. Reconstructing a definition later from output descriptors could
therefore merge states that were accumulated from different fragment-authorized input types.

The ungrouped aggregate exchange requires the complete exact definition vector on every codec and
partial-I/O boundary. Worker, transport, and coordinator owners need that authority to share the
same lifetime as the pinned snapshot rather than accept a separately reconstructed caller value.

## Decision

`CompatibleDistributedVectorSnapshotV2` owns one ordered aggregate-definition vector in addition
to its pinned v1 snapshot and result schema. During v2 binding, every ungrouped dispatch derives its
definitions from that tablet's exact projected destination shapes and the shared result schema.
The first vector becomes the owned value; every later tablet must derive an exactly equal vector,
including operation, input ordinal, logical type parameters, and nullability.

Row and grouped plans retain an empty vector. Grouped state transport remains a distinct future
contract and must not infer authority from this ungrouped field. The owner exposes only a borrowed
read-only span whose lifetime follows the move-only snapshot owner.

## Consequences and validation

Future workers, exchange codecs, and coordinators can use the same cross-tablet-proved definition
authority. Schema evolution that preserves final result descriptors but changes an aggregate input
definition fails before execution. The added storage is bounded by the existing 4,096-aggregate
plan limit and retained once per query, not once per tablet. Construction remains single-threaded;
no memory-ordering argument applies.

The compatible-snapshot test proves grouped plans expose no ungrouped authority and a two-tablet
AVG plan retains the exact projected FLOAT64 input ordinal, type, and nullability. Existing binder
tests continue to cover schema mismatch, plan order, Manifest pin lifetime, projection bounds, and
allocation classification through the enclosing exception boundary.

Worker accumulation, aggregate transport integration, retry coordination, merge/finalization, and
process integration remain separate tasks. No Phase 16 gate is claimed.

Invariants 6, 10, 11, 14, 15, and 18 apply.

## References

- [Schema-bound distributed vector fragment](0365-schema-bound-distributed-vector-fragment.md)
- [Schema-bound ungrouped vector aggregate exchange](0382-schema-bound-ungrouped-vector-aggregate-exchange.md)
- [Distributed Vector Fragment v2](../formats/distributed-vector-fragment-v2.md)
- [Distributed Vector Aggregate Exchange v1](../formats/distributed-vector-aggregate-exchange-v1.md)
- [Architecture invariants](../architecture/invariants.md)

