# ADR 0352: Canonical distributed vector plan intent

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, planner, and networking maintainers
- **Extends:** [ADR 0350](0350-canonical-distributed-vector-batch-exchange.md)

## Context

The vector-result envelope can move arbitrary current logical types, but aggregate-only and
nullable-FLOAT64 grouped requests cannot describe general row projection, multi-key grouping,
all current aggregate operations, final ordering, or LIMIT. Serializing `PhysicalPipelinePlan`
objects directly would freeze implementation variants, process-sized ordinals, resource-policy
defaults, and schema types into a network format.

## Decision

Distributed Vector Plan Intent v1 is a distinct schema-neutral checksummed value. It names projected
input indices for row output or group keys, current aggregate operation plus optional input, final
output ordering keys, and optional 64-bit LIMIT. Three explicit modes make row, ungrouped, and
grouped shapes canonical. Row projection may repeat; group and order keys are unique. Grouped output
is keys followed by aggregates, matching the local physical operator boundary.

The fixed header bounds every descriptor collection and has an early checksum before counts control
allocation. The complete checksum passes before owned vectors allocate. Decode limits can lower
input/output widths and each collection bound. No native struct representation, `size_t`, logical
type object, allocator policy, or physical operator variant is serialized.

ORDER and LIMIT are final-output semantics. They cannot be pushed independently to tablet workers
without a separately proved merge-preserving algorithm. The intent has no authority and cannot be
executed until a later binder proves its indices and operations against the exact schema and wraps
it in a snapshot/placement/group/proof-bound request.

## Consequences and validation

The format covers the missing general plan vocabulary without changing aggregate/grouped request
or result bytes. Its maximum retained encoded size is 67,644 bytes; encode/decode work is linear in
descriptor count. Two focused tests round-trip row, ungrouped, and multi-key grouped plans with
ordering and a present zero LIMIT, then reject truncation, unknown version, noncanonical aggregate
state, lower caller bounds, duplicate grouping, invalid COUNT(*), and invalid output ordering.
Header self-containment and installed consumption cover the public API.

Authority-bound vector fragments, schema/type binding, partial-state exchange semantics, global
coordination, authenticated transport, execution, and multi-process validation remain incomplete.
No Phase 16 exit gate is claimed.

Invariants 5, 6, 10, 14, 15, and 18 apply.

## References

- [Distributed Vector Plan Intent v1](../formats/distributed-vector-plan-intent-v1.md)
- [Distributed Vector Exchange v1](../formats/distributed-vector-exchange-v1.md)
- [Bounded physical pipeline plan](0023-bounded-physical-pipeline-plan.md)
- [Exact bounded SQL ORDER BY lowering](0046-exact-bounded-sql-order-by-lowering.md)
