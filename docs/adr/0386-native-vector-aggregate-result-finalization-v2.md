# ADR 0386: Native vector aggregate result finalization v2

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB cluster, query, and native-protocol maintainers
- **Extends:** [ADR 0385](0385-bounded-vector-aggregate-coordinator-v2.md),
  [ADR 0222](0222-bounded-native-vector-query-results.md)

## Context

The vector-v2 coordinator could merge and globally finalize all-type sufficient state, but its
scalar result was not yet a client-visible Native Protocol v1 payload. Returning ad hoc scalar
storage would duplicate type encoding policy and could lose the input-type authority needed to
validate COUNT, AVG, variance, and exact SUM results.

Ungrouped aggregation produces exactly one semantic row before LIMIT. ORDER BY cannot change that
row, but LIMIT zero must still return the admitted schema rather than an absent stream.

## Decision

The coordinator result now carries its exact aggregate-definition vector through global scalar
finalization. `finalize_distributed_vector_aggregate_v2` consumes that owner with the original
ungrouped plan. It independently validates the plan, definition operations and input ordinals,
definition-derived output shapes, result descriptors, finalized scalar types/nullability, column
name limits, and all collection widths before output allocation.

Every supported logical scalar is converted to the canonical little-endian Native Protocol cell
representation: fixed signed and unsigned widths, IEEE floating bits, 128-bit decimal coefficient,
timestamp/date integers, Boolean byte, UUID bytes, UTF-8 Symbol/String, and Binary. The existing
Native Protocol encoder revalidates every cell, descriptor, payload length, decimal precision, and
UTF-8 sequence. Finalization precomputes the exact descriptor, cell, output, and conservative
working-memory sizes and rejects configured exhaustion before constructing the payload.

No LIMIT or ORDER BY is applied at workers or during sufficient-state merge. At this final global
boundary, absent or positive LIMIT emits one row; LIMIT zero emits one zero-row schema-bearing
`QUERY_RESULT` payload. The result owns that one payload, its schema, row count, and exact encoded
byte count. Allocation and conversion failures publish no partial result.

## Consequences and validation

Ungrouped all-type distributed aggregates now have a canonical client payload independent of
tablet count. Work and transient collection memory are O(aggregate width), under explicit working
and payload limits; the maximum width remains 4,096. One synchronous owner thread performs the
conversion, so no synchronization or memory-ordering argument applies.

Focused coverage executes every logical-type conversion branch and decodes the result through the
production Native Protocol reader. It also proves schema-bearing LIMIT zero, wrong plan mode,
definition divergence, scalar-type divergence, and working-memory exhaustion. Allocation injection
uses a heap-owned string scalar and requires resource exhaustion until one complete payload
succeeds. Header self-containment and installed consumption cover the public API.

Authenticated aggregate transport, execution orchestration, and process lifecycle ownership remain
separate tasks. Grouped all-type exchange still requires its own keyed protocol.

Invariants 6, 10, 11, 14, and 18 apply.

## References

- [Native protocol v1](../protocol/native-v1.md)
- [Distributed Vector Aggregate Exchange v1](../formats/distributed-vector-aggregate-exchange-v1.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Architecture invariants](../architecture/invariants.md)
