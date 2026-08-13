# ADR 0364: Schema-light distributed vector results

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, networking, and distributed-systems maintainers
- **Corrects:** the general-result assumption in [ADR 0350](0350-canonical-distributed-vector-batch-exchange.md)

## Context

Columnar Batch v1 requires stored table/schema identity and table-schema shape. It cannot truthfully
identify reordered or repeated projections, aliases, or aggregate-only output. ADR 0065 already
rejected synthetic table identities and established ordered name/type/nullability descriptors for
native query results. Changing Distributed Vector Exchange v1 would violate its accepted bytes.

## Decision

Distributed Vector Result Schema v1 is a distinct checksummed owned descriptor vector using the
same logical descriptor semantics as Native Protocol v1: nonempty UTF-8 name, frozen logical type
and parameters, and nullability. Duplicate names remain legal. It contains no table/schema UUID,
roles, expressions, or native physical objects.

The binder validates the descriptor vector against one Vector Plan Intent and exact projected
physical input shapes. Row output order/repetition is preserved; grouped keys precede aggregates;
aggregate type/nullability comes from `vector_aggregate_output_shape`. Names are carried identities,
not derived guesses. ORDER BY and LIMIT preserve shape.

Existing vector exchange v1 remains unchanged for its table-shaped payload. General distributed
execution will require a distinct result-batch/exchange version that binds this schema and uses the
schema-light native cell contract rather than pretending computed output is a stored table.

## Consequences and validation

Two focused cases round-trip owned duplicate-name mixed descriptors, reject damage/future version/
invalid UTF-8/lower limits, validate repeated row projection plus grouped COUNT/SUM shapes, and
reject width/nullability mismatch. Header self-containment and installed consumption cover the API.

Schema carriage in a versioned fragment is implemented separately. Schema-light result batches,
worker execution, authenticated lifecycle, and process integration remain incomplete. No Phase 16
exit gate is claimed.

Invariants 5, 6, 10, 14, and 18 apply.

## References

- [Distributed Vector Result Schema v1](../formats/distributed-vector-result-schema-v1.md)
- [Self-describing Protocol v1 query result batches](0065-self-describing-query-result-batches.md)
- [Distributed Vector Plan Intent v1](../formats/distributed-vector-plan-intent-v1.md)
