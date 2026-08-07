# ADR 0046: Exact Bounded SQL ORDER BY Lowering

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-planning and storage-query maintainers

## Context

ADR 0044 provides a bounded physical sort but explicitly forbids using stable scan arrival as SQL
tie behavior. ADR 0045 gives CSEG and mutable-head sources a common opt-in WAL ID, record sequence,
row ordinal, and operation suffix. SQL v1 additionally requires base-row ties to use stable logical
identity before commit position and row ordinal, and aggregate ties to use group-key identity.

The bound tree already records ORDER BY alias resolution and exact expression types. Table schemas
authoritatively identify DEDUP KEY columns. A schema without a DEDUP KEY instead uses an opaque
generated logical identity in the scalar snapshot contract, but current vector sources do not
expose that value.

## Decision

- `lower_bound_sql_select()` accepts ORDER BY for the otherwise supported single-source base,
  global-aggregate, and grouped-aggregate surfaces. It preserves binder-resolved aliases,
  non-projected expressions, declared key order and direction, and SQL default or explicit NULL
  placement.
- The physical sequence is WHERE, scalar/aggregate preparation, aggregation when present, one
  combined visible/order/tie-key output, bounded sort, hidden-column removal, and LIMIT. LIMIT is
  never pushed before the exact sort.
- Base-row ordered plans require source columns in schema order followed by the ADR 0045 suffix.
  Configured SQL keys are followed by DEDUP KEY columns in schema-declared identity order, then WAL
  ID, record sequence, and row ordinal, all ascending with NULL last. WAL ID plus record sequence is
  the accepted logical commit position; the operation suffix is shape-validated but is not a tie
  key.
- Base-row ORDER BY on a schema without a DEDUP KEY is rejected as unsupported until vector sources
  expose the authoritative generated logical identity. The lowerer does not invent a mapping from
  physical ordering keys, scan order, or row-version fields.
- Grouped results append their materialized group-key columns in declared GROUP BY order as
  ascending, NULL-last tie keys. This is the typed canonical group identity used by the scalar
  oracle. A global aggregate has one group and requires no additional tie key.
- An aggregate referenced only from ORDER BY participates in aggregate traversal and preparation.
  A bound alias is re-lowered from its SELECT expression; it does not refer to a sibling output
  position inside the same vector output stage.
- Hidden ordering, logical-identity, group-identity, and version columns are removed by a checked
  subset stage before client-visible output. `PhysicalSelectLoweringLimits` carries finite sort
  limits in addition to the existing expression, aggregate, output, and plan limits.

This decision changes no durable or network format, SQL binding rule, storage visibility rule,
source ownership, or concurrency publication contract.

## Consequences

Supported bound SQL can now use the existing accounted in-memory sort without depending on scan
arrival or sort stability for SQL ties. Ordered base sources must opt into the row-version suffix,
while unordered and aggregate input shapes remain unchanged. Preparation can duplicate an aliased
expression and temporarily retains hidden columns; later common-subexpression elimination or top-N
planning requires separate optimizer evidence.

The implementation remains bounded by the physical sort's configured row, key, state, and output
limits. It does not add spill, external merge, parallel scheduling, multi-part/head composition, or
base/delta visibility resolution.

## Validation

Unit and scalar-oracle tests cover base and aggregate lowering, aliases, non-projected expressions,
multiple keys, direction, default and explicit NULL placement, LIMIT position, hidden removal,
DEDUP/commit/row ties, group-key ties, order-only aggregates, and unsupported generated identity.
Hostile limits, exhaustive lowering allocation failure, lowering fuzz cases, installed consumption,
sanitizers, full repository checks, and retained-plan lowering benchmarks cover the bounded
ownership surface. ADR 0044's operator model and allocation-failure evidence continue to cover the
blocking sort execution primitive.

## References

- [SQL v1 contract](../product/sql-v1.md)
- [ADR 0036](0036-bound-select-to-physical-pipeline-lowering.md)
- [ADR 0043](0043-bound-grouped-aggregate-physical-lowering.md)
- [ADR 0044](0044-query-accounted-bounded-physical-sort.md)
- [ADR 0045](0045-shared-vector-row-version-suffix.md)
- [Bound SELECT physical lowering guide](../learning/bound-select-physical-lowering.md)
