# ADR 0030: Exact Timestamp-Range Vector Filtering

- **Status:** accepted
- **Date:** 2026-08-06
- **Owners:** ChronosDB query-execution, CSEG-read, and schema maintainers

## Context

ADR 0019 permits Manifest and CSEG event-time zone maps to discard only ranges proven disjoint from
a predicate. ADR 0028 implements that conservative two-stage pruning and deliberately labels its
predicate as pruning evidence rather than row-level truth. Selected granules can still contain rows
outside the requested range, so returning every selected row would be an incorrect exact query.

The Phase 9 vector layer already has immutable physical columns, order-preserving selections,
query-accounted chunks, pull operators, and a bounded reusable unary plan. It does not have an exact
timestamp predicate. Using endpoint arithmetic to turn open bounds into closed bounds would also
overflow at the limits of the signed 64-bit `TIMESTAMP_NS` domain.

This increment must remain useful for both canonical CSEG chunks and materialized mutable-head
chunks without claiming that their independently exposed user columns form a complete logical
tablet scan. Hidden row-version columns and base/delta resolution remain unspecified.

## Decision

- `TimestampRangePredicate` is a query-layer conjunction of optional lower and upper signed
  nanosecond bounds. Each bound carries its endpoint and inclusive bit. Reversed bounds and equal
  endpoints with either side open are valid empty predicates, not construction errors.
- `matches()` compares values directly. It never increments or decrements an endpoint, so
  `INT64_MIN` and `INT64_MAX` are ordinary safe values.
- `VectorSelection::where_timestamp_in_range()` requires an exact `TIMESTAMP_NS` physical column
  with the same physical row domain. It stable-compacts the existing indices, drops NULL, preserves
  physical order, and reuses the existing allocation.
- `VectorChunk` and `AccountedVectorChunk` consume and return the same owners. Logical selection
  bytes shrink after filtering; retained capacity and query credit remain conservative and
  unchanged.
- `TimestampRangeFilterOperator` follows the ADR 0022 pull lifecycle. It validates the chunk on the
  pull where it is observed, propagates upstream errors, requests shared cancellation for local or
  upstream failure, returns empty chunks as progress, and returns sticky end after successful
  completion.
- `TimestampRangeFilterStage` integrates the filter into `PhysicalPipelinePlan`. Plan creation
  proves the referenced current-shape column is `TIMESTAMP_NS`; the runtime source-shape boundary
  independently rechecks exact type and nullability before execution.
- A predicate with no bounds means every non-NULL timestamp. NULL never matches, consistent with
  SQL predicate truth rather than metadata-pruning behavior.

Manifest/CSEG pruning and exact filtering intentionally remain separate stages. A future scan
lowering must copy the same endpoint values and inclusive bits into both, ensure the timestamp
column remains available until exact filtering, and only then project it away if the user did not
request it. This increment does not silently add that lowering to the current CSEG-only or
head-only sources.

The decision adds no durable or network format, dependency, concurrency algorithm, or change to
the accepted event-time, schema, CSEG, Manifest, WAL, or Columnar Batch contracts.

## Detailed rationale

Stable selection compaction is the smallest exact implementation over canonical chunks. It avoids
copying column buffers, preserves source order, composes with existing projection and LIMIT stages,
and keeps memory accounting auditable. A separate query-layer predicate prevents a physical
execution primitive from treating CSEG metadata policy as its ownership boundary, while identical
value/inclusivity fields make later lowering mechanical and testable.

Validating type and physical shape before traversal keeps malformed internal operator output from
becoming unchecked byte access. `PhysicalColumnView` has already validated fixed-width canonical
little-endian storage, so each non-NULL timestamp cell is exactly eight bytes and can be decoded
without alignment assumptions or native-struct loads.

## Alternatives considered

- **Treat zone-map selection as exact:** rejected because overlapping granules contain false
  positives by design.
- **Rewrite open bounds by adding or subtracting one:** rejected because it overflows at the signed
  domain edges and obscures the accepted boundary semantics.
- **Materialize matching rows into new columns:** rejected because selection compaction already
  represents the exact ordered subset without allocation or copying.
- **Specialize only the CSEG source:** rejected because the same exact semantics are required for
  head chunks and later composed sources.
- **Automatically wrap current CSEG scans now:** deferred because predicates may target an event-
  time column omitted by the caller projection, and complete part/head lowering is not yet defined.
- **Implement general typed vector expressions first:** deferred; the narrow range primitive is
  already required to make pruning semantically exact and does not constrain the later expression
  representation.

## Consequences

ChronosDB now has an allocation-free exact timestamp-range truth stage that works on any valid
bounded vector chunk and can be retained in a reusable physical plan. Conservative storage pruning
can be followed by an exact operation without changing storage bytes.

Per selected row, the current implementation performs one null check, canonical fixed-width cell
lookup, signed decode, and up to two comparisons. Very sparse selections retain all physical column
buffers and the original reservation until the chunk is released. Those are explicit baseline
costs for profiling, not performance claims.

Callers must not mistake the new operator for complete temporal query execution. SQL lowering,
automatic projection retention, aggregate snapshot composition, hidden version identity,
correction/tombstone visibility, and base/delta merge remain future work.

## Affected invariants

This decision strengthens invariants [6, 13, and 18](../architecture/invariants.md). Exact row truth
is evaluated within each already pinned snapshot chunk; open/closed event-time semantics are not
weakened at integer edges; and zone-map pruning remains a no-false-negative optimization rather
than a substitute for truth evaluation. Invariant 11 remains supported because the operator
retains the input owner and query credit until the output chunk is destroyed.

## Validation plan

- Unit tests cover unbounded, open, closed, reversed, equal-open, `INT64_MIN`, `INT64_MAX`, NULL,
  sparse input selection, empty output progress, sticky end, invalid type/ordinal/shape, foreign
  query credit, and propagated failure/cancellation.
- A deterministic property compares operator output against the independent scalar `matches()`
  oracle across predicates, NULLs, sparse selections, and forced chunk boundaries.
- Physical-plan tests prove type/ordinal rejection, shape preservation, and instantiated execution.
- The vector and physical-plan fuzzers exercise hostile bounds, ordinals, selection shapes, stage
  sequences, and edge values under configured sanitizers.
- `chronos_query_benchmarks` measures dense and sparse timestamp selection compaction at 64, 1,024,
  and 4,096 physical rows, reporting examined items and bytes without claiming end-to-end speed.
- Public-header self-containment, staging-install layout, and installed external-consumer linkage
  cover the exported types, stage, and factory.

## Migration or rollback considerations

There is no persisted state or compatibility migration. Rollback removes the query-layer types and
operator/stage. Any replacement must preserve direct edge-safe comparisons, NULL behavior, stable
order, empty-chunk progress, query-credit ownership, and the separation between pruning evidence
and exact row truth.

## Unresolved questions

Bound-SQL lowering, event-time-column retention through scan projection, automatic exact wrapping
of CSEG/head sources, general vector expression output, hidden row-version columns, complete tablet
composition, and parallel scheduling remain later Phase 9 work.

## References

- [ADR 0007](0007-event-time-system-time-and-row-versioning.md)
- [ADR 0019](0019-rebuildable-pruning-delta-planning-and-part-reclamation.md)
- [ADR 0020](0020-bounded-vector-chunk-representation.md)
- [ADR 0022](0022-pull-based-physical-operator-lifecycle.md)
- [ADR 0023](0023-bounded-physical-pipeline-plan.md)
- [ADR 0028](0028-pruned-multi-part-snapshot-cseg-scan.md)
- [Pruned snapshot CSEG scan](../learning/pruned-snapshot-cseg-scan.md)
- [Physical operator foundation](../learning/physical-operator-foundation.md)
