# ADR 0051: Exact Bounded LATEST BY Physical Lowering

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-execution maintainers

## Context

SQL v1 binds `LATEST BY` keys to primary-source schema ordinals and binds one deterministic
primary-source `TIMESTAMP_NS` expression. The scalar oracle selects the greatest timestamp per
typed key tuple, then the greatest schema physical-ordering key, WAL ID, record sequence, and row
ordinal. Phase 9 already has a bounded accounted sort and a shared row-version suffix, but relying
on scan order or stable-sort behavior for equal rows would not implement that contract.

## Decision

- A `LatestByOperator` composes the existing blocking `SortOperator`. Its complete key sequence is:
  LATEST keys ascending/NULL-last; timestamp descending/NULL-last; each schema physical-ordering
  key descending/NULL-first; then WAL ID, record sequence, and row ordinal descending/NULL-last.
  The directions and NULL placement make the exact scalar winner the first row of each adjacent
  LATEST group. Every semantic tie is explicit; sort stability is irrelevant.
- After sort emits its one canonical chunk, LATEST allocation-free compacts the existing selection
  to the first row of every adjacent exact typed key group. Comparison uses the shared scalar-total
  physical-cell comparator, so NULL, floating zero/NaN, decimal parameters, UUID, and variable
  bytes agree with the scalar oracle. Unordered result order remains outside SQL's contract.
- Bound lowering requests the shared row-version suffix for every LATEST query. It first
  materializes all source/suffix columns plus the bound timestamp expression, applies LATEST,
  immediately removes only the timestamp helper, and then lowers WHERE, aggregate preparation,
  aggregation, final output, ORDER BY, hidden-column removal, and LIMIT in that order.
- Plan creation validates finite group/physical-key limits, the derived sort-key bound, timestamp
  type, every ordinal, and the exact non-null four-column suffix. Runtime validates the timestamp
  and suffix again before compaction. A malformed source cancels the query and releases all sort,
  input, and output credit.
- The operation byte is retained with the suffix so source shape remains uniform but is not a
  LATEST tie key. Correction/delete resolution is not inferred here and remains outside the
  accepted append-only snapshot boundary.

## Consequences

The implementation is bounded by the configured sort row/state/output limits and has
`O(R log R * K)` comparison work for `R` rows and complete sort-key width `K`, followed by an
allocation-free `O(R * G)` adjacent group comparison for LATEST key width `G`. This is not the
eventual linear hash/index selection algorithm, but it reuses an already-accounted ownership
boundary and establishes exact semantics before an optimization is considered.

## Alternatives considered

- **Hash winners while scanning:** can be linear, but requires a new query-accounted heterogeneous
  winner table and canonical hashing ownership decision. It is an optimization, not needed for the
  current bounded correctness increment.
- **Stable sort only timestamp and group keys:** would make equal timestamps depend on scan arrival
  order and violates SQL's physical-key and row-version tie contract.
- **Use DEDUP KEY as the tie:** LATEST explicitly uses the schema physical-ordering key. Substituting
  logical dedup identity changes winners.
- **Filter before LATEST:** changes which row wins and contradicts the accepted SQL stage order.

## Affected invariants

This decision supports invariants [6, 11, 16, and 18](../architecture/invariants.md): one snapshot
source supplies the suffix, retained sort memory is query-accounted, cancellation unwinds all
ownership, and the physical result is differentially checked against the scalar semantics.

## Validation plan

- Unit and scalar-oracle tests cover direct/computed timestamps, multiple and NULL keys, all-NULL
  timestamps, physical-key NULL ordering, WAL/sequence/row ties, WHERE/aggregate/ORDER/LIMIT stage
  order, non-projected order expressions, and hidden helper removal.
- Hostile plan/source-shape tests cover invalid ordinals, suffix types/nullability, finite limits,
  cancellation, empty input, and credit cleanup.
- Allocation failure covers every new owned configuration allocation; LATEST compaction itself
  allocates nothing. Physical-plan fuzzing includes hostile LATEST stages.
- Lowering and execution microbenchmarks record group cardinality and row count. ASan/UBSan, TSan,
  static analysis, installation, and external-consumer checks remain release gates.

## Migration or rollback considerations

There is no persisted-state migration. A later hash/index implementation may replace the sort only
if it preserves exact typed grouping, timestamp NULL behavior, the complete physical/version tie,
bounded query ownership, cancellation cleanup, and scalar differential results.

## Unresolved questions

ASOF physical joins, correction/delete winner resolution, optimizer selection, parallel partial
selection, and spill remain separate Phase 9 decisions.

## References

- [ADR 0007](0007-event-time-system-time-and-row-versioning.md)
- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0044](0044-query-accounted-bounded-physical-sort.md)
- [ADR 0045](0045-shared-vector-row-version-suffix.md)
- [SQL v1](../product/sql-v1.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
