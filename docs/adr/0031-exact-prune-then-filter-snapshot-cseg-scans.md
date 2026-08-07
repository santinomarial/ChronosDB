# ADR 0031: Exact Prune-Then-Filter Snapshot CSEG Scans

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-execution, CSEG-read, and schema maintainers

## Context

ADR 0028 composes the durable CSEG parts of one snapshot tablet and uses Manifest and authenticated
CSEG extrema to skip disjoint parts and granules. Those zone maps deliberately permit false
positives. ADR 0030 supplies exact row-level `TIMESTAMP_NS` filtering, but callers would have to
retain the event-time column manually and could otherwise expose candidate rows as query matches.

The aggregate CSEG factory already knows the destination schema, caller projection, exact pruning
predicate, selected images, and query limits. It is therefore the first boundary that can safely
compose storage work avoidance with authoritative row truth. This remains narrower than bound SQL
lowering or a complete tablet scan because mutable heads and hidden row-version semantics are not
part of the source.

## Decision

- `SnapshotCsegPartScanPlan` retains one owned `cseg::EventTimePredicate`. During planning it is
  conservative evidence only.
- `create_snapshot_cseg_part_scan` mechanically copies both endpoint values and inclusive bits to
  a query-layer `TimestampRangePredicate` and applies it after the sequential pruned CSEG source.
  No endpoint arithmetic or semantic normalization is permitted.
- If the caller requested the destination event-time column, its caller-visible output position is
  used for exact filtering and the original projection order is unchanged.
- If the caller omitted event time, the factory appends its destination ordinal to every child
  scan as a final helper column. Exact filtering runs on that final column and a stable prefix
  projection removes it before any chunk is returned.
- Reader and chunk projection limits apply to the effective scan projection, including the helper.
  A helper that would cross either limit fails with `RESOURCE_EXHAUSTED` before image adoption or
  query reservation.
- Exact filtering wraps the aggregate sequential source once, not every part child. This keeps one
  truth stage over a uniform output shape while part and granule pruning remain inside each child.
- Empty filtered chunks remain valid progress. End, error, cancellation, pin lifetime, and query
  credit follow the existing physical-operator contracts.
- `CsegScanOperator::create_event_time_pruned` remains explicitly pruning-only. Its callers may use
  it for metadata/page-work control, but must not treat its complete candidate granules as exact
  matches.

This decision changes no durable bytes, schema semantics, dependency, storage ownership,
concurrency algorithm, or public CSEG format.

## Detailed rationale

Appending the helper at the end means the existing CSEG projection machinery preserves every
caller output position. Removing a strict prefix is expressible by the existing stable
`ColumnSubsetOperator`, including zero visible columns while retaining exact row cardinality. The
schema role resolves event time by nominal `ColumnId`; source-to-destination schema projection then
continues to handle retained ancestors and successor schemas normally.

Wrapping the sequential source avoids duplicating filter state and helper-removal configuration per
part. It also makes the semantics independent of physical part boundaries: every returned
candidate chunk passes through the same exact predicate before projection.

## Alternatives considered

- **Treat zone maps as exact:** rejected because granules may contain nonmatching rows.
- **Require callers to request event time:** rejected because physical storage requirements must
  not leak into the requested output shape.
- **Filter each CSEG child independently:** rejected because one aggregate wrapper is simpler and
  preserves the same results and ownership.
- **Push row filtering into the CSEG decoder:** rejected because exact predicates also apply to
  mutable-head and future computed chunks; the decoder's responsibility remains physical
  validation and candidate work selection.
- **Compose CSEG and mutable heads now:** deferred until shared hidden row-version shape and
  visibility/merge semantics are accepted.

## Consequences

An aggregate snapshot CSEG scan with an event-time predicate now returns only exact matching rows.
It can decode one extra projected column when event time is not visible, and sparse matches retain
the complete selected granule backing until the chunk is released. Both costs are bounded and
measured rather than hidden.

The plan's selected-row metric remains a conservative candidate count because it is computed before
page decode. It must not be reported as the exact result cardinality.

## Affected invariants

This decision strengthens invariants [6, 11, 13, and 18](../architecture/invariants.md). Selected
snapshot owners remain pinned through exact evaluation, projection never changes snapshot identity,
and pruning can no longer leak false-positive rows through this aggregate factory.

## Validation plan

- Deterministic tests cover point matches, open/closed ranges, caller-visible event time at a
  nonzero output position, omitted event time through a successor-schema NULL column, both
  effective projection limits, empty plans, and exact reservation release.
- A fixed-seed property compares aggregate point results with an independent row model across
  matching and disjoint parts.
- Allocation-failure injection covers child creation, exact-filter construction, helper-removal
  construction, and complete reservation unwind.
- The CSEG scan fuzzer composes hostile pruned decode with exact filtering and optional helper
  removal under sanitizers.
- The CSEG benchmark separately measures pruning-only selected-granule pulls and
  prune-then-exact single-row pulls with source construction excluded from timing.
- Existing public-header, install, and external-consumer tests cover the behavior through the
  unchanged exported aggregate factory.

## Migration or rollback considerations

There is no persisted-state migration. This intentionally strengthens the observable semantics of
`create_snapshot_cseg_part_scan` when its plan carries a predicate: callers that relied on complete
candidate granules must instead use the explicitly pruning-only single-part factory. A replacement
must preserve exact bounds, helper non-leakage, caller projection order, and bounded failure before
adoption.

## Unresolved questions

Bound-SQL lowering, exact predicates over mutable-head scans, hidden system columns, base/delta
row-version resolution, complete tablet composition, parallel scheduling, and optimizer expression
placement remain later Phase 9 work.

## References

- [ADR 0019](0019-rebuildable-pruning-delta-planning-and-part-reclamation.md)
- [ADR 0028](0028-pruned-multi-part-snapshot-cseg-scan.md)
- [ADR 0030](0030-exact-timestamp-range-vector-filtering.md)
- [Pruned snapshot CSEG scan](../learning/pruned-snapshot-cseg-scan.md)
- [Exact timestamp-range filter](../learning/exact-timestamp-range-filter.md)
