# ADR 0032: Exact Event-Time Mutable-Head Scans

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-execution, mutable-head, and schema maintainers

## Context

ADR 0029 materializes one pinned mutable-head publication into bounded canonical query chunks but
deliberately applies no predicate. ADR 0030 provides exact `TIMESTAMP_NS` row truth, and ADR 0031
retains/removes an event-time helper while composing exact durable CSEG scans. Without the same
projection lowering for heads, a caller must either request event time explicitly or manually build
a wrapper pipeline, creating asymmetric semantics across the two existing storage sources.

Mutable heads have no accepted zone-map pruning layer. The required operation is therefore exact
filtering after bounded canonical materialization. This still does not authorize concatenating
heads and parts: their shared hidden row-version shape and visibility merge remain unresolved.

## Decision

- `HeadScanOperator::create()` remains the raw, predicate-free source factory.
- `HeadScanOperator::create_event_time_filtered()` accepts an owned `TimestampRangePredicate` and
  returns a composed `PhysicalOperator` over the same exact `HeadSnapshot` boundary.
- The factory validates the caller projection before changing it. If event time is already
  requested, exact filtering uses its caller-visible output position and preserves output order.
- If event time is omitted, its destination-schema ordinal is appended as the final materialized
  helper. Exact filtering runs on that output and a stable prefix projection removes it, including
  for a zero-column caller output.
- The effective projection including a helper must fit `HeadScanLimits::chunk.maximum_columns`.
  Failure is `RESOURCE_EXHAUSTED` before source reservation or snapshot adoption.
- Bounds retain their exact signed values and inclusive bits. No endpoint arithmetic, head-specific
  normalization, or pruning approximation is introduced.
- Filtering happens after each canonical head chunk is materialized. Empty matching selections are
  valid progress chunks, so later physical chunks remain observable.
- Existing source/output reservations and pin lifetimes remain authoritative. Selection compaction
  and helper removal retain the output reservation conservatively until the chunk is destroyed.

This decision changes no mutable-head storage, publication atomic, durable bytes, schema rules,
dependency, or concurrency algorithm.

## Detailed rationale

Reusing the existing physical filter and subset operators keeps one exact truth implementation for
CSEG-backed, owned head, and future physical chunks. Appending the helper at the end preserves every
caller output ordinal. Because head chunks own copied canonical columns, removing the helper can
release its direct storage while the unchanged reservation remains a safe upper bound.

Validating the original request first preserves useful error classification: an out-of-schema or
duplicate caller ordinal does not become a helper-limit error. The raw factory then independently
validates the effective projection before allocation, retaining the existing defense in depth.

## Alternatives considered

- **Require event time in caller output:** rejected because a physical requirement must not change
  the requested result shape.
- **Filter mutable bytes before canonicalization:** rejected because the physical predicate is
  defined over validated canonical columns and head bitmaps/offsets intentionally use a different
  race-safe representation.
- **Add head zone maps:** deferred; no evidence justifies new publication metadata for this exact
  integration.
- **Change the raw `create()` semantics:** rejected because callers may intentionally need complete
  physical head chunks and because an explicit factory makes the truth boundary visible.
- **Compose heads with CSEG parts now:** deferred until hidden version columns and base/delta
  visibility rules are accepted and differentially tested.

## Consequences

Both existing storage sources now have projection-aware exact event-time entry points. A head query
that omits event time materializes one extra column and scans every captured row; sparse output may
therefore retain a conservatively charged mostly-empty chunk. These are explicit baseline costs.

The source still represents one publication only. It makes no claim about multiple active/sealed
generations, durable-part overlap, correction/tombstone visibility, or complete tablet results.

## Affected invariants

This decision strengthens invariants [6, 11, 13, 16, and 18](../architecture/invariants.md). Exact
truth is evaluated inside the acquire-observed row frontier, the publication pin remains live during
materialization, and helper projection cannot expose unpublished or unrequested state.

## Validation plan

- Unit tests cover open/closed bounds across forced chunk boundaries, a nonzero visible event-time
  position, successor-schema helper removal, zero-column output, empty progress, and effective-limit
  rejection before reservation.
- A deterministic property compares point filtering with the independently known published event
  sequence across multiple chunk widths and absent endpoints.
- Allocation-failure injection covers helper growth, base-source construction, exact-filter
  construction, subset construction, and complete reservation unwind.
- The head fuzzer varies raw versus exact factories, hostile projections/limits/bounds, helper
  removal, cancellation, chunk boundaries, and exact result cardinality under sanitizers.
- `materialize_and_exact_filter_one_head_chunk` measures one hidden-event-time point predicate at
  64, 1,024, and 65,536 rows with source construction excluded.
- Public-header, installation, and external-consumer checks bind the new exported factory.

## Migration or rollback considerations

There is no persisted-state migration. Rollback removes only the exact factory and its composition.
Any replacement must preserve the snapshot frontier, exact bounds, empty-chunk progress, helper
non-leakage, caller order, and failure before unbounded allocation.

## Unresolved questions

Bound-SQL source selection, multiple-head composition, shared hidden system columns, base/delta
row-version resolution, complete tablet scans, general typed expressions, shared pin credit,
parallel scheduling, and spill remain later Phase 9 work.

## References

- [ADR 0029](0029-query-accounted-mutable-head-scan-source.md)
- [ADR 0030](0030-exact-timestamp-range-vector-filtering.md)
- [ADR 0031](0031-exact-prune-then-filter-snapshot-cseg-scans.md)
- [Mutable-head scan source](../learning/mutable-head-scan-source.md)
- [Exact timestamp-range filter](../learning/exact-timestamp-range-filter.md)
