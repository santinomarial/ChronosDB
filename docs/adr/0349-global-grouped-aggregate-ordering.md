# ADR 0349: Global grouped aggregate ordering

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and distributed-systems maintainers
- **Extends:** [ADR 0348](0348-global-grouped-order-and-limit.md)

## Context

Global grouped result shaping supported ORDER BY only on the FLOAT64 group key. SQL also permits
ordering by aggregate expressions, including aggregates selected solely for ordering. Applying such
order independently at workers is unsound because a key's final COUNT, SUM, extrema, mean, or
variance may combine partials from every tablet.

## Decision

`DistributedGroupedFloat64ResultOrderKey` selects group key, count, sum, minimum, maximum, mean, or
population variance as the sole global order expression. The coordinator still validates terminal
completeness and merges all canonical groups before evaluating the selected aggregate value,
ordering, and applying LIMIT.

COUNT uses exact unsigned order. Floating results use the scalar numeric/NaN contract. Nullable
extrema and variance use the configured explicit null placement, independent of direction. Equal
primary values receive a deterministic ascending group-key/NULLS FIRST tie-breaker; signed zero and
NaN keys were already canonicalized by the exchange. Invalid enum values fail at construction.

This remains the currently supported one-key/one mergeable-state surface. It does not serialize
arbitrary bound expressions or physical plans, exchange arbitrary rows, or provide multi-key and
non-FLOAT64 grouping. No durable or network format changes.

## Consequences and validation

The ordering complexity and bound remain those of ADR 0348. Aggregate evaluation is constant work
per comparison and does not retain additional state. The coordinator remains single-threaded, so no
inter-thread memory-ordering argument applies.

The focused query test merges key 1 across two tablets to SUM 6, orders descending by SUM with LIMIT
2, and proves the deterministic group-key tie places key 1 before another SUM-6 key. It also rejects
an invalid aggregate-order enum. All grouped exchange tests, public header builds, and installed
consumer compilation cover the completed surface.

General vector fragments/exchanges, arbitrary expression ordering, multi-key/non-FLOAT64 grouping,
real multi-process failover, and broad fault/measurement evidence remain incomplete. No Phase 16
exit gate is claimed.

Invariants 5, 6, 10, 14, 15, and 18 apply.

## References

- [Global grouped order and limit](0348-global-grouped-order-and-limit.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Architecture invariants](../architecture/invariants.md)
