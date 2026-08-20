# ADR 0102: Exact multi-tablet subscription snapshots

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB query, live-query, storage, and networking maintainers
- **Extends:** [ADR 0096](0096-plan-bound-subscription-snapshot-execution.md) and
  [ADR 0095](0095-multi-tablet-subscription-delivery-order.md)

## Context

A canonical multi-tablet continuation vector is gap-free only if the historical query reads every
component at its registered position. Applying a complete SQL plan independently to each tablet is
also wrong for global aggregation, ordering, latest, and limit semantics.

## Accepted decision

`MultiTabletSnapshotSubscription` registers the coordinator first, then acquires one aggregate
database storage publication. It exact-validates database, table, schema, every canonical tablet,
WAL lineage, and applied record sequence against the complete registered vector. One mismatch
cancels the subscription; no partial snapshot is emitted.

The query layer creates one checked raw scan per tablet under one shared publication reservation,
concatenates those sources in canonical tablet order, and instantiates the physical pipeline once
above the combined source. Thus filters and projections remain streaming while global aggregate,
sort, latest, and limit stages see the complete row set. The current append-only storage epoch is
WAL-bound, so every member must match the publication WAL; a future Raft snapshot vector requires a
separate accepted boundary.

Output chunks use the existing self-describing `QUERY_RESULT` encoding. The driver emits exactly
one empty schema-bearing `END_STREAM` result after the global pipeline ends, and only then completes
the manager snapshot phase and emits `SUBSCRIPTION_READY`. Post-registration committed changes stay
buffered and unavailable until READY. Destruction or any pre-READY failure cancels the subscription
through the no-token abandonment path and releases query reservations and publication pins. Thus
teardown cannot leave an active subscription merely because resume-token allocation failed.

## Consequences and alternatives

Tablet scans are sequential in v1. This preserves deterministic bounded ownership and global SQL
semantics; later parallel scan scheduling may replace the source merge only if downstream ordering
and cancellation contracts remain intact. Running the full plan per tablet was rejected because it
changes SQL results. Acquiring separate database publications per tablet was rejected because the
result would not name one stable aggregate epoch.

The driver still receives an already prepared physical plan. Durable fingerprint-to-plan lookup,
provided by ADR 0103, can feed the durable coordinator owner directly without exposing its mutable
manager. Reactor worker dispatch and packaged service lifecycle remain separate work.

**Retrospective update (ADR 0411):** source-tagged continuation vectors now compose WAL and Raft
members without asserting one scalar epoch across them. The WAL subset still uses exactly the
aggregate-publication contract defined here.

## Affected invariants and validation

Invariants 4, 6, 11, 12, 15, and 17 apply. Focused tests build two published tablets, prove one
global `COUNT(*)` result, verify live changes remain unavailable before READY, decode END_STREAM and
READY, cancel on one-component boundary mismatch, and destroy a pre-READY driver while proving the
manager is cancelled and query credit released. Concurrent publication schedules, all global
operator shapes, allocation sweeps, socket backpressure, and Raft-backed vectors remain Phase 18
work.
