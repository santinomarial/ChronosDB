# ADR 0343: Packaged leader-linearizable grouped-query construction

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB service, query, cluster, and replicated-runtime maintainers
- **Extends:** [ADR 0302](0302-packaged-replicated-distributed-query-construction.md),
  [ADR 0340](0340-compatible-grouped-float64-snapshot-binding.md),
  [ADR 0342](0342-pinned-grouped-query-tcp-scheduling.md)

## Context

The leader-linearizable aggregate constructor already joined correlated barriers, committed
metadata, one pinned Manifest snapshot, and authenticated routes. The grouped binder and TCP
scheduler existed, but an embedding still had to repeat that authority sequence and could specialize
a different aggregate snapshot or schema into grouped dispatches.

## Decision

`create_replicated_distributed_grouped_float64_query` is the packaged synchronous construction
boundary for the nullable-FLOAT64 grouped aggregate path. Its distinct configuration owns no
policy objects; the committed catalog, read-barrier owner, authentication policies, projection, and
node TLS contexts are borrowed and must outlive the returned scheduler.

The constructor requires a leader-linearizable plan, acquires the exact group-sorted correlated
authority, proves metadata-group barrier coverage and nonaliasing, and invokes the existing
group-backed compatible aggregate binder. It resolves only that owner's immutable serving nodes
through the same committed catalog before any socket opens.

`bind_compatible_distributed_grouped_float64_snapshot` now has a specialization overload that
accepts the move-only compatible aggregate owner. It exact-matches every dispatch's table and
destination schema, validates the shared projected key bounds and FLOAT64 type, derives each grouped
dispatch from the aggregate owner's exact group and nested fragment, and transfers the same
Manifest pin. No admission, group, fragment, or route vector can be substituted by the caller. The
packaged constructor then creates the grouped execution and TCP scheduler.

The constructor does not support bounded-stale authority or explicit whole-query rebinding. Those
remain distinct contracts. No durable or network format changes.

## Consequences and validation

The authority and route work retains the existing bounded complexities. Grouped specialization is
linear in fragments plus copied projected ordinals and retains one grouped dispatch vector beside
the compatible aggregate owner. Construction opens no socket until the returned poll owner is
driven. All objects after the borrowed policy boundary are move-owned.

The focused replicated service test uses durable single-voter metadata and tablet Raft groups,
applied correlated barriers, a barrier-covered catalog, and a real Manifest-v2 snapshot. It proves
the returned running grouped scheduler retains the exact group, generation, and key input. Reusing
the boundary with the projected TIMESTAMP column fails `NOT_SUPPORTED` before I/O. Public-header
self-containment and the installed-consumer gate cover the constructor.

Bounded-stale grouped construction, remote acquisition composition, general vector fragments,
multi-key/non-FLOAT64 grouping, and broad fault/measurement evidence remain incomplete. No Phase 16
exit gate is claimed.

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## References

- [Packaged replicated distributed query construction](0302-packaged-replicated-distributed-query-construction.md)
- [Compatible grouped FLOAT64 snapshot binding](0340-compatible-grouped-float64-snapshot-binding.md)
- [Pinned grouped-query TCP scheduling](0342-pinned-grouped-query-tcp-scheduling.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
