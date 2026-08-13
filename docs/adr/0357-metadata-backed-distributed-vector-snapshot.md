# ADR 0357: Metadata-backed distributed vector snapshot

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, metadata, and distributed-systems maintainers
- **Extends:** [ADR 0297](0297-metadata-backed-distributed-query-authority.md),
  [ADR 0355](0355-compatible-multi-tablet-vector-snapshot.md)

## Context

The compatible vector snapshot binder pinned one Manifest generation but still accepted parallel
caller-supplied schema, placement, and group references. A general vector query needs the same
single-publication metadata join already enforced by the aggregate path.

## Decision

`bind_metadata_backed_distributed_vector_snapshot` accepts one committed canonical metadata catalog,
one plan-ordered runtime proof per fragment, one projection, and one owning Manifest snapshot. The
aggregate and vector entry points share one internal authority resolver. It validates catalog order,
resolves the active schema plus every placement and immutable tablet-to-group binding, proves stable
committed membership, and derives policy-specific admissions from the supplied observations.

The vector entry point then creates only borrowed per-fragment binding views over that resolved
authority and delegates all Manifest, projection, logical-type, plan-shape, order, and compatible
generation checks to the existing vector binders. Only the returned move-only compatible snapshot
escapes; it owns the Manifest pin and complete dispatch values. Catalog, proof, and temporary
admission references do not escape the call. The operation performs no I/O or publication.

## Consequences and validation

The focused two-tablet test constructs grouped SUM/order/LIMIT vector dispatches from one catalog
and two current leader barriers, then verifies exact schema, placement, group, barrier, and plan
values. An aggregate/type mismatch derived from that catalog rejects before publication. Existing
metadata-backed aggregate proof, stale/reconfiguration, bounded-stale, and eventual tests remain
green, covering the shared resolver. Header self-containment and installed consumption cover the
new public entry point.

Leader-linearizable group-keyed proof binding is implemented separately. Correlated follower group
authority, remote proof acquisition, vector worker execution, global coordination, authenticated
transport, and process integration remain incomplete. No Phase 16 exit gate is claimed.

Invariants 4–6, 11, 14, 15, and 18 apply.

## References

- [Metadata-backed distributed query authority](0297-metadata-backed-distributed-query-authority.md)
- [Compatible multi-tablet vector snapshot](0355-compatible-multi-tablet-vector-snapshot.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
