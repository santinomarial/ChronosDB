# ADR 0359: Correlated follower vector proof binding

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, Raft, and distributed-systems maintainers
- **Extends:** [ADR 0303](0303-correlated-follower-read-proof-binding.md),
  [ADR 0357](0357-metadata-backed-distributed-vector-snapshot.md)

## Context

Bounded-stale vector construction could accept plan-ordered follower proofs, but its group-keyed
entry point was missing. A naked leader-commit scalar cannot prove which group, term, leader, or
membership produced the lag frontier.

## Decision

`bind_follower_group_backed_distributed_vector_snapshot` accepts a canonical group-sorted vector of
owning leader/follower observation pairs and only a follower-bounded-stale vector plan. The vector
and aggregate paths share one resolver. Every pair must identify the same nonnil group and term,
distinct leader/follower nodes, exact follower-to-leader identity, ordered indexes, no joint or
pending membership, and identical stable voter sets.

For each planned tablet the resolver follows the committed immutable tablet-to-group binding,
selects the exact pair, and derives the leader commit frontier from the leader observation itself.
Those plan-ordered proofs enter the metadata-backed vector binder, which retains placement, lag,
Manifest, schema, projection, plan, and compatible-generation checks. Unrelated valid groups are
ignored. The operation borrows inputs for the call, performs no I/O, and changes no format.

## Consequences and validation

The focused bounded-stale test binds a row-output vector dispatch at the follower's exact applied
position and verifies the serving node, leader frontier, and plan. A leader/follower term mismatch
fails before dispatch construction in both vector and aggregate paths. Header self-containment and
installed consumption cover the public vector entry point.

Authenticated remote observation acquisition, vector worker execution, global coordination,
transport, and process integration remain incomplete. No Phase 16 exit gate is claimed.

Invariants 4–6, 11, 14, 15, and 18 apply.

## References

- [Correlated follower read proof binding](0303-correlated-follower-read-proof-binding.md)
- [Metadata-backed distributed vector snapshot](0357-metadata-backed-distributed-vector-snapshot.md)
- [Proof-bound distributed read admission](0115-proof-bound-distributed-read-admission.md)
