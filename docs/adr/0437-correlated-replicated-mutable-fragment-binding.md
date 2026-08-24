# ADR 0437: Correlated replicated mutable fragment binding

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB service, query, Raft, and distributed-systems maintainers
- **Extends:** [ADR 0291](0291-stable-local-applied-replicated-query-snapshot.md),
  [ADR 0299](0299-correlated-replicated-read-authority.md),
  [ADR 0436](0436-compatible-mutable-query-authority-rebinding.md)

## Context

The replicated snapshot already owned one committed metadata publication, retained schema
lineages, and every resident immutable TabletState publication. Correlated read authority already
owned the exact leader observation used to validate each barrier. Building mutable fragments still
required an embedding to join plan tablets, publications, committed placements, immutable group
bindings, barriers, observations, projection, and result schema. A caller-side join could pair a
barrier with another group, mix a newer publication into an older plan, or omit one tablet while
returning apparently valid fragments.

## Decision

`ReplicatedQuerySnapshot` now retains the committed metadata publication and durable database
identity used during acquisition. `bind_linearizable_mutable_vector_fragments` accepts one
leader-linearizable vector plan, table identity, canonical group-authority vector, projection,
optional event-time predicate, and result schema. It returns a complete owning fragment vector in
exact plan order or no vector.

The binder requires unique sorted group authority. Every authority must pair one nonzero barrier
with the exact current-term stable leader observation that covers it. For every planned tablet it
then requires a resident pinned publication, matching committed placement and immutable group
binding, stable observed voters equal to placement replicas, publication coverage of the barrier,
and exact planner copies of the serving leader, local applied position, and observed leader commit
position. The established single-fragment binder performs the final schema, projection, result,
placement-leader, and exact Raft publication checks.

The output owns all transport authority, so it may outlive the snapshot. A remote worker still
reacquires its own current immutable publication and exact-revalidates the fragment. If that
publication has advanced, execution returns unavailable and the existing whole-query rebind path
must acquire and bind fresh authority; the server never retargets stale bytes.

## Consequences

Native service composition no longer needs to reconstruct a tablet/group/placement/proof join.
The binder deliberately accepts only leader-linearizable plans; bounded-stale and local-eventual
construction retain their separate proof contracts. A coordinator must be a replica with a pinned
publication for every selected tablet. Nonresident selected tablets fail closed.

Binding uses ordered metadata lookups and a bounded identity set, costing
`O(tablets log tablets + groups log groups)`. It performs no I/O, starts no thread, and introduces
no synchronization or durable/network format. Snapshot metadata and head pins remain immutable
throughout the call.

## Affected invariants

- [Invariant 4](../architecture/invariants.md): each fragment names one exact applied group
  position.
- [Invariant 5](../architecture/invariants.md): only committed/applied TabletState publications are
  admitted.
- [Invariant 6](../architecture/invariants.md): metadata, schema lineage, tablet pins, and proof
  authority are joined under one owning snapshot.
- [Invariant 11](../architecture/invariants.md): the snapshot retains every publication through
  binding and each returned fragment owns its values.
- [Invariant 18](../architecture/invariants.md): mixed or incomplete authority produces no partial
  fragment set.

## Validation

The two-tablet replicated recovery test acquires a correlated three-group authority vector, pins a
barrier-covered snapshot, binds a deliberately reversed tablet plan, and proves exact plan order,
database/group identity, and barrier retention. Changing one planned applied position rejects the
whole bind as unavailable. Header self-containment and the installed external consumer protect the
public API.

## Migration and rollback

This API is additive and is not yet invoked by native request handling. Rollback removes the
snapshot binder and retained metadata pointer without changing any bytes.

## References

- [Distinct proof-bound mutable vector fragment](0429-distinct-proof-bound-mutable-vector-fragment.md)
- [Proof-bound mutable vector query execution](0434-proof-bound-mutable-vector-query-execution.md)
- [Compatible mutable query authority rebinding](0436-compatible-mutable-query-authority-rebinding.md)
