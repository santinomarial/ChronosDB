# ADR 0397: Metadata-backed schema-bound vector v2 snapshots

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, metadata, and distributed-systems maintainers
- **Extends:** [ADR 0357](0357-metadata-backed-distributed-vector-snapshot.md),
  [ADR 0367](0367-bounded-distributed-vector-fragment-v2-ownership.md),
  [ADR 0383](0383-owned-cross-tablet-vector-aggregate-definitions.md)

## Context

Metadata-backed, leader-group-backed, and correlated-follower constructors produced only the v1
compatible vector owner. Schema-bound row and aggregate transports require the v2 owner, whose
result schema and ungrouped aggregate definitions must be proved against the same committed schema,
placement, group, read proof, and Manifest projection. Reconstructing those values later in service
code would reopen the authority join.

## Decision

Three v2 entry points extend the existing authority paths:

- `bind_metadata_backed_distributed_vector_snapshot_v2`;
- `bind_group_backed_distributed_vector_snapshot_v2`; and
- `bind_follower_group_backed_distributed_vector_snapshot_v2`.

The metadata-backed entry point runs the existing one-catalog authority resolver, retaining its
canonical catalog, active-schema, placement, immutable group, stable-membership, and policy-specific
proof checks. It creates call-local fragment binding views over that resolved authority and delegates
to `bind_compatible_distributed_vector_snapshot_v2`. The delegated binder proves the supplied owned
result schema against every exact projected shape and, for ungrouped aggregates, derives and
cross-tablet-compares every complete definition.

The leader and follower entry points first use their existing canonical group-keyed resolvers and
then transfer the resulting plan-ordered proofs into the metadata-backed v2 entry point. Unrelated
groups remain ignored. No catalog, proof, temporary admission, or binding reference escapes; only
the move-only compatible owner, Manifest pin, owned result schema, dispatches, and exact aggregate
definitions survive.

## Alternatives considered

- **Upgrade an already-bound v1 owner:** rejected because it no longer owns the destination schema
  references needed to prove projected result and aggregate-input shapes.
- **Let the service reconstruct definitions from output descriptors:** rejected because COUNT,
  AVG, and variance outputs do not uniquely identify their input authority.
- **Add schema fields to the group proof:** rejected because result schema is query authority, not a
  Raft observation property.

## Consequences

The operation remains allocation-bounded, synchronous, and free of I/O. Work is linear in tablets,
projection width, result columns, and aggregate width. Temporary references die before return; the
compatible owner retains one Manifest generation and one shared schema/definition vector. One
caller thread owns construction, so no synchronization or memory-ordering argument applies.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): only proof-bound committed positions form
  dispatches.
- [Invariant 6](../architecture/invariants.md): all tablets, schemas, projections, and definitions
  come from one stable compatible snapshot.
- [Invariant 11](../architecture/invariants.md): the returned owner pins referenced Manifest
  storage.
- [Invariant 14](../architecture/invariants.md): Fragment-v2 and result-schema formats remain
  unchanged and versioned.
- [Invariant 18](../architecture/invariants.md): eliminating service-side reconstruction preserves
  the existing proof and snapshot guarantees.

## Validation plan

Bind a two-tablet leader-linearizable AVG plan from canonical group-sorted authorities and prove the
owned result schema, exact nullable FLOAT64 input definition, group identities, and one Manifest
generation. Bind a bounded-stale row plan through a same-term leader/follower pair and prove the
follower route and exact shared schema. Retain existing missing-group, mixed-term, schema mismatch,
and projection-bound failures. Run header self-containment, installed consumption, formatting,
static analysis, ASan/UBSan, allocation-failure neighbors, and the full serialized suite.

## Migration or rollback considerations

No durable or network bytes change. Packaged query construction should move to these v2 entry
points before creating schema-bound execution. Rollback must keep the same single-call authority
join and must not infer schema or aggregate definitions after v1 binding.

## References

- [Metadata-backed distributed vector snapshot](0357-metadata-backed-distributed-vector-snapshot.md)
- [Bounded distributed vector fragment v2 ownership](0367-bounded-distributed-vector-fragment-v2-ownership.md)
- [Owned cross-tablet vector aggregate definitions](0383-owned-cross-tablet-vector-aggregate-definitions.md)
- [Distributed Vector Fragment v2](../formats/distributed-vector-fragment-v2.md)
