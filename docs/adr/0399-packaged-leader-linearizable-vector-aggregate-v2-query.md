# ADR 0399: Packaged leader-linearizable vector aggregate v2 query

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB service, query, cluster, and replicated-runtime maintainers
- **Extends:** [ADR 0302](0302-packaged-replicated-distributed-query-construction.md),
  [ADR 0396](0396-pinned-vector-aggregate-query-v2-tcp-scheduling-and-finalization.md),
  [ADR 0397](0397-metadata-backed-schema-bound-vector-v2-snapshots.md),
  [ADR 0398](0398-committed-vector-v2-query-route-resolution.md)

## Context

The schema-bound aggregate-v2 path could bind canonical replicated authority, resolve committed
routes, own finite multi-tablet execution, schedule definition-bound mutual-TLS attempts, and
finalize one Native Protocol result. An embedding still had to correlate those layers manually and
could route a different compatible owner or lose the caller's exact result-schema authority.

## Decision

`create_replicated_distributed_vector_aggregate_query_v2` is the packaged synchronous
leader-linearizable construction boundary. It accepts one vector plan, acquire-pinned Manifest
snapshot, caller-owned result schema, committed catalog, metadata-group identity, replicated read
barrier, projection, explicit node TLS contexts, authentication policy, and finite nested limits.

Construction requires an ungrouped aggregate plan with leader-linearizable policy and no staleness
bound. It then performs this sequence exactly once:

1. acquire the canonical group-sorted read authority and require the catalog to cover the exact
   metadata-group barrier without aliasing a tablet group;
2. bind every plan tablet through committed schema, placement, group, barrier, Manifest, projection,
   result-schema, and exact cross-tablet aggregate-definition authority;
3. resolve only the compatible owner's immutable serving nodes through that committed catalog and
   explicit TLS map;
4. transfer the owner into one query-wide-memory-bounded portable aggregate execution; and
5. transfer that execution and the owned routes into the TCP scheduler/finalizer.

No intermediate proof, admission, dispatch, definition, or route vector is exposed. The call opens
no socket; polling the returned owner begins connection work. Catalog, barrier, plan, and projection
views need only outlive the synchronous call. Authentication, authorization, and TLS policy are
borrowed for the returned owner's lifetime. Bound plan values, result schema, Manifest pin,
definitions, routes, senders, coordinator, query resources, and carrier slots are owned.

## Alternatives considered

- **Accept an already-bound owner in the service API:** rejected because the process boundary must
  also correlate the metadata barrier and committed catalog used for routes.
- **Recreate aggregate definitions in the scheduler:** rejected because definitions are exact input
  authority and cannot be inferred from result columns.
- **Support follower policy through the leader barrier:** rejected because bounded-stale execution
  requires a correlated remote leader/follower authority contract.

## Consequences

Every nested layer retains its independent validation and status classification. Construction may
block only on the configured replicated read barrier and bounded DNS resolution; sockets remain
closed until polling. Complexity and ownership stay within the supplied binding, route, execution,
carrier, resource, and finalization limits. One caller thread owns construction and the returned
poll lifecycle, so no inter-thread memory-ordering argument applies. No durable or network bytes
change.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): only applied proof-bound tablet positions are
  dispatched.
- [Invariant 6](../architecture/invariants.md): one compatible owner carries the exact snapshot,
  schema, plan, and definitions through finalization.
- [Invariant 11](../architecture/invariants.md): the Manifest pin and query resources outlive every
  attempt and merge.
- [Invariant 14](../architecture/invariants.md): existing versioned fragment, transport, state, and
  Native Protocol formats remain unchanged.
- [Invariant 18](../architecture/invariants.md): packaging removes correlation freedom without
  weakening any nested guarantee.

## Validation plan

Elect durable metadata and tablet groups, advance both applied barriers, bind a real Raft-backed
Manifest snapshot and barrier-covered catalog, and construct an AVG aggregate-v2 owner. Prove the
running scheduler retains the exact group, Manifest generation, result schema, nullable FLOAT64
input definition, and immutable route. Reject row mode before execution. Retain stale-catalog,
missing-group, invalid-limit, header self-containment, installed-consumer, formatter,
static-analysis, ASan/UBSan, and full serialized-suite coverage.

## Migration or rollback considerations

Leader-linearizable aggregate-v2 embeddings should replace manual construction with this function
and retain the borrowed authentication, authorization, and TLS policies for the returned scheduler
lifetime. Rollback is wire- and durable-format compatible but restores unsafe manual correlation.
Follower lifecycle packaging is a separate next contract.

## References

- [Packaged replicated distributed query construction](0302-packaged-replicated-distributed-query-construction.md)
- [Pinned vector aggregate query v2 TCP scheduling and finalization](0396-pinned-vector-aggregate-query-v2-tcp-scheduling-and-finalization.md)
- [Metadata-backed schema-bound vector v2 snapshots](0397-metadata-backed-schema-bound-vector-v2-snapshots.md)
- [Committed vector v2 query route resolution](0398-committed-vector-v2-query-route-resolution.md)
- [Distributed Vector Aggregate Query Transport v2](../formats/distributed-vector-aggregate-query-transport-v2.md)
