# ADR 0302: Packaged replicated distributed query construction

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, query, cluster, and replicated-runtime maintainers
- **Extends:** [ADR 0178](0178-pinned-multi-tablet-tcp-query-scheduling.md),
  [ADR 0299](0299-correlated-replicated-read-authority.md),
  [ADR 0300](0300-group-keyed-distributed-query-proof-binding.md),
  [ADR 0301](0301-bound-snapshot-distributed-query-execution.md)

## Context

All production-strength pieces for the specialized distributed aggregate path existed, but an
embedding still had to invoke them in the correct authority order. It could acquire a barrier and
then bind different metadata, manually rebuild admissions, or resolve routes for dispatches from a
different compatible snapshot.

## Decision

`create_replicated_distributed_aggregate_query` is the packaged synchronous construction boundary
for a leader-linearizable aggregate. Given one logical plan, one acquire-pinned Manifest v2
snapshot, one committed metadata catalog, the metadata Raft-group identity, one replicated
read-barrier owner, and explicit node-specific TLS policy, it performs this sequence exactly once:

1. acquire the group-sorted correlated read authority and require the catalog applied index to
   cover the exact metadata-group barrier;
2. bind selected tablets to exact groups, schema, placement, barriers, and durable Manifest state;
3. resolve only immutable dispatch targets through the same committed catalog;
4. create execution directly from the bound snapshot; and
5. return the existing move-only TCP poll/cancel/result lifecycle owner.

Construction supports only leader-linearizable policy because `ReplicatedReadBarrier` proves
current leaders. Bounded-stale follower proof acquisition remains a separate authority protocol.
The metadata group must be present, applied through its barrier, and distinct from every tablet
group; omission is unavailable and aliasing is corruption.
Authentication, node authorization, and TLS contexts are borrowed and must outlive the returned
owner; the plan, compatible snapshot, routes, senders, coordinator, and carrier slots are owned.

## Consequences

A production embedding no longer correlates intermediate proof, catalog, dispatch, admission, or
route vectors. Every layer still revalidates its own contract and returns its existing status
classification. The call may block only while awaiting the configured read barrier; it opens no
socket until the returned owner is polled. Allocation and cardinality remain bounded by the supplied
binding, route, execution, and carrier limits. No durable or wire format changes.

The caller remains responsible for acquiring a mutually compatible committed catalog and Manifest
publication and for serially driving the returned owner. Whole-query rebinding still requires a
fresh call and the existing explicit compatibility gate.

## Validation

A service test elects durable single-voter metadata and tablet groups, advances their application
boundaries, acquires exact correlated authority, binds a one-tablet Raft-backed Manifest snapshot
and barrier-covered catalog, resolves an explicit mTLS route, and receives a running owner with the
exact pinned generation and group dispatch. A barrier that omits the selected tablet group fails
unavailable before an execution is returned. Public header and installed-consumer builds cover the
composition boundary.

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## Migration and rollback

Replicated leader-linearizable aggregate embeddings should replace manual construction with this
function and retain policy owners for the returned scheduler lifetime. Rolling back restores the
manual sequence without changing persisted state or network compatibility.

## References

- [Pinned multi-tablet TCP query scheduling](0178-pinned-multi-tablet-tcp-query-scheduling.md)
- [Correlated replicated read authority](0299-correlated-replicated-read-authority.md)
- [Group-keyed distributed query proof binding](0300-group-keyed-distributed-query-proof-binding.md)
- [Bound-snapshot distributed query execution](0301-bound-snapshot-distributed-query-execution.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
