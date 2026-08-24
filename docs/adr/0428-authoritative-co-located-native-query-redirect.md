# ADR 0428: Authoritative co-located native query redirect

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB service, query, metadata, Raft, and networking maintainers
- **Extends:** [ADR 0295](0295-negotiated-native-leader-redirect.md),
  [ADR 0296](0296-authoritative-replicated-ingest-leader-redirect.md)

## Context

The packaged finite-query client could follow an authenticated exact-group redirect, but the daemon
never emitted one. A table may have several tablet placements, metadata and tablet groups may have
different leaders, and the packaged read gate currently contacts metadata plus every resident
tablet group. Redirecting from one advisory hint would therefore send a whole query to a node that
might not hold the remaining required authority.

## Decision

`ReplicatedQuerySnapshot` derives a redirectable table route only when every committed placement for
that table maps to one identical Raft group, placement epoch, and sorted replica set. The route is
owned by the snapshot and remains only a preliminary binding fact; its unbarriered rows are never
used as the query result.

For a Protocol 2 finite SELECT that negotiated leader redirect, `NativeProtocolService` acquires a
preliminary local-applied snapshot, binds the exact SQL, and considers routing only for one ordinary
table source without ASOF or system-time semantics. `ReplicatedIngestDatabase::resolve_query_leader`
then enqueues an ordered observation behind prior work for every group in the packaged query-barrier
vector. After all observations complete, it reacquires committed metadata and requires:

- the preliminary table route is byte-for-value unchanged;
- every observation names the expected local node and group in a nonzero term;
- log, commit, and apply indexes are ordered;
- membership is stable, committed voters equal voters, and no joint state is pending;
- every resident tablet-group voter set equals its current committed placement replicas; and
- either every group is locally led or every group is a follower naming one identical, in-membership
  remote leader.

All-local authority continues through the existing quorum read barrier and applied-publication
snapshot. Common remote authority returns one `LEADER_REDIRECT` using the exact single table group,
its revalidated placement epoch, and that group's ordered observed term. Candidate or unknown
leadership, split local/remote leadership, different remote leaders, route/membership change,
malformed observation, missing negotiation, and multi-group table placement never redirect. They
continue to fail closed through the normal read path or a correlated error.

## Consequences

A co-located deployment can now move the whole finite query to the one node that currently leads
every group the packaged gate will contact. The redirect remains advisory: the destination repeats
the full barrier and publication proof. A table split across group identities and any split-leader
deployment still require remote fragments; no arbitrary leader is selected.

The preliminary bind adds bounded parsing/catalog ownership only for redirect-negotiating requests.
Observations are processed synchronously on the serial query consumer and retain one bounded value
per configured query group. The asynchronous durable runtime serializes observation publication;
no new memory-ordering algorithm is introduced. No durable or network format changes.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): only ordered committed group state can identify the
  remote leader, and the destination still proves its read barrier.
- [Invariant 6](../architecture/invariants.md): preliminary unbarriered snapshots route only and
  never supply rows; result execution uses the later confirmed snapshot.
- [Invariant 11](../architecture/invariants.md): snapshot routes and observation copies own all data
  used after worker publication.
- [Invariant 14](../architecture/invariants.md): the existing Protocol 2 redirect payload is reused
  without reinterpretation.
- [Invariant 18](../architecture/invariants.md): split authority and metadata races fail closed
  rather than weakening the packaged consistency contract.

## Validation

A durable two-voter database test commits schema, placement, and binding metadata plus both group
histories, reopens the packaged database, installs the same remote leader observation for metadata
and tablet groups, and proves the exact Protocol 2 redirect. It then elects only the metadata group
locally and proves split leadership is rejected. Existing local-leader and partial-residency tests
prove negotiated preflight continues to the normal result and multi-group placement is not exposed
as a single route.

## Migration and rollback

This is additive for clients that negotiate the existing feature bit. Clients without it retain the
previous query behavior. Rollback removes preliminary routing and leaves the barrier/result path,
durable state, and protocol bytes unchanged.

## References

- [Applied replicated read-barrier vector](0294-applied-replicated-read-barrier-vector.md)
- [Packaged single-group routed SQL client](0427-packaged-single-group-routed-sql-client.md)
- [Native Protocol v2](../protocol/native-v2.md)
