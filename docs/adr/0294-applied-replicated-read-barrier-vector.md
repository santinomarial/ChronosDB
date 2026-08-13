# ADR 0294: Applied replicated read-barrier vector

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB query, replicated service, Raft transport, and application maintainers

## Context

Replicated native SELECT pinned one stable vector of local committed application publications, but
did not contact each resident group's leader quorum. The unified transport could route ordered
application transitions, yet query threads could not safely call its single-thread-affine owner.
Moreover, Raft barrier completion proves a committed read index but does not prove that metadata or
tablet application has published through that index.

## Decision

`ReplicatedReadBarrier` provides a bounded synchronous query gate over the exact metadata and
resident-tablet group set. Local mode submits a current-term no-op and barrier directly for each
one-voter group. Transported mode retains at most one request; its poll owner submits the same
operations through `RaftTransportRuntime::try_submit_application`, observes every FIFO completion,
and correlates later quorum completion by exact group, term, and nonzero context. Admission
backpressure retries without allocating, unavailable leaders retry until the finite request
deadline, cancellation discards the request, and stale completions cannot satisfy a later context.

Every barrier first ensures that the leader has a current-term entry. The packaged single-voter
startup also commits that no-op immediately after election. A successful gate returns one sorted
`GroupReadBarrier` per configured query group.

`ReplicatedIngestDatabase::acquire_query_snapshot(barriers)` then requires the immutable metadata
catalog's applied index and every resident tablet publication's matching Raft position to cover the
corresponding read index. The worker-affine application extension publishes those objects before an
asynchronous completion becomes visible, so snapshot acquisition cannot expose a merely committed,
unapplied prefix. Native SELECT uses this overload in both packaged replicated modes.

## Consequences

Replicated native reads now prove current leadership/quorum and application coverage for every
resident component before binding and execution. Partially resident tables still fail closed.
Timeout, leadership loss, transport failure, missing application coverage, and malformed vectors
return an error rather than stale rows.

The result is a leader-confirmed applied vector, not a globally atomic instant across independent
Raft groups. Remote fragments, leader redirection, cross-group transactions, follower-bounded-stale
reads, snapshot installation handling, and a three-process failover harness remain separate work.
One active transported waiter matches the daemon's serial query dispatcher; safe coalescing or
concurrent query admission requires a later bounded design.

This decision directly protects invariants 4, 5, 6, and 18.

## Verification

Focused tests cover sorted one-voter barriers, current-term no-op creation, finite timeout, invalid
group configuration, exact application-publication coverage, native SELECT through the gate, and a
real loopback transport owner. Existing Raft tests cover stale term/context responses, joint quorum
requirements, leader changes, and the distinction between read and applied indexes.

## References

- [ADR 0113](0113-linearizable-raft-read-barrier.md)
- [ADR 0291](0291-stable-local-applied-replicated-query-snapshot.md)
- [ADR 0293](0293-ordered-application-raft-transport-submissions.md)
- [Linearizable Raft read barriers](../learning/linearizable-raft-read-barrier.md)
- [Replicated-ingest database recovery](../learning/replicated-ingest-database.md)
