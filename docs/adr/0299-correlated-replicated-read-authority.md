# ADR 0299: Correlated replicated read authority

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB replicated service, query, and Raft maintainers
- **Extends:** [ADR 0294](0294-applied-replicated-read-barrier-vector.md),
  [ADR 0297](0297-metadata-backed-distributed-query-authority.md)

## Context

`ReplicatedReadBarrier` already exact-correlated group, term, context, and current leader
observation while completing a linearizable barrier. It returned only `GroupReadBarrier`,
discarding the observation before query code could use it. The metadata-backed distributed binder
therefore still required an embedding to reacquire or reconstruct the observation that proved the
serving node, term, commit, apply, and membership state.

Reobserving after barrier completion is not equivalent: leadership or membership may change
between operations, and pairing a barrier with an unrelated observation creates a mixed proof.

## Decision

`ReplicatedReadBarrier::await_authority` returns one group-sorted `ReplicatedReadAuthority` per
configured group. This is an alias of query-layer `DistributedAggregateGroupReadAuthority`, so the
result enters group-backed fragment binding without conversion. Each value owns the completed
`GroupReadBarrier` and the exact ordered `RaftGroupObservation` used to validate it. Validation
requires matching group and term, nonzero
local node, current leader role with self leader identity, ordered log/commit/apply indexes, and
commit coverage of the read index.

Transported mode copies the observation attached to the exact application or later quorum
completion only after term/context correlation. Observation allocation failure completes the sole
waiter with resource exhaustion. Local one-voter mode appends `ObserveGroupOperation` after the
current-term commit and barrier operations in the same durable batch, then moves that result into
the authority vector.

The existing `await` API remains barrier-only and does not request, copy, or retain observations.
Both APIs preserve the one-active-waiter rule, sorted group order, finite deadline, and shutdown
contract. A caller must still prove application publication covers the returned read index before
serving rows; the observation alone does not make state visible.

## Consequences

Distributed-query composition can now feed an exact leader observation and its barrier into the
metadata-backed binder without a second Raft observation race. The observation includes stable or
joint membership state; the binder remains responsible for requiring stable membership identical
to committed placement.

Authority capture adds one bounded observation copy per group in transported mode and one ordered
observe operation per group in local mode. The final vector owns all membership arrays and has no
borrowed runtime state. Barrier-only packaged reads keep their prior allocation and operation
profile.

## Alternatives considered

- **Reobserve after `await`:** rejected because term, role, or membership can change between the
  barrier and observation.
- **Return raw runtime pointers:** rejected because the durable and transport owners are
  single-thread-affine and their state cannot escape safely.
- **Always return observations:** rejected because existing local snapshot reads need only barriers
  and should not pay for membership copies.
- **Treat the observation as applied application state:** rejected because Raft `applied_index` and
  immutable database publication still require an exact coverage check.

## Failure modes and operations

Missing, malformed, stale-term, nonleader, or structurally invalid observations fail closed.
Timeout, shutdown, backpressure, and leadership loss retain the existing unavailable behavior.
The API performs no network I/O itself; its poll owner continues to drive the authenticated Raft
transport.

## Validation

Focused local tests acquire two groups, verify sorted barrier/observation identity and term/commit
coverage, and retain the existing barrier-only path. A real transported loopback test captures the
same authority after ordered application completion. Existing barrier tests cover timeouts,
duplicates, current-term progress, and stale completion correlation.

Invariants 4–6, 11, and 18 apply.

## Migration and rollback

Distributed coordinators should call `await_authority`; existing native local snapshot callers may
continue calling `await`. This is an in-memory source API with no durable or wire change. Rolling
back requires a coordinator to reacquire observations and reintroduces the mixed-proof race.

## References

- [Applied replicated read-barrier vector](0294-applied-replicated-read-barrier-vector.md)
- [Metadata-backed distributed query authority](0297-metadata-backed-distributed-query-authority.md)
- [Linearizable Raft read barriers](../learning/linearizable-raft-read-barrier.md)
- [Phase 16 roadmap](../roadmap.md#phase-16--distributed-query-execution-and-rebalancing)
