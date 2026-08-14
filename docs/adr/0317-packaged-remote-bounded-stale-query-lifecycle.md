# ADR 0317: Packaged remote bounded-stale query lifecycle

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, cluster, query, and networking maintainers
- **Extends:** [ADR 0304](0304-packaged-bounded-stale-query-construction.md),
  [ADR 0316](0316-placement-backed-raft-observation-batch-construction.md)

## Context

Placement-backed observation construction and batch acquisition still ended before the existing
bounded-stale query constructor. An embedding had to retain the plan and Manifest snapshot, wait for
all authority, pass the result to the metadata binder, transfer ownership into TCP execution, and
route cancellation and terminal results across both phases.

## Decision

`ReplicatedFollowerDistributedAggregateQuery` is a move-only single-threaded lifecycle owner with
explicit acquiring-authority, executing, complete, failed, and cancelled states. Construction
requires the observation and query configs to share the same source node, authenticator, and node
authorizer. It builds the placement-backed canonical observation batch while retaining an owning
plan and copyable pinned Manifest snapshot. Construction opens no socket.

While acquiring, `poll_once` delegates the caller's bounded wait to the batch owner. No authority
escapes. After the complete group-sorted vector exists, the owner invokes
`create_replicated_follower_distributed_aggregate_query`, which reacquires the metadata-group
barrier, proves catalog coverage, binds follower application and Manifest visibility, resolves the
selected query routes, and returns the TCP execution owner. Network query attempts begin only on a
later poll.

Cancellation applies to the active phase and becomes one sticky service-level cancellation.
Acquisition or construction failure cancels any surviving child owner. Execution failure,
cancellation, completion, metrics, and result are projected through the service owner without
exposing intermediate admissions, observations, or sockets. All borrowed catalog, barrier,
projection, authentication, and TLS objects must outlive the owner.

## Consequences and validation

One object now owns the complete in-process boundary from committed placement through authenticated
remote authority to a bound distributed-query execution. The pinned snapshot cannot disappear
during acquisition, and authority cannot be fabricated or substituted between phases. The
metadata barrier is deliberately acquired after remote observations, so the final query binding
uses the configured catalog only when its applied index covers the newly acquired barrier.

A focused service test runs two real mutual-TLS observation servers, acquires one leader/follower
pair exactly once, binds a metadata-barrier-covered Manifest snapshot, transitions to an executing
TCP query owner, exposes both phase metrics, and proves execution-phase cancellation is sticky. The
test requires approved host execution where sandbox policy forbids loopback bind.

A deterministic allocation sweep now covers both lifecycle construction and the complete
mutual-TLS authority-to-scalar-execution transition. Every selected allocation failure is returned
as sticky `RESOURCE_EXHAUSTED`, cancels all authority work, installs no execution or result, and
releases the exact Manifest pin. The sweep removed incorrect `noexcept` specifications from the
service owner and TCP scheduler constructors: their owned diagnostic statuses may allocate, so
termination was not a valid failure boundary.

A subsequent sweep pre-acquires independent lifecycle owners, replaces the observation endpoint
with the real scalar mutual-TLS server, and selects every main-thread allocation across response
decode, retained ownership, sender/coordinator completion, and aggregate installation. Each
injected failure is sticky `RESOURCE_EXHAUSTED`, closes the attempt, exposes no result, and restores
the exact pin; the no-fault boundary publishes the expected count and sum.

The test response comes from a synthetic scalar worker, not a real remote CSEG scan. Real
three-process SQL/data-plane execution, process loss/failover, remote CSEG reads, broader fault
matrices, and measurement evidence remain incomplete.

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## References

- [Packaged bounded-stale query construction](0304-packaged-bounded-stale-query-construction.md)
- [Placement-backed Raft observation batch construction](0316-placement-backed-raft-observation-batch-construction.md)
- [Bound-snapshot distributed query execution](0301-bound-snapshot-distributed-query-execution.md)
