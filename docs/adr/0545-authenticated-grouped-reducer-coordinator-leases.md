# ADR 0545: Authenticated grouped reducer coordinator leases

- **Status:** accepted
- **Date:** 2026-08-30
- **Owners:** ChronosDB distributed-query, cluster transport, service, and daemon maintainers
- **Extends:** [ADR 0543](0543-packaged-grouped-shuffle-job-lifecycle.md) and
  [ADR 0544](0544-authenticated-grouped-reducer-job-cancellation.md)

## Context

Explicit v3 cancellation promptly releases reachable reducer jobs when the coordinator is still
running. A process crash, kill, or permanent coordinator partition sends no cancellation, leaving
every admitted reducer alive until the potentially much longer relative PREPARE timeout. Serializing
an absolute wall-clock deadline would introduce clock-synchronization assumptions, while silently
shortening all v1 jobs would break older coordinators that do not renew.

## Decision

Add fixed Job Control v4 `RENEW_LEASE` request and response frames. The request carries the exact
query/coordinator/target tuple and one bounded relative lease duration. Mutual TLS authenticates and
principal-authorizes the coordinator before the reducer service exact-matches that tuple.

Lease activation is an explicit lifecycle gate after every reducer has accepted the complete v2
route set and before any source worker starts. The first accepted renewal installs a process-local
`steady_clock` deadline. Exact later renewals retain the same duration and replace that deadline
with receipt time plus duration. A conflicting duration fails. Renewal after expiry cannot resurrect
the job. Reducer polling closes all active job owners when the lease expires; the original PREPARE
deadline remains the pre-activation fallback, hard execution bound, and terminal-retention bound.

The reducer-set coordinator owns a distinct finite all-reducer renewal round alongside SEAL and
result collection. It schedules rounds from the start of the preceding round, retries immutable
requests only within the earlier of the lease duration and whole-query deadline, and accepts a
round only when every exact response is successful. Any missing or rejected renewal fails the whole
query and enters the existing best-effort v3 cancellation phase. The coordinator exposes its next
maintenance wake deadline so the whole-query owner cannot block in worker polling across a renewal.
While a renewal socket is active, the single-thread-affine owner caps other waits to a short poll
interval; no background thread or shared-memory publication edge is introduced.

## Consequences

Once route installation and lease activation complete, coordinator process loss releases each
reachable reducer no later than its last acknowledged relative lease duration plus service polling
delay. No synchronized clocks, durable query state, or failure detector is required. Whole-query
output remains all-or-nothing if one renewal fails.

A coordinator that disappears during PREPARE or route installation never activates the lease and
still relies on the original PREPARE deadline. This is deliberate compatibility behavior: no source
worker has started at that point. A paused but live coordinator is indistinguishable from a failed
one and loses the query after the lease bound. Lease state is in-memory and does not recover a query
after either process restarts.

## Ownership, threading, and memory ordering

Coordinator lease acquisitions are move-only and owned separately from the current phase-control
acquisitions so renewal can overlap worker, SEAL, or result progress. The coordinator serializes
both owners on one caller thread, includes their sockets in the same bounded control poll when
possible, and caps result/worker waits by the lease maintenance deadline. The reducer service
serializes renewal and expiry with all job operations on its one poll owner. Borrowed TLS,
authorization, route, authority, and result dependencies retain their existing lifetimes.

No shared-memory concurrency algorithm or new atomic edge is introduced, so there is no new
memory-ordering argument.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): a lease retains exact query, coordinator, target,
  route-installed job, and authenticated-principal identity.
- [Invariant 11](../architecture/invariants.md): renewal acquisition, TLS/socket, job, and teardown
  lifetimes remain explicit and reverse-safe.
- [Invariant 13](../architecture/invariants.md): partial activation or renewal exposes no route,
  source result, reducer result, or Native prefix.
- [Invariant 14](../architecture/invariants.md): v4 is additive and v1-v3 bytes remain unchanged.
- [Invariant 15](../architecture/invariants.md): duration, interval, attempts, backoff, round
  deadline, socket work, service polling, and total query time are bounded.
- [Invariant 18](../architecture/invariants.md): only authenticated exact all-reducer renewal keeps
  the packaged query live; optimization cannot weaken cleanup or output atomicity.

## Validation

Fixed codec and fragmented transport tests cover v4 request/response correlation, checksum damage,
invalid duration, and allocation classification. Service tests cover pre-route rejection, exact
activation, renewal, conflicting duration, non-resurrection, expiry teardown, and metrics. A real
shared-endpoint mutual-TLS coordinator test requires activation before `kPrepared`, then completes
another renewal round while remaining prepared before source delivery. Whole-query tests continue
to exercise worker, SEAL, result, failure, and cancellation ownership with lease maintenance active.

The final warning-as-errors build passed 356 cluster, 80 cluster allocation-failure, 113 service,
7 service allocation-failure, and 1 feature-smoke tests. The same 356/80/113/7 cluster and service
suites passed under ASan/UBSan, and all 356 cluster plus 113 service tests passed under TSan. One
unrelated Raft read-authority timing case failed once during the first full ASan run, passed
immediately in isolation, and the complete 356-test ASan cluster rerun passed. Clang-tidy 18 passed
the changed v4 codec, transport, coordinator, service, whole-query, and provider sources after its
first pass prompted replacement of four implicit optional-owner invariants and one const local that
prevented automatic move. Formatting, workflow-action pinning, and `git diff --check` passed. The
daemon builds on this macOS host, but its Linux epoll process gate cannot run here; no packaged
process-kill or split-leader evidence is claimed.

## Migration or rollback considerations

Versions 1, 2, and 3 are unchanged. Version 4 is additive and activated only by a coordinator that
requires every selected reducer to support it. Rolling back removes lease activation and returns
coordinator-loss cleanup to the original PREPARE timeout. Mixed pre-alpha binaries are not
qualified.

## Unresolved questions

- Qualify lease expiry and fresh whole-query replacement under packaged coordinator process kill
  and network partition on Linux.
- Decide whether a future durable-query mode requires durable lease epoch or fencing identity.
- Support a coordinator that is also a source or destination without network self routes.

## References

- [Job Control v4](../formats/distributed-vector-grouped-aggregate-shuffle-job-control-v4.md)
- [Packaged grouped shuffle job lifecycle](0543-packaged-grouped-shuffle-job-lifecycle.md)
- [Authenticated grouped reducer-job cancellation](0544-authenticated-grouped-reducer-job-cancellation.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
