# ADR 0544: Authenticated grouped reducer-job cancellation

- **Status:** accepted
- **Date:** 2026-08-30
- **Owners:** ChronosDB distributed-query, cluster transport, service, and daemon maintainers
- **Extends:** [ADR 0542](0542-finite-grouped-reducer-job-coordinator.md) and
  [ADR 0543](0543-packaged-grouped-shuffle-job-lifecycle.md)
- **Extended by:** [ADR 0545](0545-authenticated-grouped-reducer-coordinator-leases.md)

## Context

The packaged lifecycle could close local worker, control, and result owners on cancellation or
failure, but an admitted remote reducer job remained live until its relative deadline. A naïve
CANCEL request also has a cross-connection race: closing an in-flight PREPARE socket does not prove
the remote service processed PREPARE before a new cancellation connection.

## Decision

Add fixed Job Control v3 CANCEL request and response frames. Mutual TLS authenticates and
principal-authorizes the coordinator before the reducer service exact-matches query, coordinator,
and target identity. Cancelling an installed job synchronously closes all service-owned ingress,
source, and result execution. Exact duplicate cancellation is idempotent.

If no job exists, retain a bounded expiring cancellation tombstone and acknowledge only after that
tombstone is installed. A later PREPARE with the same query/coordinator/target fails as cancelled;
conflicting coordinator reuse fails. The default retention equals the existing 24-hour maximum job
timeout and may be reduced by deployment configuration. Capacity exhaustion fails closed. These
tombstones are in-memory execution guards and carry no durability guarantee.

The reducer-set coordinator now treats failure and explicit cancellation as a finite fourth control
phase. It cancels current attempts and the result listener, creates one immutable v3 acquisition for
every authority destination, and remains `kCancelling` until all exact acknowledgements arrive or
the existing absolute query deadline or retry budget terminates delivery. The whole-query owner
cancels workers first, preserves the original failure or cancellation status, and keeps polling the
reducer coordinator before becoming terminal. Native client cancellation drains that phase within
the already-authoritative query deadline before returning its cancelled response.

Cancellation delivery failure never makes partial query output visible. Metrics distinguish
acknowledged reducer cancellations and delivery failure. A reducer that cannot be reached still
expires under its PREPARE-relative deadline.

## Consequences

Explicit client cancellation and coordinator-observed execution failure promptly release reachable
remote jobs, including the cancel-before-prepare race. The operation remains finite and uses the
same immutable routes, TLS identities, correlation, backoff, and absolute deadline as the query.

This does not detect a crashed coordinator, durably recover queries, or prove a remote job was
cancelled when its node is unreachable. Those cases retain deadline cleanup and future fault-
detection work. The gateway-only self-route restriction is unchanged.

## Ownership, threading, and memory ordering

The coordinator and whole-query execution remain move-only and single-thread-affine. Callers must
continue polling while either exposes `kCancelling`. The reducer service serializes job and
tombstone admission with all other job operations on its one poll owner. Tombstone storage is
pre-reserved at service construction and bounded independently from active jobs.

No shared-memory concurrency algorithm or new atomic edge is introduced, so there is no new
memory-ordering argument.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): cancellation retains exact query, coordinator,
  target, route, and authenticated-principal identity.
- [Invariant 11](../architecture/invariants.md): worker, control, TLS/socket, result, remote-job, and
  tombstone lifetimes have explicit reverse-safe owners.
- [Invariant 13](../architecture/invariants.md): failure or cancellation still exposes no source,
  reducer, or Native result prefix.
- [Invariant 15](../architecture/invariants.md): tombstones, attempts, backoff, connection,
  exchange, per-poll work, and total cancellation time are bounded.
- [Invariant 18](../architecture/invariants.md): cleanup acknowledgement requires authenticated,
  authorized, exact-correlated v3 success.

## Validation

Codec and fragmented transport tests cover fixed v3 request/response correlation and corruption.
Service tests cover exact duplicate cancellation, cancel-before-prepare tombstones, expiry,
installed-job teardown, metrics, and allocation classification. A real shared-endpoint mutual-TLS
whole-query test cancels before PREPARE admission, drives `kCancelling`, and requires a reducer
tombstone before terminal local cancellation. The existing partial-PREPARE failure test now requires
the admitted reducer to observe remote CANCEL even when another reducer is unreachable.

The final warning-as-errors build passed 353 cluster, 79 cluster allocation-failure, 113 service,
7 service allocation-failure, and 1 feature-smoke tests. The same 353/79/113/7 cluster and service
suites passed under ASan/UBSan, and all 353 cluster plus 113 service tests passed under TSan. The
sanitizer pass exposed and the implementation fixed an accidentally non-progressing whole-query
`kCancelling` branch before acceptance. Clang-tidy 18 passed the changed v3 codec and coordinator
sources after replacing one statically unchecked optional access. Formatting, workflow-action
pinning, and `git diff --check` passed. The two Linux daemon process tests cannot be qualified on the
macOS development host: the process reports that its epoll reactor requires Linux before serving.

## Migration or rollback considerations

Versions 1 and 2 are byte-for-byte unchanged. Version 3 is additive. Rolling back to a binary
without v3 restores reducer-deadline cleanup; callers must not claim prompt remote cancellation.
Mixed pre-alpha binaries are not qualified.

## Unresolved questions

- Qualify coordinator lease expiry and replacement under packaged process loss.
- Decide whether cancellation tombstones require durable recovery in a future durable-query mode.
- Qualify cancellation under packaged multi-daemon partitions and abrupt process loss.

## References

- [Job Control v3](../formats/distributed-vector-grouped-aggregate-shuffle-job-control-v3.md)
- [Authenticated grouped reducer coordinator leases](0545-authenticated-grouped-reducer-coordinator-leases.md)
- [Packaged grouped shuffle job lifecycle](0543-packaged-grouped-shuffle-job-lifecycle.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
