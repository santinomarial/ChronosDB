# ADR 0549: Pre-activation coordinator process-loss qualification

- **Status:** accepted
- **Date:** 2026-08-30
- **Owners:** ChronosDB distributed-query, cluster transport, reducer service, and test maintainers
- **Extends:** [ADR 0543](0543-packaged-grouped-shuffle-job-lifecycle.md),
  [ADR 0545](0545-authenticated-grouped-reducer-coordinator-leases.md), and
  [ADR 0548](0548-coordinator-process-loss-reducer-lease-qualification.md)

## Context

The packaged reducer lifecycle deliberately activates its relative coordinator lease only after
every PREPARE and INSTALL_ROUTES response succeeds. A coordinator that disappears earlier leaves no
lease to expire and relies on the relative execution deadline admitted by PREPARE. Focused service
tests covered that deadline, while the existing process gate killed a coordinator only after lease
activation. There was no independent-process evidence at either pre-activation boundary, and the
service metrics did not distinguish execution-deadline expiry from authenticated cancellation.

## Decision

Add a saturated `execution_expirations` reducer-service metric. Increment it only when the original
PREPARE-relative execution deadline cancels a live job; explicit CANCEL and v4 lease expiry retain
their existing distinct counters. The metric changes no protocol, durable state, or cleanup
semantics.

Extend the standalone production-owner process harness with two deterministic gates:

1. acknowledged PREPARE with no INSTALL_ROUTES request applied; and
2. acknowledged INSTALL_ROUTES with no v4 lease request applied.

At either gate, the reducer child verifies the completed shared-endpoint response count and exact
service metrics, reports the boundary, and stops itself with `SIGSTOP` before another server poll.
The parent kills the coordinator with `SIGKILL`, resumes the reducer with `SIGCONT`, and the reducer
closes the suspended control endpoint before processing any queued later request. It then starts a
fresh production shared endpoint over the same retained job service. This makes the observed state
an exact PREPARE-only or routes-installed/no-lease boundary rather than a scheduling race.

The first query uses a 500 ms execution timeout and a 200 ms lease duration. Because no lease was
activated, the parent requires `execution_expirations` to advance within two seconds; a lease expiry
cannot satisfy the assertion. A distinct fresh query identity must then PREPARE, install routes,
activate and renew its lease, deliver authenticated cancellation, and exit cleanly through the
restarted endpoint.

## Consequences

There is now independent-process evidence for both pre-activation cleanup boundaries and the
existing post-activation lease boundary. Abrupt coordinator loss cannot retain PREPARE-only or
route-installed reducer state beyond the admitted execution budget, and cleanup leaves the service
usable for a fresh whole-query identity.

The fallback is intentionally slower than the activated lease and does not promise prompt cleanup.
No worker has started before activation, so the abandoned state cannot publish source, reducer, or
Native output. Restarting only the control endpoint is test orchestration around the same live
production job service; it is not reducer-process recovery or durable query resumption.

This gate does not cover a crash during partial PREPARE or route responses, multi-reducer partial
acknowledgement, network partitions, process restart, or durable query recovery. Those require
separate fault campaigns and, for resumption, a durable identity and fencing contract.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): both abandoned and replacement jobs retain exact
  query, coordinator, target, route, and authenticated-principal identity.
- [Invariant 11](../architecture/invariants.md): endpoint shutdown discards suspended connections
  before the retained service resumes, while every borrowed service dependency stays alive.
- [Invariant 13](../architecture/invariants.md): neither pre-activation boundary can publish source,
  reducer, or Native output.
- [Invariant 15](../architecture/invariants.md): the PREPARE timeout, process waits, endpoint
  restart, service capacity, retries, and replacement query remain explicitly bounded.
- [Invariant 18](../architecture/invariants.md): qualification uses production owners and exact
  authenticated protocols without weakening the fallback to make the test pass.

## Validation

A focused deterministic service test proves expiry occurs exactly at the admitted execution
deadline and increments `execution_expirations` plus `cancelled_jobs`, but not `lease_expirations`.
Two standalone process tests prove the acknowledged-PREPARE and acknowledged-route-install gates,
coordinator kill, endpoint discard, execution-deadline expiry, and fresh replacement lifecycle.
The complete warning-as-error cluster suite passes 362 tests, the allocation-failure suite passes
80 tests, and the five-test process suite passes normally and in ten consecutive complete-suite
repetitions. Focused ASan/UBSan passes the three cancellation/expiry service cases and the complete
process suite; the complete process suite also passes under TSan and a GCC 13 Ubuntu 24.04
container. Repetition exposed and prompted correction of a harness-only race where a poll that
completed authenticated cancellation returned the expected terminal `Cancelled` status; the child
now accepts that status only when the production owner is already `kCancelled`.

Clang-format 18, workflow-action pinning, and whitespace checks pass. Pinned clang-tidy 18 reaches
only the known macOS 26 libc++ unsupported-builtin errors and reports no project-source diagnostic.
Linux qualification keeps warnings enabled but does not promote the repository's unrelated GCC
warning debt to errors.

## Migration or rollback considerations

No durable or network migration exists. The new metric is process-local observability. Rolling back
removes the explicit counter and process evidence without changing the established PREPARE timeout
or v4 lease behavior. Mixed pre-alpha binaries remain unqualified.

## Unresolved questions

- Qualify partial frames, partial multi-reducer acknowledgements, and asymmetric partitions.
- Specify durable query/job identity, fencing, and restart recovery before resumption is allowed.
- Measure cleanup and replacement behavior under CPU stalls, loss, and larger reducer sets.

## References

- [Job Control v1](../formats/distributed-vector-grouped-aggregate-shuffle-job-control-v1.md)
- [Job Control v2](../formats/distributed-vector-grouped-aggregate-shuffle-job-control-v2.md)
- [Job Control v4](../formats/distributed-vector-grouped-aggregate-shuffle-job-control-v4.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
