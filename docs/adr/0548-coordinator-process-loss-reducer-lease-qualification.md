# ADR 0548: Coordinator process-loss reducer-lease qualification

- **Status:** accepted
- **Date:** 2026-08-30
- **Owners:** ChronosDB distributed-query, cluster transport, reducer service, and test maintainers
- **Extends:** [ADR 0535](0535-independent-grouped-result-process-qualification.md),
  [ADR 0543](0543-packaged-grouped-shuffle-job-lifecycle.md), and
  [ADR 0545](0545-authenticated-grouped-reducer-coordinator-leases.md)

## Context

Job Control v4 implemented authenticated relative coordinator leases and focused tests proved lease
activation, renewal, expiry, and non-resurrection. That evidence kept the coordinator and reducer in
one process. It did not prove that an abrupt operating-system process loss stops renewals, that the
surviving reducer independently expires the job before its longer execution deadline, or that the
same reducer process can admit a fresh whole-query identity afterward.

## Decision

Extend the standalone grouped-shuffle process harness with two production-owner modes:

1. a reducer child owns the bounded reducer-job service behind the shared mutual-TLS query-control
   server and polls lease expiry independently;
2. a coordinator child owns the real reducer-set coordinator, PREPARE and INSTALL_ROUTES phases,
   Job Control v4 activation and renewal rounds, and its result listener;
3. the parent waits for one activation and at least one later acknowledged renewal before sending
   `SIGKILL` to the coordinator; and
4. after the surviving reducer reports lease expiry, a second coordinator uses a distinct nonnil
   query identity, completes activation and renewal, sends authenticated cancellation, and exits
   cleanly.

The qualifying lease is 200 ms while the original reducer execution deadline is five seconds. The
parent requires expiry within two seconds, distinguishing lease cleanup from the older execution
deadline without claiming a latency benchmark. Every control exchange uses the existing frozen
v1-v4 bytes, real TCP, mutual TLS, exact principal-to-node authorization, bounded retries, and the
production shared-endpoint dispatcher.

Fresh replacement deliberately requires a new query identity. An expired lease cannot resurrect
its old job, and this process gate does not introduce a durable lease epoch or authorize reuse of a
crashed coordinator's identity. Terminal reducer records remain bounded by their original retention
contract while a separately admitted fresh identity owns new execution state.

## Consequences

There is now independent-process evidence that post-activation coordinator death stops renewals,
the surviving reducer cancels its owned job under its last acknowledged relative lease, and the
same service remains usable for a fresh query. No production code or format changed: the milestone
closes a process-qualification gap around already implemented owners.

The gate does not prove pre-activation crash cleanup, network partitions, durable query recovery,
automatic retry selection, multi-reducer partial partitions, or restart recovery of an in-flight
query. Pre-activation loss still uses the PREPARE execution deadline. A future durable-query mode
must define durable job identity and fencing before it can resume rather than start fresh.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): every old and replacement control exchange retains
  exact query, coordinator, target, route, and authenticated principal identity.
- [Invariant 11](../architecture/invariants.md): operating-system process ownership now exercises
  the coordinator/reducer lifetime split and independent reducer teardown.
- [Invariant 13](../architecture/invariants.md): killed, expired, and cancelled attempts publish no
  partial reducer or Native result.
- [Invariant 15](../architecture/invariants.md): activation, renewal, process wait, lease expiry,
  retries, and replacement remain bounded by explicit deadlines.
- [Invariant 18](../architecture/invariants.md): the process gate uses the same authenticated
  production owners and cannot weaken lease or cancellation semantics for test convenience.

## Validation

The focused process test starts the production reducer child, observes activation and a later
renewal from the first coordinator, kills that coordinator, requires expiry before the five-second
fallback, and then requires a fresh coordinator to activate, renew, cancel, and exit successfully.
The complete standalone process suite also retains independent result retry/global publication and
abrupt reducer-loss/whole-attempt replacement coverage.

The warning-as-error Debug build and all 361 cluster tests pass, as do all 80 cluster
allocation-failure tests. The three-test process suite passes under the normal build, ASan/UBSan,
TSan, and a GCC 13 Ubuntu 24.04 container build; the new process-loss case also passes ten
consecutive normal repetitions. Formatting, workflow-action pin validation, and whitespace checks
pass. Pinned LLVM 18 static analysis reaches only the known macOS 26 libc++ builtin incompatibility
after its actionable local findings were corrected. Linux qualification keeps warnings enabled but
does not promote them to errors because the repository still has unrelated GCC warning debt.

## Migration or rollback considerations

No durable, network, or runtime migration exists. Rolling back removes process evidence only; it
does not change Job Control v4 behavior. Mixed pre-alpha binaries remain unqualified.

## Unresolved questions

- Qualify coordinator loss before lease activation and at every PREPARE/route-install boundary.
- Add multi-reducer packet-partition and asymmetric-loss campaigns.
- Specify durable query/job recovery, fencing, and restart semantics before resumption is allowed.

## References

- [Job Control v4](../formats/distributed-vector-grouped-aggregate-shuffle-job-control-v4.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
