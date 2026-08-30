# ADR 0550: Partial multi-reducer coordinator-loss qualification

- **Status:** accepted
- **Date:** 2026-08-30
- **Owners:** ChronosDB distributed-query, cluster transport, reducer service, and test maintainers
- **Extends:** [ADR 0542](0542-finite-grouped-reducer-job-coordinator.md),
  [ADR 0545](0545-authenticated-grouped-reducer-coordinator-leases.md), and
  [ADR 0549](0549-pre-activation-coordinator-process-loss-qualification.md)

## Context

The production reducer-set coordinator requires every reducer to acknowledge PREPARE, route
installation, and lease activation before advancing the whole query. Existing independent-process
gates qualified coordinator loss at each lifecycle boundary with one reducer. They did not prove
cleanup when two reducers had observed different prefixes of the same all-reducer phase. Such
partial progress is the important failure shape behind the coordinator's all-reducer gates: each
reducer must retain only its own admitted state and must select cleanup from its own exact phase.

## Decision

Extend the standalone production-owner process harness to run two reducer children with distinct
node identities, authenticated routes, listeners, and retained job services. The real coordinator
addresses both reducers. A reducer can stop itself with `SIGSTOP` before accepting control, after
acknowledged PREPARE, after acknowledged route installation, or after acknowledged lease
activation. The parent confirms the stop with `waitpid(..., WUNTRACED)`, kills the coordinator with
`SIGKILL`, and resumes both reducers. Each resumed child closes its suspended control endpoint
before another poll and restarts the endpoint over the same live production job service, so queued
later controls cannot move the observed boundary.

Qualify three asymmetric cases:

1. Reducer 2 acknowledges PREPARE while reducer 3 accepts no control. Only reducer 2 admits the
   abandoned identity and expires it at the PREPARE-relative execution deadline.
2. Both reducers acknowledge PREPARE, reducer 2 acknowledges route installation, and reducer 3
   stops after PREPARE. Both admitted identities expire at their execution deadlines because no
   lease was activated.
3. Both reducers acknowledge route installation, reducer 2 acknowledges lease activation, and
   reducer 3 stops before lease control. Reducer 2 expires by its relative coordinator lease while
   reducer 3 expires by its original execution deadline.

After cleanup, a distinct query identity must PREPARE, install routes, activate and renew leases on
both reducers, and complete authenticated cancellation. Exact metric totals distinguish lease from
execution expiry and prove the replacement did not inherit the abandoned identity.

## Consequences

The all-reducer coordinator gates now have independent-process evidence for the most important
partial-acknowledgement prefixes. An admitted reducer does not depend on its peers to clean up, and
reducers at different phases choose their own bounded cleanup authority. A reducer that accepted no
control has no abandoned job to expire. The same retained services remain usable together after
asymmetric cleanup.

This is test-harness qualification only. It changes no production behavior, durable format, wire
protocol, or acknowledged-write guarantee. Closing suspended endpoints makes phase boundaries
deterministic; it does not model partial TLS records, packet-level loss, or a restarted reducer.
Two reducers are sufficient to express the asymmetric prefixes but do not qualify larger-set skew.
Durable query recovery and fencing remain unspecified.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): both reducers exact-correlate abandoned and
  replacement query, coordinator, node, route, and authenticated-principal identities.
- [Invariant 11](../architecture/invariants.md): endpoint replacement discards suspended
  connections while the retained job service and its borrowed dependencies remain alive.
- [Invariant 13](../architecture/invariants.md): partial coordinator progress cannot expose source,
  reducer, or Native output before every reducer passes the activation gate.
- [Invariant 15](../architecture/invariants.md): process stops, coordinator loss, endpoint restart,
  execution deadlines, leases, retries, and replacement queries are explicitly bounded.
- [Invariant 18](../architecture/invariants.md): the gate composes production coordinator, service,
  mTLS, and protocol owners without a test-only cleanup path.

## Validation

Three standalone process tests cover partial PREPARE, partial route acknowledgement, and partial
lease acknowledgement. The complete eight-test result-process suite passes normally. The three new
cases pass in ten consecutive focused repetitions. The complete suite also passes under focused
ASan/UBSan, TSan, and GCC 13 Ubuntu 24.04 qualification. The warning-as-error native build,
clang-format 18, workflow-action pinning, and whitespace checks pass. Pinned clang-tidy 18 reaches
only the known macOS 26 libc++ unsupported-builtin errors and reports no project-source diagnostic.

## Migration or rollback considerations

No migration exists because only the process harness and documentation change. Rolling back removes
partial-acknowledgement evidence without changing the established coordinator or reducer semantics.

## Unresolved questions

- Qualify partial frames and asymmetric packet-level partitions without replacing production
  transport owners.
- Specify durable query/job identity, fencing, and restart recovery before resumption is allowed.
- Measure cleanup and replacement under CPU stalls, loss, and larger reducer sets.

## References

- [Job Control v1](../formats/distributed-vector-grouped-aggregate-shuffle-job-control-v1.md)
- [Job Control v2](../formats/distributed-vector-grouped-aggregate-shuffle-job-control-v2.md)
- [Job Control v4](../formats/distributed-vector-grouped-aggregate-shuffle-job-control-v4.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
