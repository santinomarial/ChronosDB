# ADR 0552: Linux netfilter directional job-control partition qualification

- **Status:** accepted
- **Date:** 2026-08-30
- **Owners:** ChronosDB distributed-query, networking, reducer service, and test maintainers
- **Extends:** [ADR 0545](0545-authenticated-grouped-reducer-coordinator-leases.md),
  [ADR 0550](0550-partial-multi-reducer-coordinator-loss-qualification.md), and
  [ADR 0551](0551-authenticated-partial-job-control-frame-qualification.md)

## Context

Authenticated application-frame prefixes were deadline-bound, but application scheduling did not
control kernel packet delivery. The two-reducer process harness also paused processes at exact
control boundaries rather than keeping both peers live through a directional network black hole.
There was no evidence that a production coordinator would cancel the still-reachable reducer,
allow the isolated reducer's lease to expire, and use both services again after packet delivery was
restored.

The fault mechanism itself must not become an ambient developer-machine mutation. Packet rules
need exact scope, explicit opt-in, an isolated network namespace, privilege checks, bounded waits,
and verified removal.

## Decision

Add a distinct process-test executable that is compiled on POSIX platforms but runs its fault cases
only on Linux when `CHRONOS_RUN_PACKET_FAULT_TESTS=1` is explicitly set, the process is root, and a
caller-selected or canonical `/usr/sbin/iptables` can administer the current namespace. Normal
developer and CI discovery therefore skips the tests. Qualification runs in an ephemeral container
with only `NET_ADMIN` and `NET_RAW` added.

Each case starts the production process child as one coordinator and two reducer identities over
real loopback TCP and mutual TLS. It waits until both reducers acknowledge lease activation and one
renewal, then inserts one netfilter `OUTPUT` drop rule for only reducer 2's exact TCP port and
`127.0.0.1/32`:

1. `--dport` drops coordinator-to-reducer packets while leaving reducer 3 reachable; and
2. `--sport` drops reducer-to-coordinator packets while leaving requests able to reach the reducer
   side of the kernel path.

The rule remains installed until the partitioned coordinator terminates. In both directions,
reducer 2 must expire exactly one active lease, reducer 3 must receive authenticated CANCEL, and the
coordinator must fail rather than claiming another complete all-reducer lease round. The test then
deletes the exact rule, starts a distinct query identity, and requires both retained reducers to
activate, renew, and acknowledge cancellation. Per-reducer activation and cancel-request totals
prove the replacement used both services and did not inherit the abandoned identity.

One RAII rule owner attempts deletion on every return path, while the successful path requires
explicit deletion before healing is claimed. Qualification additionally compares the final
namespace `OUTPUT` rules with its original accept-only policy after repeated runs. The reducer child
prints changes to its existing cancellation metrics; no production service behavior or metric is
added.

## Detailed rationale

The reducer control protocol uses one finite TCP/mTLS connection per action. Installing the rule
after a completed lease renewal makes the next renewal encounter a silent kernel black hole without
racing an earlier acknowledged request. Port-scoping isolates one reducer while preserving a live
peer, so the coordinator's all-reducer failure and cancellation behavior is observable in the same
run.

Testing both port directions distinguishes inability to deliver a request from inability to receive
the reducer's TCP/TLS response path. Holding the rule through coordinator termination proves the
fresh lifecycle succeeds because of explicit healing, not because a failing coordinator retried
after an early rule removal.

## Alternatives considered

- **Pause or kill one reducer:** this qualifies process loss but not kernel-owned packet loss while
  both processes remain scheduled.
- **Use an application proxy:** a proxy can stall bytes but is another application owner and cannot
  prove the host network stack discarded the selected direction.
- **Disconnect an entire container network:** that is a useful bidirectional partition but does not
  distinguish request and response directions or preserve exact reducer-port scope.
- **Run automatically whenever root is available:** this risks mutating a shared host namespace and
  is rejected. Explicit opt-in and namespace isolation are part of the test contract.

## Consequences

ChronosDB now has OS-controlled evidence for a one-reducer directional Job Control black hole and
healing in both directions. The healthy peer is cancelled promptly, the isolated peer remains
bounded by its already-acknowledged relative lease, the coordinator cannot publish partial liveness,
and the healed pair accepts a fresh identity.

The gate requires Linux netfilter, root inside an isolated namespace, and a test-only `iptables`
tool. It does not add a production dependency. It does not qualify established-stream truncation,
partial TLS records, probabilistic loss, delay, duplication, reordering, bandwidth pressure,
multi-hop routing, or reducer sets larger than two. Those remain separate fault and scale campaigns.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): the abandoned and replacement identities remain
  exact across both reducers despite asymmetric reachability.
- [Invariant 11](../architecture/invariants.md): coordinator connection owners terminate before
  rule removal, and retained reducer services outlive the failed query.
- [Invariant 13](../architecture/invariants.md): a partial lease round cannot publish worker,
  reducer, or Native output.
- [Invariant 15](../architecture/invariants.md): connect, exchange, lease, cancellation, process,
  rule-administration, and replacement waits are finite.
- [Invariant 18](../architecture/invariants.md): the fault gate uses production TCP, mutual TLS,
  coordinator, and reducer owners without weakening their deadlines or all-reducer requirements.

## Validation plan

Both directional cases pass in three consecutive final Linux netfilter repetitions, and the OUTPUT
chain is clean afterward. The two cases also pass with the coordinator, reducers, and test owner
instrumented by ASan/UBSan and by TSan in privileged GCC 13 Ubuntu 24.04 containers. The existing
eight-test result-process suite passes after the added child observability. The unchanged cluster
and allocation-failure suites retain their previously qualified 364 and 80 passing tests.
Clang-format 18, workflow-action pinning, and whitespace checks pass. Pinned clang-tidy 18 reaches
only the known macOS 26 libc++ unsupported-builtin errors and reports no project-source diagnostic.

## Migration or rollback considerations

No durable or network migration exists. Rolling back removes the opt-in fault target, test-child
metric output, and evidence without changing production bytes or behavior. If a test is interrupted
outside its RAII path, discard its required ephemeral namespace rather than reusing it.

## Unresolved questions

- Qualify established TLS-record interruption and TCP reset at controlled byte boundaries.
- Add deterministic delay, duplication, reordering, and probabilistic loss campaigns.
- Qualify larger reducer sets, skew, CPU stalls, and deadline behavior under load.
- Specify durable query/job identity, fencing, and restart recovery before resumption is allowed.

## References

- [Authenticated grouped reducer coordinator leases](0545-authenticated-grouped-reducer-coordinator-leases.md)
- [Partial multi-reducer coordinator-loss qualification](0550-partial-multi-reducer-coordinator-loss-qualification.md)
- [Authenticated partial job-control frame qualification](0551-authenticated-partial-job-control-frame-qualification.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
