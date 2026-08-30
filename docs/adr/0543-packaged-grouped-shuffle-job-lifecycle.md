# ADR 0543: Packaged grouped shuffle job lifecycle

- **Status:** accepted
- **Date:** 2026-08-30
- **Owners:** ChronosDB distributed-query, cluster transport, service, and daemon maintainers
- **Extends:** [ADR 0517](0517-atomic-grouped-shuffle-source-fanout.md),
  [ADR 0518](0518-worker-to-shuffle-grouped-execution.md),
  [ADR 0519](0519-explicit-native-grouped-shuffle-selection.md), and
  [ADR 0542](0542-finite-grouped-reducer-job-coordinator.md)

## Context

The reducer-set coordinator could PREPARE and SEAL independent reducer processes, but it returned
listener endpoints only to the query coordinator. A coordinator cannot send a source tablet's
shuffle stream on that tablet node's behalf: the mutual-TLS receiver authorizes the authenticated
principal as the claimed source node. Starting workers before every source knew the complete route
set could also publish a prefix into only part of the reducer set. The Native service needed one
owner spanning reducer setup, true-source worker publication, seal, result return, and final output.

## Decision

Add Job Control v2 `INSTALL_ROUTES`. After all version 1 PREPARE requests succeed, the coordinator
broadcasts one canonical numeric route set to every reducer and waits for every exact-correlated
success. The route set contains each destination with at least one remote source. A reducer exact-
matches its own advertised listener and resolves every node through its immutable configured TLS
context map. No route or worker work becomes visible while this phase is partial.

The reducer service retains one query-accounted source submission per local authority tablet. A
submission is accepted only on its true source node after route installation. Exact byte-identical
retry is idempotent; conflicting reuse fails. Local edges enter local reducers directly. Remote
edges become immutable finite TCP/mTLS retries and are progressed asynchronously by the same
single-thread-affine job-service poll owner. All expected local tablets must publish before source
transport can become complete.

Wrap the production mutable grouped worker with a source-publishing decorator. Binding and worker
execution remain unchanged. After a successful complete worker result, the decorator offers its
canonical messages to the job service. Absence of a matching job is a deliberate no-op, preserving
the direct grouped lifecycle. Job publication failure replaces the worker success so no query can
mistake an unshuffled source for completion. The packaged shared query-control server owns the job
service and decorator at stable addresses and exposes that same decorated worker to both remote and
local Native dispatch.

Add one move-only whole-query owner with this order:

1. derive and retain exact shuffle/finalization authority;
2. PREPARE every reducer and install the complete route set;
3. activate the authenticated relative coordinator lease at every reducer;
4. start the existing finite local/remote grouped workers while renewing that lease;
5. require complete worker publication and discard their now-internal source payload copies;
6. SEAL every reducer only after source transport closes while lease renewal continues;
7. collect receipt-proven reduced partitions and atomically take the Native result.

Failure or cancellation tears down workers and enters the authenticated remote CANCEL lifecycle
added by [ADR 0544](0544-authenticated-grouped-reducer-job-cancellation.md). An unreachable remote
job still uses its relative deadline as the bounded fallback. Jobs and cancellation tombstones are
in-memory execution state, not acknowledged writes, and make no durability guarantee.
Post-route coordinator loss is bounded by the authenticated relative lease added by
[ADR 0545](0545-authenticated-grouped-reducer-coordinator-leases.md); pre-activation loss retains
the original deadline because no worker has started.

The Native deployment provider receives the exact worker routes resolved from the same committed
query snapshot. It selects reducer jobs only when the coordinator is a gateway with no local source
or destination. Existing control/result protocols reject self routes and require the authenticated
principal to equal the claimed node; a query containing a coordinator-local fragment therefore
selects the established direct grouped lifecycle explicitly. The daemon installs the provider from
its shared peer authority, TLS credentials, and a result listener bound to its advertised address.

## Consequences

The complete independent-process lifecycle is now reachable from packaged Native SQL in gateway
topologies. Route authority cannot become stale relative to the prepared query snapshot, source
streams originate at their authenticated nodes, and Native bytes remain unavailable until every
reducer result is acknowledged and globally finalized.

This is not a claim of durable query recovery or arbitrary relational shuffle. Packaged Linux
split-leader qualification, process-kill proof of lease expiry and fresh replacement, abrupt daemon
faults during each phase, skew/loss campaigns, and scale measurements remain required.
Coordinator-local queries use the direct sufficient-state merge path until a future protocol
explicitly supports self roles.

## Ownership, threading, and memory ordering

Each coordinator, TCP scheduler, reducer job, and packaged query-control server remains owned and
polled by one thread. The worker decorator borrows the stable job service owned immediately before
it and is destroyed first. The Native service borrows the stable provider, local worker, peer
authority, and TLS contexts from the daemon owner, which outlives synchronous query execution.
Source payload retention is charged to the job's query resource context; no per-row allocation is
introduced.

No shared-memory concurrency algorithm or new atomic publication edge is introduced, so there is
no new memory-ordering argument. Existing daemon request cancellation remains the release/acquire
publication documented by its owner.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): query, source, destination, route, result schema,
  and finalization authority remain one exact product from a committed snapshot.
- [Invariant 11](../architecture/invariants.md): stable service/decorator/provider ownership and
  reverse teardown preserve every borrowed lifetime.
- [Invariant 13](../architecture/invariants.md): partial PREPARE, route installation, source
  transport, SEAL, or result collection exposes no Native prefix.
- [Invariant 15](../architecture/invariants.md): jobs, routes, submissions, retained bytes,
  attempts, backoff, accepts, per-poll work, and total deadlines are bounded independently.
- [Invariant 18](../architecture/invariants.md): authenticated true-source publication and complete
  receipt-proven reducer coverage are required before output.

## Validation

Codec, transport, TLS, shared-dispatch, job-service, source-worker, coordinator, and whole-lifecycle
tests cover empty and multi-route frames, corruption, partial I/O, exact retry/conflict behavior,
route coverage, true-source publication, PREPARE-before-worker ordering, SEAL, result return,
take-once output, cancellation, and allocation failure. A replicated Native service fixture uses a
gateway coordinator, real committed mutable fragments, the shared mTLS endpoint, packaged decorated
worker, reducer job, and Native grouped SQL finalization. The complete three-daemon split-leader
gate remains outstanding and is not replaced by that in-process service fixture.

The warning-as-error normal build passed for the cluster, cluster allocation-failure, service,
service allocation-failure, daemon, feature-smoke, and standalone result-process targets. Their
complete normal suites passed 349 cluster, 78 cluster allocation-failure, 113 service, 7 service
allocation-failure, 1 feature-smoke, and 2 result-process tests. Focused ASan/UBSan passed 17
cluster, 11 cluster allocation-failure, 5 service, and 1 service allocation-failure tests with leak
detection disabled on Apple's runtime; focused TSan passed the 5 provider and replicated Native
cases. Clang-format 18, workflow-action pinning, and whitespace checks passed. Repository-pinned
clang-tidy 18 found and prompted removal of one const local that inhibited automatic move, then
stopped only in the installed macOS 26 libc++ headers on unsupported `__builtin_clzg`,
`__builtin_ctzg`, and `__builtin_popcountg` diagnostics; the rerun reported no project-source
diagnostic. This macOS host cannot build the existing Linux-only daemon-process target, so no
multi-daemon evidence is claimed.

## Migration or rollback considerations

PREPARE and SEAL v1 bytes are unchanged. Version 2 is additive and used only by binaries that
support the complete job lifecycle; mixed pre-alpha binaries are not qualified. Rolling back the
provider removes job selection and returns grouped queries to the existing direct lifecycle. A
partial rollback that leaves workers publishing to jobs without route installation is invalid.

## Unresolved questions

- Qualify v4 lease expiry and fresh replacement under packaged coordinator process loss.
- Support a coordinator that is also a source/destination without weakening principal-to-node
  authorization or constructing a network self route.
- Run packaged multi-daemon split-leader, partition/loss, skew, and measurement gates.

## References

- [Job Control v1](../formats/distributed-vector-grouped-aggregate-shuffle-job-control-v1.md)
- [Job Control v2](../formats/distributed-vector-grouped-aggregate-shuffle-job-control-v2.md)
- [Job Control v3](../formats/distributed-vector-grouped-aggregate-shuffle-job-control-v3.md)
- [Job Control v4](../formats/distributed-vector-grouped-aggregate-shuffle-job-control-v4.md)
- [Authenticated grouped reducer-job cancellation](0544-authenticated-grouped-reducer-job-cancellation.md)
- [Authenticated grouped reducer coordinator leases](0545-authenticated-grouped-reducer-coordinator-leases.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
