# ADR 0466: Per-group replicated read-authority service

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB replicated service, Raft, cluster transport, and query maintainers
- **Extends:** [ADR 0299](0299-correlated-replicated-read-authority.md) and
  [ADR 0461](0461-authenticated-remote-raft-read-authority.md)

## Context

The remote receiver needs a production service that issues one barrier for the requested group.
`ReplicatedReadBarrier` acquired every configured group, which would turn one remote request into
unrelated quorum work. Its completion also depends on the Raft poll owner, so invoking it on that
poll thread would deadlock until timeout.

## Decision

Add `ReplicatedReadBarrier::await_group_authority`. It accepts only an exact configured group and
reuses the same current-term commit, barrier, group/term/context correlation, ordered leader
observation, finite deadline, one-active-waiter, and shutdown machinery as all-group acquisition.
Local mode submits only that group's three ordered operations. Transported mode creates one request
state for only that group while the existing poll-owner API drives its durable and quorum work.

`ReplicatedRaftReadAuthorityService` is the production synchronous adapter to
`RaftReadAuthorityService`. It borrows the barrier owner, requests one group, and transfers the
complete owning barrier/observation proof into the cluster protocol value. Unknown groups fail with
`NOT_FOUND`; concurrent calls retain the existing bounded `RESOURCE_EXHAUSTED` outcome.

The adapter must run on a non-poll thread. The Raft transport thread remains the sole caller of
`poll_owner_drive` and `poll_owner_observe`, so it can make progress while the authority listener
thread waits. The barrier owner must outlive the adapter and be shut down only after listener
admission and service calls stop.

## Consequences

A remote request now has a real runtime-backed authority source and does not perform unrelated group
work. Existing all-group Native callers and barrier-only callers retain their APIs and semantics.
The single-waiter limit intentionally applies across local Native and remote service requests; later
measurement may justify a bounded request queue, but no unbounded concurrency is introduced.

The daemon listener and Native outbound batch are still separate integration work. This slice makes
their required thread boundary explicit rather than hiding a synchronous wait in the Raft event
loop. No wire, consensus, or durable format changes.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): the service returns only the barrier and exact leader
  observation correlated by the existing durable/quorum path.
- [Invariant 6](../architecture/invariants.md): one request performs work only for its configured
  group and cannot receive a mixed all-group result.
- [Invariant 11](../architecture/invariants.md): the result owns its observation while runtime,
  adapter, poll-thread, and shutdown borrowing order is explicit.
- [Invariant 18](../architecture/invariants.md): the adapter reuses the same barrier algorithm and
  does not weaken publication coverage or consistency requirements.

## Validation

Local tests select one of two configured groups through both the direct API and production adapter,
verify exact group/leader proof, and reject null adapters and unknown groups. A transported loopback
test waits for one group on a separate thread while the sole poll owner drives its completion.
Before commit, all 106 normal service tests and all three service allocation-failure tests passed
with loopback socket permission. All four focused tests passed under ASan/UBSan with leak detection
disabled because Apple's sanitizer runtime does not support LeakSanitizer. Both changed production
sources passed repository-pinned clang-tidy 18; all changed C++ files passed clang-format 18; and
the diff passed whitespace review.

## Migration or rollback considerations

No stored or network bytes change. Rollback removes an unconsumed service adapter until daemon
integration lands. After that integration, stop and remove the authority listener before removing
this API so no accepted request can retain the borrowed barrier.

## Unresolved questions

- Whether measured contention requires a bounded FIFO of service requests instead of immediate
  `RESOURCE_EXHAUSTED` while another Native or remote barrier is active.
- How the shared private query endpoint selects the read-authority protocol after mutual-TLS
  authentication without violating authenticate-before-parse ordering.

## References

- [Correlated replicated read authority](0299-correlated-replicated-read-authority.md)
- [Authenticated remote Raft read authority](0461-authenticated-remote-raft-read-authority.md)
- [Linearizable Raft read barriers](../learning/linearizable-raft-read-barrier.md)
