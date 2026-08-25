# ADR 0468: Packaged shared query-control service

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB service, daemon, query, Raft, networking, and security maintainers
- **Extends:** [ADR 0445](0445-committed-daemon-mutable-query-plane.md),
  [ADR 0466](0466-per-group-replicated-read-authority-service.md), and
  [ADR 0467](0467-authenticated-shared-query-control-endpoint.md)

## Context

The shared authenticated carrier is useful only after the packaged service owns both production
receivers and the daemon replaces its mutable-only listener. The read-authority adapter blocks
synchronously while the replicated barrier waits, so its listener cannot run on the Raft poll
thread that must drive durable and quorum completion.

## Decision

`ReplicatedDistributedMutableQueryControlTcpServer` owns, in destruction-safe dependency order, the
request-local replicated mutable worker, one per-group `ReplicatedRaftReadAuthorityService`, the
mutable and authority receivers, and the shared bounded TCP/mTLS listener. It borrows the database
context provider, replicated read barrier, peer authenticator, node authorizer, and optional leader
hint provider; every borrowed owner must outlive it.

`chronosd` now constructs this owner on the one private data endpoint committed for its local node.
It passes the already-owned replicated read barrier and reuses the same immutable peer certificate
authority and TLS credential bundle as the Raft and distributed-query planes. The existing joined
query-control thread polls the shared listener; the distinct Raft transport thread continues to
drive transported barrier operations. Shutdown stops and joins the query-control thread and closes
the listener before releasing the barrier, authentication authority, database, or Raft transport.

Existing mutable and read-authority clients and their frozen bytes are unchanged. This milestone
publishes the authority service endpoint but does not yet make the Native coordinator acquire
remote group authorities; split-leader query completion therefore remains subsequent work.

## Consequences

Every packaged multi-peer daemon can now serve one authenticated remote read-authority request and
mutable fragments on the same committed endpoint without port derivation. The owner prevents
dangling receiver dependencies and the thread split prevents a synchronous authority wait from
starving its own Raft progress.

One query-control thread still serializes admitted receiver calls. Its connection and polling bounds
remain those of ADR 0467; parallel listener execution requires measured justification and an
explicit synchronization design.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): the service returns authority only through the
  durable current-term quorum barrier owner.
- [Invariant 6](../architecture/invariants.md): daemon publication exposes only complete correlated
  mutable streams or one complete authority response.
- [Invariant 11](../architecture/invariants.md) and
  [Invariant 15](../architecture/invariants.md): owner order, borrowed lifetimes, listener admission,
  threads, deadlines, buffers, and shutdown are explicit and bounded.
- [Invariant 18](../architecture/invariants.md): daemon composition reuses the authenticated peer
  authority and does not weaken source-node or target-node authorization.

## Validation

The focused production-owner test creates a real one-voter durable Raft runtime, elects its leader,
and acquires the exact group authority with the unchanged TCP/mTLS client through the shared service
owner; the mutable worker is not invoked. The complete normal service suite passed 107 of 107 tests,
and the complete service allocation-failure suite passed 4 of 4 tests, including exhaustive shared
owner construction failure classification and unwind. The focused owner test passed under
ASan/UBSan with leak detection disabled because Apple's sanitizer runtime does not support
LeakSanitizer. The warning-as-error service and daemon builds and clang-format 18 passed. The
Linux-only three-daemon gate cannot be rebuilt on this macOS host. Repository-pinned clang-tidy 18
could not parse the installed macOS 26 libc++ headers because that older analyzer lacks the SDK's
new generic bit-count builtins; this is recorded as unavailable rather than a passing check.

## Migration or rollback considerations

No wire or durable migration. Rollback can restore the mutable-only packaged listener while Native
does not depend on remote authority acquisition. Once Native consumes the authority endpoint, a
rollback must disable that dependency first.

## Unresolved questions

- How the Native coordinator discovers and retries independently led groups under one absolute
  query deadline.
- Whether measured query-control contention warrants bounded listener sharding.

## References

- [Raft Read Authority Transport v1](../formats/raft-read-authority-transport-v1.md)
- [Packaged Native Daemon](../learning/packaged-native-daemon.md)
- [Implementation Roadmap](../roadmap.md)
