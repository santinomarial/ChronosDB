# ADR 0171: Fail-closed distributed query execution owner

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB query, cluster, and distributed-systems maintainers
- **Extends:** [ADR 0169](0169-bounded-distributed-query-carrier-lifecycle.md), [ADR 0170](0170-compatible-multi-tablet-manifest-snapshot-binding.md)

## Context

The compatible snapshot, per-tablet sender, and aggregate coordinator were independently usable,
but the embedding still had to correlate them correctly. It could deliver a result twice, mark a
retryable backoff as a worker failure, mismatch admission order, release the Manifest pin early, or
return a partial aggregate after only some tablets completed.

## Decision

`DistributedQueryExecution` is a move-only, single-owner orchestration object for one distributed
aggregate. Creation exact-matches the plan-ordered compatible dispatches, admissions, query/read
policy, database generation, serving nodes, positions, and barriers before constructing one sender
per tablet and the existing coordinator. It retains the compatible snapshot for its full lifetime.

Tablet lookup uses a bounded ordered index. `begin_attempt`, `accept_response`, and
`record_transport_failure` delegate to exactly one sender. A successful sender's terminal exchange
is delivered to the coordinator once. Backoff and other nonterminal sender states do not mutate the
coordinator. Only a terminal/exhausted sender is reported through `worker_failed`, preserving its
status code. `finish` delegates to the coordinator, so missing tablets and any terminal worker
failure remain fail-closed.

The owner has no socket, thread, timer, or background callback. Its caller serializes methods,
supplies monotonic time, drives the bounded write/read owners, and decides whether an advisory
leader hint warrants explicit authority rebinding into a new execution.

ADR 0178 subsequently adds the single-owner multi-tablet TCP scheduler. It retains this execution,
uses the sender's exposed retry deadline, and preserves explicit authority rebinding.

## Consequences and validation

Creation is `O(fragments log fragments + total dispatch size)` and retains one compatible Manifest
owner, one bounded sender/dispatch per fragment, the coordinator's bounded state, and an ordered
tablet index. Per-event lookup is `O(log fragments)`. No concurrency memory-order argument is
needed because the object is explicitly single-owner and unsynchronized.

Tests construct two Raft-tablet dispatches from one pinned Manifest generation. They prove an
incomplete execution cannot finish, each terminal response is accepted once, the final Welford
states merge exactly, an unknown tablet and a duplicate response reject, retry backoff does not
poison the coordinator, exhausted transport failure becomes the query failure, and reordered
admissions cannot be paired with pinned dispatches.

An end-to-end packaged follower allocation sweep selects every main-thread allocation from scalar
TLS response decode through sender acceptance, coordinator merge, `finish`, and result publication.
Every selected failure is sticky `RESOURCE_EXHAUSTED`, leaves no active attempt or public aggregate,
and restores the exact Manifest pin; the no-fault boundary publishes the exact count and sum.

Socket/TLS readiness, deadlines, cancellation propagation, leader rebinding, general vector
fragments, and multi-node fault simulation remain separate work.

Invariants 4, 5, 6, 10, 11, 14, 15, and 18 apply.

## Migration and rollback

This adds an in-memory orchestration API and changes no durable or wire format. Embeddings may keep
equivalent explicit ownership, but must retain the compatible snapshot, correlate every tablet,
distinguish retry from terminal failure, and never return the coordinator result early.

## References

- [Bounded distributed query carrier lifecycle](0169-bounded-distributed-query-carrier-lifecycle.md)
- [Compatible multi-tablet Manifest snapshot binding](0170-compatible-multi-tablet-manifest-snapshot-binding.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Pinned multi-tablet TCP query scheduling](0178-pinned-multi-tablet-tcp-query-scheduling.md)
