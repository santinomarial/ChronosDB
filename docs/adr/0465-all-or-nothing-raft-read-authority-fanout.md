# ADR 0465: All-or-nothing Raft read-authority fan-out

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB cluster transport, query, networking, and Raft maintainers
- **Extends:** [ADR 0464](0464-finite-multi-address-raft-read-authority-acquisition.md)

## Context

A split-leader query needs one fresh authority per required Raft group. Sequential acquisition can
waste the whole-query deadline, while exposing each completed child independently can let a caller
accidentally bind a partial or mixed-attempt vector. Failure or cancellation of one child must also
release every sibling descriptor promptly.

## Decision

`RaftReadAuthorityTcpBatchAcquisition` owns a nonempty canonical group-sorted vector of finite-route
acquisitions. The vector is capped by the distributed fragment limit. Group IDs and correlation IDs
are unique, and every request has the same nonzero source node. Each child retains its own immutable
target, route, TLS policy, deadlines, retries, and barrier correlation.

One caller thread serializes the batch. A drive pass gives every running child a nonblocking poll
before the batch waits, so all groups begin concurrently. The batch then polls at most one descriptor
per child and shortens its wait to the earliest child connect, TLS, exchange, or backoff deadline.
One child failure cancels every still-running sibling and makes the original failure sticky. Explicit
batch cancellation does the same with `CANCELLED`.

Completed child values remain private until every group succeeds. Only then does the owner publish
one canonical vector of exact barrier/observation pairs. The vector is copied out under bounded
allocation handling; no API exposes partial children or resets a failed attempt.

## Consequences

Wall-clock authority latency is bounded by concurrent child progress instead of the sum of group
latencies. Descriptor and poll storage are `O(group_count)`, bounded by the configured and distributed
plan limits. Per-child route and retry bounds remain unchanged.

This owner does not discover leaders, resolve committed routes, revalidate publications, or integrate
with the daemon. It supplies the all-or-nothing attempt boundary needed by that later composition. A
group authority still names a per-group linearization point, not one atomic cross-group instant.

The batch and every child are caller-thread-affine. Borrowed TLS contexts, authenticators, and
authorizers must outlive the batch and provide their own synchronization. No shared-memory
concurrency algorithm or memory-ordering edge is introduced.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): only complete canonical barrier/leader proofs can
  leave the batch; applied publication checks remain a later admission step.
- [Invariant 6](../architecture/invariants.md): a query attempt cannot observe a partially acquired
  authority vector, and group/source/correlation ambiguity fails before opening sockets.
- [Invariant 10](../architecture/invariants.md) and
  [Invariant 11](../architecture/invariants.md): group count, descriptors, poll storage, child retry,
  result ownership, and cancellation teardown are bounded and explicit.
- [Invariant 18](../architecture/invariants.md): concurrent acquisition changes latency only; it
  neither weakens linearizable authority nor claims a global cross-group transaction snapshot.

## Validation

A real two-server mutual-TLS test acquires two different groups from two different leader nodes,
proves both services run exactly once, and observes no result until the canonical vector is complete.
A failure case rejects duplicate group/correlation ambiguity, holds one sibling active while another
route fails, and proves whole-attempt failure has no partial result or active child. Explicit
cancellation proves both active siblings are torn down and cancellation remains sticky. Broader
suite, sanitizer, format, and static-analysis evidence is recorded with the implementing commit.
Before commit, all 220 normal cluster tests and all 28 cluster allocation-failure tests passed with
loopback socket permission. All 14 focused authority tests passed under ASan/UBSan with leak
detection disabled because Apple's sanitizer runtime does not support LeakSanitizer. The new
production source passed repository-pinned clang-tidy 18; all changed C++ files passed clang-format
18; and the diff passed whitespace review.

## Migration or rollback considerations

No wire, consensus, or durable format changes. The batch is an in-memory query-attempt owner and can
be removed without migration until daemon integration consumes it. After integration, roll back the
complete authority fan-out and query-admission composition together.

## Unresolved questions

- Define the daemon-owned service that serializes barrier issuance through the local durable Raft
  runtime and the committed route/TLS resolver used to build each batch.
- Decide whether a later query-attempt policy refreshes all leader routes after one authority failure
  or returns a retryable terminal error to the existing whole-query rebind owner.

## References

- [Finite multi-address Raft read-authority acquisition](0464-finite-multi-address-raft-read-authority-acquisition.md)
- [Raft Read Authority Transport v1](../formats/raft-read-authority-transport-v1.md)
- [Linearizable Raft read barriers](../learning/linearizable-raft-read-barrier.md)
