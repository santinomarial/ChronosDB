# ADR 0507: Finite immutable-route grouped shuffle retry

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB cluster and distributed-query maintainers
- **Extends:** [ADR 0504](0504-atomic-authorized-grouped-shuffle-streams.md),
  [ADR 0506](0506-bounded-grouped-shuffle-mutual-tls-session.md)

## Context

One connected grouped-shuffle TLS session owns exactly one stream sender and consumes it as bytes
are written. A failed connection therefore cannot reuse that sender. Repartitioning or resolving a
new destination during retry would make a later attempt differ from the query's immutable authority,
while retaining a partly consumed outer-frame cursor would make the retry depend on failure offset.
An acknowledgment lost after destination acceptance also requires the source to resend exactly the
same stream until destination admission becomes idempotent.

## Decision

Add a move-only, single-thread-affine retry owner for one nonlocal authority edge. It borrows the
complete shuffle authority and owns the exact canonical nested message vector, shared query resource
context, stream limits, attempt budget, and capped exponential-backoff state. Construction creates
and discards one complete stream sender before publication, so invalid edge, sequence, terminal,
decode, frame, byte, or resource state cannot survive into the retry policy.

Each `begin_attempt` creates a fresh all-or-none stream sender from the unchanged nested bytes and
returns the monotonically numbered attempt plus the authority-bound target node. Construction
failure does not consume the attempt budget or change state. Only one attempt may be active. The
caller reports acknowledgment only after the attempt TLS owner has validated `CHDVGAK1`; local
write completion is not success.

`UNAVAILABLE`, `IO_ERROR`, and `RESOURCE_EXHAUSTED` failures retry after positive exponential
backoff capped by caller policy. All other failures are terminal. Attempts are capped at 1,024,
time addition saturates, and the last status plus next-attempt deadline remain inspectable. Retry
never mutates the edge, accepts a leader hint, rotates a node, or reacquires catalog authority.

The owner has no socket or clock and introduces no shared concurrent algorithm or memory-ordering
obligation. TCP acquisition, address candidates, listener ownership, cancellation, duplicate
destination admission, reducer installation, and packaged scheduling remain separate.

## Detailed rationale

Retaining canonical nested bytes rather than already framed cursors makes every attempt independent
of the prior partial-write offset. The deterministic outer codec and immutable edge then produce
byte-identical attempts. Separating policy from a connected session preserves the existing rule
that each descriptor/TLS owner has one finite lifecycle and lets higher scheduling layers decide
when readiness or deadline failures are recorded.

## Alternatives considered

- **Resume a failed frame at its last write offset.** Rejected because a new connection cannot know
  which TLS records or application bytes reached the peer.
- **Repartition on each retry.** Rejected because catalog or hashing drift would violate the fixed
  query authority and duplicate-admission identity.
- **Treat final stream write as success.** Rejected because only the exact reverse receipt proves
  destination application acceptance.
- **Move TCP and address rotation into the retry owner.** Deferred so immutable data policy remains
  independently testable and one-attempt transport ownership stays explicit.

## Consequences

Sources can now reconstruct finite byte-identical attempts for one remote source-partition edge.
Memory retains the canonical nested stream and constructs one complete framed stream for the active
attempt. A lost acknowledgment can still produce a duplicate accepted stream; the future reducer
admission owner must detect that exact edge/extent before merging.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): query, source, partition, target, hash version, and
  stream bytes stay immutable across attempts.
- [Invariant 10](../architecture/invariants.md): each attempt is reconstructed only through the
  existing exact nested and outer codecs.
- [Invariant 11](../architecture/invariants.md): retained messages, active cursors, query resources,
  and borrowed authority have explicit lifetimes.
- [Invariant 15](../architecture/invariants.md): attempt count, backoff, stream frames, bytes, and
  decode resources are independently bounded.
- [Invariant 18](../architecture/invariants.md): retry cannot change canonical hash routing,
  terminal closure, or source-order semantics.

## Validation plan

Focused tests prove byte-identical multi-frame reconstruction, immutable target identity, one active
attempt, exact backoff boundaries, capped exponential growth, finite exhaustion, receipt-only
success, permanent authentication failure, and invalid limits. Allocation injection covers policy
prevalidation and attempt reconstruction while proving failure consumes no attempt or query credit.
The warning-as-error ASan/UBSan build passes; all 280 cluster tests and 45 cluster allocation-failure
tests pass. Changed-source clang-tidy reaches only the known LLVM 18/macOS 26 libc++ builtin
incompatibility and emits no ChronosDB-source finding.

## Migration or rollback considerations

No durable or wire bytes change. Rollback removes the policy owner while retaining single-attempt
stream, receipt, and TLS owners; embeddings must then fail an edge after one attempt rather than
retrying from partial state.

## Unresolved questions

- Add deadline-bound nonblocking TCP connection and bounded listener ownership.
- Make destination reducer admission idempotent across lost receipts and retries.
- Compose all source/partition attempts under one query deadline and cancellation boundary.

## References

- [Distributed Vector Grouped Aggregate Shuffle Frame v1](../formats/distributed-vector-grouped-aggregate-shuffle-frame-v1.md)
- [Distributed Vector Grouped Aggregate Shuffle Acknowledgment v1](../formats/distributed-vector-grouped-aggregate-shuffle-ack-v1.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
