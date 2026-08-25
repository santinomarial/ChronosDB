# ADR 0510: Idempotent all-source grouped shuffle partition reducer

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB cluster and distributed-query maintainers
- **Extends:** [ADR 0473](0473-bounded-all-tablet-grouped-state-coordination.md),
  [ADR 0502](0502-complete-node-bound-grouped-shuffle-authority.md),
  [ADR 0509](0509-bounded-grouped-shuffle-tcp-server.md)

## Context

The bounded grouped-shuffle listener can retain one acknowledged complete stream per admitted
connection, but it does not decide whether a stream is a first delivery, an exact retry, or a
conflict. A destination partition needs one authority-bound owner that accepts every source tablet
exactly once, tolerates a lost receipt followed by exact retransmission, withholds output until all
sources close, and merges in canonical source order. Admission can allocate while canonical nested
messages enter the existing query-accounted coordinator, so allocation failure must not turn an
acknowledged stream into silent loss.

## Decision

Add one move-only, single-thread-affine reducer per local shuffle partition. Construction verifies
that the complete shuffle authority assigns the partition to the local node, builds an exact source
tablet index, and creates the existing bounded grouped coordinator in authority source order. The
authority is borrowed and must outlive the reducer.

`accept_stream` revalidates the immutable edge, local destination, source identity, complete
empty-or-contiguous terminal sequence, nested canonical encoding, encoded outer extent, and
`hash-v1 % partition_count` route for every nonempty key. The first delivery reserves its complete
outer-byte extent against per-source and reducer-wide limits before coordinator admission. It then
records an accepted message prefix as each nested message is retained. If allocation fails, the
caller still owns the complete stream and may retry it; the coordinator exact-compares the retained
prefix and admission continues at the first unretained message.

An exact complete-stream retry is a successful no-op and increments a duplicate metric. A retry
with a different extent, message count, or canonical message bytes returns `ALREADY_EXISTS`.
Completion is counted once per authority source. `finish` returns `UNAVAILABLE` until every source
has supplied its terminal, then delegates the deterministic authority-order merge to the shared
coordinator. No result is pullable before successful finish.

Metrics expose accepted sources, exact duplicate streams, and reserved outer stream bytes. The
reducer owns no socket and no durable state. The listener retains acknowledged streams until the
caller extracts them, and the caller must retain a stream across a retryable reducer allocation
failure. Process crash recovery and cross-process durable deduplication are not claimed.

## Detailed rationale

The outer stream identity is the natural retry boundary because the sender already reconstructs
byte-identical whole attempts and the receipt acknowledges that exact edge and extent. Prefix
tracking makes allocation failure retryable without copying the complete transport object into a
second owner. Reusing the shared coordinator preserves exact-message conflict detection, bounded
query accounting, canonical source-order merge, and physical output behavior instead of creating a
second aggregation implementation.

## Alternatives considered

- **Treat every reconnect as a new fragment.** Rejected because a lost receipt could duplicate
  aggregate state.
- **Deduplicate only by source tablet and byte count.** Rejected because equal extents can carry
  conflicting canonical messages.
- **Merge directly in listener completion order.** Rejected because network order is nondeterministic
  and cannot define grouped first-seen order.
- **Persist reducer state in this milestone.** Deferred because no durable distributed-query result
  contract or recovery protocol exists yet.

## Consequences

A local partition now has a finite, idempotent all-source closure and deterministic grouped-state
merge. Lost-receipt retransmission cannot duplicate aggregate input, conflicting retransmission
fails closed, and missing sources cannot expose partial output. Packaged all-edge scheduling,
catalog-derived destination choice, cancellation, and query-level multi-partition output ownership
remain separate work.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): every accepted source remains bound to the exact
  immutable authority edge and query identity.
- [Invariant 9](../architecture/invariants.md): an exact protected stream retry adds no grouped
  input, while conflicting reuse returns a deterministic error.
- [Invariant 10](../architecture/invariants.md): untrusted complete-stream objects are fully
  revalidated before allocation-driving admission or merge.
- [Invariant 11](../architecture/invariants.md): authority borrowing, stream ownership, coordinator
  retention, and single-thread affinity are explicit.
- [Invariant 15](../architecture/invariants.md): source, total outer, nested, message, and output
  resources remain bounded.
- [Invariant 18](../architecture/invariants.md): canonical hash routing, exact key equality, source
  closure, and deterministic source-order merge are preserved.

## Validation plan

Focused tests deliver sources in reverse order, retransmit an exact complete stream, reject a
conflicting retry, wrong route, noncanonical extent, invalid destination and invalid limits, refuse
partial finish, merge equal keys once in authority order, and pull the terminal physical output.
Allocation injection sweeps construction and stream admission, observes a partially retained
prefix, retries the caller-owned stream, and proves one accepted source. Header self-containment,
the warning-as-error ASan/UBSan build, all 286 cluster tests, and all 48 cluster allocation-failure
tests pass. Changed C++ files pass LLVM 18 formatting. The repository-wide formatting check reaches
one unchanged pre-existing grouped-query TLS header violation. Changed-source clang-tidy reaches
only the known LLVM 18/macOS 26 libc++ builtin incompatibility without a ChronosDB-source finding.

## Migration or rollback considerations

No durable or wire format changes. Rollback removes the partition reducer while retaining complete
acknowledged streams at the TCP listener boundary; no production path may claim shuffle completion
without an equivalent idempotent all-source owner.

## Unresolved questions

- Compose every local reducer with outbound source partitioning and bounded retry/TCP scheduling.
- Derive destination partitions from committed catalog placement rather than caller assembly.
- Define cancellation, process-crash behavior, and query-level multi-partition result ordering.

## References

- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
