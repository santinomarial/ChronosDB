# ADR 0514: Lossless grouped shuffle destination execution

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB distributed-query and networking maintainers
- **Extends:** [ADR 0473](0473-bounded-all-tablet-grouped-state-coordinator.md),
  [ADR 0509](0509-bounded-grouped-shuffle-tcp-server.md),
  [ADR 0510](0510-idempotent-all-source-grouped-shuffle-partition-reducer.md),
  [ADR 0513](0513-bounded-grouped-shuffle-remote-edge-scheduling.md)

## Context

The bounded listener retained acknowledged streams and each partition reducer provided idempotent
all-source closure, but no node owner connected those boundaries. Extracting a listener result
before reducer admission creates a critical ownership transition: allocation may fail after the
source has already been acknowledged, and dropping the extracted value would silently lose query
input. Reducers also sealed duplicate admission at finish, so a receipt lost after destination
finalization could make an exact source retry fail forever.

## Decision

Add one move-only, single-thread-affine destination execution per participating node. Construction
derives every authority partition assigned to the local node, privately creates its reducer, and
starts one bounded TCP/mTLS listener only when at least one authority source is remote. A node with
no assigned partition is rejected. Reducers and the listener borrow the exact immutable authority;
the security dependencies and authority outlive the owner.

Local deliveries must be authority-valid self-routes. Each remote poll first retries an optional
pending stream, then drains at most the configured admission count from the listener's retained
FIFO. Extraction moves into the destination's optional slot before reducer admission. If admission
returns `RESOURCE_EXHAUSTED`, the slot remains populated and the next poll retries the same object;
no acknowledged stream returns to the listener or disappears. Permanent invalidity or retry
conflict fails the whole destination and closes transport.

Each reducer finalizes only after it has accepted every authority source. Finalization allocation
failure is retryable because canonical coordinator frames remain intact. The destination becomes
`READY` when all local reducers are finalized, and per-partition output is then pullable. For a
remote destination the listener remains live while ready so a source that lost its receipt can
reconnect. Exact retained coordinator messages and exact complete reducer streams therefore remain
idempotent after sealing and output completion; byte-different reuse still returns
`ALREADY_EXISTS`. An external coordinator calls `seal_transport` only after proving every source
receipt, changing `READY` to `COMPLETE`. Local-only destinations become complete immediately.

Transport admission, retained FIFO capacity, pending ownership, per-poll reducer admissions,
partition count, reducer state, message bytes, final merge memory, and output chunks retain their
existing finite bounds. Metrics expose local and remote deliveries, local and ready partitions,
pending remote ownership, nested reducer metrics, and listener metrics. One thread serializes the
owner, so no shared-memory ordering algorithm is introduced.

## Detailed rationale

The listener must acknowledge only a fully validated stream, but merging before that receipt would
couple network progress to allocation-heavy reducer work. A single reserved pending slot preserves
the established transport contract while closing the post-acknowledgment loss window. Keeping the
listener alive after reducer readiness is required because a completed server write does not prove
the peer consumed the receipt. The higher-level coordinator is the first component able to prove
all sender acknowledgments and safely seal ingress.

## Alternatives considered

- **Drop a stream when reducer admission exhausts memory.** Rejected because the source may already
  have accepted the receipt as success.
- **Acknowledge only after reducer merge.** Rejected because it couples bounded carrier deadlines
  to allocation-heavy finalization and duplicates reducer state inside the transport session.
- **Finish reducers and immediately close the listener.** Rejected because receipt loss after the
  server write would strand an exact retry.
- **Merge partitions in listener completion order.** Rejected because arrival order is not the
  authority's canonical source order.

## Consequences

Every destination node now owns a lossless path from local source fan-out and acknowledged remote
listener results through all-source reducer closure. Allocation failure cannot open an ownership
gap, late exact retries remain serviceable while output is read, and transport sealing has an
explicit source-receipt precondition. Query-wide partition-result gathering and the coordinator
that jointly drives every source and destination remain separate work.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): every local reducer and admitted stream remains
  bound to the exact immutable query/source/destination authority.
- [Invariant 9](../architecture/invariants.md): exact retries remain no-ops after reducer finish and
  output, while conflicting reuse fails closed.
- [Invariant 10](../architecture/invariants.md): only the authenticated listener or an authority
  self-route can reach reducer admission.
- [Invariant 11](../architecture/invariants.md): listener FIFO, pending slot, reducer, query
  reservation, authority borrowing, and teardown lifetimes are explicit.
- [Invariant 15](../architecture/invariants.md): listener, pending, admission work, reducers,
  retained bytes, final memory, and outputs are finite.
- [Invariant 18](../architecture/invariants.md): allocation retry and transport order do not change
  canonical hash routing, source closure, or authority-order merge.

## Validation plan

A real two-source mTLS case sends one local self-route and one remote retry into the same partition,
waits for reducer readiness, reads the merged row, then sends an exact remote retry after output
completion and proves a second authenticated receipt plus one reducer duplicate. A local-only case
proves transport is omitted and rejects an unassigned node. Coordinator and reducer tests prove
exact post-seal retry and conflicting-byte rejection. Allocation injection sweeps destination
construction and, at the acknowledged FIFO boundary, forces reducer admission failure, observes
one pending owned stream and a released listener slot, retries successfully, then waits for sender
receipt completion before sealing. Header self-containment, warning-as-error ASan/UBSan suites,
formatting, changed-source clang-tidy, and final diff review are required.

The warning-as-error ASan/UBSan build, all 293 cluster tests, all 52 cluster allocation-failure
tests, and all 427 query tests pass. Changed C++ files pass LLVM 18 formatting. The repository-wide
format check reaches one unchanged pre-existing grouped-query TLS header violation. Changed-source
clang-tidy found and removed const return locals that prevented automatic move, then reached only
the known LLVM 18/macOS 26 libc++ builtin incompatibility. Whitespace and scope review pass.

## Migration or rollback considerations

No durable or wire bytes change. Rollback removes the destination execution and post-seal exact
retry extension. An embedding must not extract acknowledged listener streams without an equivalent
retention slot, and must not close ingress before it can prove every sender received its receipt.

## Unresolved questions

- Gather disjoint partition outputs into one query-wide deterministic result owner.
- Jointly drive all source fan-outs, remote schedulers, destinations, cancellation, and deadlines.
- Add receipt-loss, node-loss, skew, and multi-process differential qualification.

## References

- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
