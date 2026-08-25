# ADR 0496: Portable mutable grouped sufficient-state execution

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB query and cluster maintainers
- **Extends:** [ADR 0476](0476-portable-pinned-grouped-sufficient-state-execution-owner.md),
  [ADR 0490](0490-proof-revalidated-mutable-grouped-sufficient-state-worker.md)

## Context

The portable grouped execution owner pinned an immutable Manifest generation. Mutable grouped
requests instead name exact committed/applied TabletState publications and intentionally have no
Manifest generation. Reusing the immutable owner would conflate snapshot identities; leaving
coordination to the TCP scheduler would duplicate sender, retry, decode, and atomic publication
policy.

## Decision

Add a distinct single-thread-affine portable owner for one value-owned mutable fragment per tablet.
Creation validates one complete logical identity, grouped plan/result widths, exact key/state
authority against every fragment, unique tablets, and finite sender, decode-memory, and coordinator
limits. It owns one finite mutable grouped sender per tablet, a shared query-accounted decode
context, complete key/aggregate authority, plan/result schema, target routing identities, and the
all-tablet grouped coordinator.

Callers begin whole attempts, deliver only complete canonical response vectors, or record transport
failure by tablet. Successful sender frames are re-decoded under the retained authority before
entering the coordinator. Terminal sender/decode/coordinator failure is sticky. `finish` requires
every sender to have succeeded and delivered its complete terminal stream; only then does it seal
the coordinator and enable merged physical output. No transport, polling, Native encoding, or
authority rebinding is included.

## Consequences

Mutable heads now have the same finite all-tablet sufficient-state coordination boundary as
Manifest/CSEG snapshots without inventing a generation. Memory is bounded by fragment/sender
configuration, shared decode accounting, retained canonical coordinator bytes, and grouped table
limits. One thread serializes calls, so no synchronization or inter-thread memory-ordering
algorithm is introduced.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): exact `CHDMREQ1` and `CHDVGRP2` bytes remain
  versioned and are not reinterpreted as Fragment-v2.
- [Invariant 6](../architecture/invariants.md): senders, attempts, frames, bytes, decode memory,
  retained coordinator state, and grouped table memory are finite.
- [Invariant 10](../architecture/invariants.md): all tablets share one exact key/state authority.
- [Invariant 11](../architecture/invariants.md): every sender retains its value-owned mutable
  publication proof through all attempts.
- [Invariant 14](../architecture/invariants.md): query/tablet/source/target correlation is rechecked
  before coordinator delivery.
- [Invariant 18](../architecture/invariants.md): ownership and single-thread affinity are explicit.

## Validation

A two-tablet case owns two exact senders, accepts count-one and count-two state streams for the same
key, withholds output after the first tablet, and emits one merged group only after global closure.
A second case rejects mixed query authority and proves a terminal one-attempt transport failure is
sticky. Deterministic allocation injection walks logical identity, sender, authority, shared
resources, and coordinator construction until success.

The complete cluster and cluster-allocation suites pass 261 and 37 tests. The focused cases and
allocation sweep pass under ASan/UBSan with leak detection disabled. Changed-file LLVM 18
formatting, the warning-as-error build, and whitespace validation pass. Static analysis found and
corrected three automatic-move issues; no project-local finding remains before the known LLVM 18/
macOS 26 libc++ compiler incompatibility. The repository-wide format check retains only the pre-
existing violation in the unchanged grouped TLS v2 header self-containment test.

## Migration and rollback

This is additive and changes no durable or wire bytes. Rollback removes the portable mutable
grouped owner while retaining the worker and transport pieces; callers must not substitute the
Manifest-pinned execution owner.

## Unresolved questions

- Atomic Native grouped finalization and scheduler publication.
- Partitioned shuffle/skew policy and computed pre-group programs.

**Retrospective (2026-08-25):** [ADR 0497](0497-bounded-mutable-grouped-sufficient-state-tcp-scheduling.md)
adds the bounded all-tablet TCP poll owner, finite address rotation, deadline arbitration,
cancellation, rebinding, and transport lifecycle metrics without changing the portable owner's
transport-free contract.

## References

- [Portable pinned grouped sufficient-state execution owner](0476-portable-pinned-grouped-sufficient-state-execution-owner.md)
- [Proof-revalidated mutable grouped sufficient-state worker](0490-proof-revalidated-mutable-grouped-sufficient-state-worker.md)
- [Distinct mutable grouped sufficient-state transport](0491-distinct-mutable-grouped-sufficient-state-transport.md)
