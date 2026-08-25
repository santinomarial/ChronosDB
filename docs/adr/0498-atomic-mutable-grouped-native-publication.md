# ADR 0498: Atomic mutable grouped Native publication

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB query, cluster, networking, and protocol maintainers
- **Extends:** [ADR 0484](0484-bounded-grouped-sufficient-state-native-finalization.md),
  [ADR 0485](0485-atomic-grouped-native-tcp-publication.md), and
  [ADR 0497](0497-bounded-mutable-grouped-sufficient-state-tcp-scheduling.md)

## Context

The mutable grouped TCP scheduler closed and merged every tablet but exposed pull-based physical
chunks. An embedding could omit the declared final projection/order/limit, choose output bounds
after network acquisition, or publish a prefix before Native encoding failed. The existing grouped
finalizer already proved those semantics for Manifest-pinned execution, but its input validation was
hard-wired to a compatible Manifest snapshot.

## Decision

The grouped finalizer now has a narrow internal authority adapter. Manifest-backed input supplies
its pinned dispatch plan, result schema, and projected input width. Mutable input supplies the same
logical fields from its exact cross-tablet identity without fabricating a Manifest generation.
Both inputs then use one implementation for key/aggregate shape revalidation, shared
`QueryResourceContext` ownership, optional checked final projection, global sort and limit, bounded
Native Protocol v1 encoding, and empty-result schema publication.

The mutable TCP scheduler owns finalization limits and an optional coordinator projection. Creation
validates output rows/batches/bytes, Native payload/schema/name limits, sort memory/width, raw or
projected result authority, and final ordering before opening any descriptor. Rebinding must retain
byte-equivalent logical projection and every finalization limit in addition to the prior logical
query checks.

After all senders succeed, the scheduler seals the portable coordinator and synchronously drains it
through the shared finalizer. It enters `kComplete` only after retaining the complete Native result.
`result()` is absent while running and after cancellation, transport/coordinator/finalization
failure, or one-time `take_result()`. No public physical-row pull remains on the scheduler.

## Consequences

Mutable grouped sufficient-state execution now has the same atomic public result boundary as the
Manifest-pinned grouped scheduler. Final projection, order, limit, query-memory accounting, and
Native encoding cannot be skipped by callers. A finalization failure is whole-query terminal
because physical output may have been consumed; no encoded prefix is retained.

The authority adapter shares policy without erasing the distinction between Manifest generation
and mutable applied-publication proof. One thread serializes execution and finalization, so no
inter-thread synchronization or memory-ordering algorithm is introduced. Durable and wire formats
are unchanged.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): only state already admitted by exact mutable
  publication proofs can reach final output.
- [Invariant 6](../architecture/invariants.md): finalization rechecks the same cross-tablet logical
  plan, schema, key, aggregate, and input-width authority.
- [Invariant 10](../architecture/invariants.md): Native payload creation validates all shapes and
  bounds before publication.
- [Invariant 11](../architecture/invariants.md): physical chunks, sort state, encoded batches, and
  copied resource handles retain explicit ownership until terminal publication or destruction.
- [Invariant 14](../architecture/invariants.md): Native Protocol v1 remains the versioned output
  envelope; no grouped transport bytes change.
- [Invariant 18](../architecture/invariants.md): sharing the finalizer does not relax either input's
  authority or memory guarantees.

## Validation

Two mutable tablet servers return count-one and count-two sufficient states for one STRING key. A
checked coordinator projection doubles the globally merged count and the scheduler publishes one
decoded Native row with value six only after both streams close. No result exists before
completion; one-time transfer removes it. Negative cases reject invalid finalization bounds before
I/O and retain no result after deadline expiry or explicit cancellation. Deterministic allocation
injection repeatedly finalizes a completed mutable execution and proves every failure is classified
as resource exhaustion without a published prefix.

The focused mutable scheduler, shared finalizer, and allocation cases pass normally and under
ASan/UBSan with leak detection disabled. The complete cluster suite passes 263 tests and the
complete cluster allocation-failure suite passes 39 tests. Changed-file LLVM 18 formatting, the
warning-as-error build, and whitespace validation pass. Static analysis reaches both changed
sources but is blocked by the installed LLVM 18/macOS 26 libc++ builtin mismatch; project-local
missing-initializer warnings reported before that compiler failure were corrected.

## Migration and rollback

This is an additive internal finalizer generalization and a pre-alpha mutable scheduler API change.
Callers consume `result()` or `take_result()` instead of physical `next()`. Rollback may restore the
portable mutable physical owner, but a Native-facing scheduler must preserve preflighted bounds and
all-or-nothing finalization rather than expose a prefix.

## Unresolved questions

- Select the mutable grouped scheduler from replicated/package query preparation.
- Computed pre-group physical-plan splitting.
- Partitioned shuffle routing, skew policy, and broader process/fault/measurement evidence.

## References

- [Bounded grouped sufficient-state Native finalization](0484-bounded-grouped-sufficient-state-native-finalization.md)
- [Atomic grouped Native TCP publication](0485-atomic-grouped-native-tcp-publication.md)
- [Bounded mutable grouped sufficient-state TCP scheduling](0497-bounded-mutable-grouped-sufficient-state-tcp-scheduling.md)

**Retrospective (2026-08-25):** [ADR 0499](0499-shared-mutable-grouped-query-control-endpoint.md)
makes the committed private endpoint capable of serving the scheduler's remote mutable grouped
requests alongside rows and read authority. Native SQL selection remains separate.
