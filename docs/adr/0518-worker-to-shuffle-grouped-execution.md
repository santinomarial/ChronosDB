# ADR 0518: Worker-to-shuffle grouped execution

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB distributed-query, cluster, and Native protocol maintainers
- **Extends:** [ADR 0496](0496-portable-mutable-grouped-sufficient-state-execution.md) and
  [ADR 0517](0517-owned-end-to-end-grouped-shuffle-lifecycle.md)

## Context

The grouped worker TCP scheduler already produced complete, canonical, proof-bound tablet streams,
but it immediately merged them into the original single-coordinator path. The partitioned shuffle
owner consequently required an embedding to reconstruct the same stream boundary manually. That
left worker scheduling and shuffle execution as two individually complete but uncomposed phases.

## Decision

Add a mutually exclusive completed-source publication mode to mutable grouped execution. After all
finite local or remote attempts succeed, the portable owner can transfer every canonical sender
stream exactly once in original fragment order. Source transfer and the existing `finish()/next()`
coordinator path cannot both run. The TCP owner publishes either the existing atomic Native result
or these complete sources; it never publishes both.

Add a heap-stable, single-thread-affine owner that derives shuffle authority from the same mutable
fragments, schedules all workers through the existing local/TCP/mTLS owner, transfers complete
streams into the grouped shuffle lifecycle, and exposes only the shuffle's atomically finalized
Native result. Cancellation and terminal failure cover whichever phase currently owns progress.
Allocation failure before source transfer remains retryable where the nested owner permits it;
failure after one-shot transfer is terminal.

The new owner requires destination execution configurations from its embedding. It does not claim
that independently running database daemons can yet return reduced partitions to the coordinator,
and the Native SQL service does not select this path until that deployment policy is configured.

## Consequences

One executable boundary now spans proof-bound mutable worker execution, complete source-stream
publication, partitioned local/remote shuffle, global projection/order/limit, and Native encoding.
The established direct grouped path remains the default and its result contract is unchanged.

One caller thread serializes both phases, so no inter-thread memory ordering is introduced. The
change adds no durable or wire format. Worker routes, TLS contexts, local workers, shuffle listener
security policy, and query resource contexts retain their existing borrow lifetimes.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): worker and shuffle phases derive from one exact
  fragment/key/aggregate authority and publish one result.
- [Invariant 11](../architecture/invariants.md): the stable owner retains every phase and borrowed
  dependency across the one-shot handoff.
- [Invariant 13](../architecture/invariants.md): no source becomes shuffle input before every
  tablet has a complete terminal stream.
- [Invariant 15](../architecture/invariants.md): existing sender, decode, coordinator, source,
  transport, reducer, gather, and Native limits remain finite.
- [Invariant 18](../architecture/invariants.md): source transfer and result publication are
  explicit terminal transitions.

## Validation plan

Focused tests run a real local mutable grouped worker through completed-source publication, local
shuffle partition/reduction, Native decoding, metrics, and cross-phase cancellation. Portable-owner
tests verify incomplete rejection, fragment-order transfer, one-shot semantics, and exclusion from
the direct finalizer. Deterministic allocation injection sweeps construction, worker execution,
source handoff, shuffle construction, and finalization. Header self-containment, normal and
ASan/UBSan suites, formatting, changed-source static analysis, and final diff review are required.

The warning-as-error build and all 302 cluster, 55 cluster allocation-failure, 427 query, and 63
query allocation-failure tests pass. Three focused lifecycle/source-transfer tests and the complete
construction-to-finalization allocation sweep pass under ASan/UBSan with leak detection disabled
for the macOS runtime. Changed C++ files pass LLVM 18 formatting. Changed-source clang-tidy reaches
only the known LLVM 18/macOS 26 libc++ builtin incompatibility. The repository-wide formatting
check retains only the pre-existing unchanged grouped TLS v2 header self-containment violation.

## Migration or rollback considerations

The existing publication mode remains the default. Rollback removes the alternate source result
and composite owner without changing request/response bytes or the direct Native grouped path.

## Unresolved questions

- Select and configure this owner from replicated Native SQL execution.
- Return reduced partitions from independent destination processes.
- Carry computed pre-group expressions in an owned, versioned worker program.
- Qualify node loss, skew, scale-out, and multi-process differential behavior.

## References

- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
