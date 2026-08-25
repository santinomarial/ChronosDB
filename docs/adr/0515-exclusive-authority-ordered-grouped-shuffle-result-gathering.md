# ADR 0515: Exclusive authority-ordered grouped shuffle result gathering

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB distributed-query maintainers
- **Extends:** [ADR 0514](0514-lossless-grouped-shuffle-destination-execution.md)

## Context

Destination executions exposed disjoint finalized partition outputs, but no owner proved complete
destination coverage or selected a deterministic global pull order. Borrowing destinations would
also permit a caller to consume a partition before gathering, silently omitting rows.

## Decision

Add one move-only, single-thread-affine result execution that takes exclusive ownership of exactly
one sealed, unconsumed destination execution per distinct authority destination node. Construction
requires exact authority object identity, rejects missing, extra, duplicate, unsealed, or previously
consumed destinations, and builds one bounded partition-to-owner index in canonical partition-ID
order. Destination output calls now record emitted chunks and terminal partitions so prior
consumption is observable before ownership transfer.

`next` drains one partition to terminal before advancing to the next partition ID. Hash authority
makes partition key sets disjoint, so concatenation requires no second aggregate merge; global SQL
ORDER BY and LIMIT remain downstream operations over the complete stream. Empty partitions advance
without fabricating rows. Any child output failure becomes sticky. Repeated terminal pulls return
terminal without revisiting destinations.

The gatherer owns a separately bounded query resource context for downstream global physical
operators and exposes the exact authority key and aggregate definitions. Destinations and their
reducer output resources are owned until the gatherer is destroyed. The immutable authority is
borrowed and outlives the gatherer. One thread serializes all calls; no shared-memory concurrency is
introduced.

## Consequences

All in-process partition results now have complete coverage, exclusive consumption, deterministic
partition order, sticky failure, and bounded downstream working authority. Cross-process result
transport and integration with final projection/ORDER BY/LIMIT remain separate work.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): every owned destination and output partition shares
  one exact immutable authority.
- [Invariant 11](../architecture/invariants.md): destination, reducer output, gatherer, and global
  working-resource lifetimes are exclusive and explicit.
- [Invariant 15](../architecture/invariants.md): destination count, partition index, output chunks,
  and downstream memory are bounded.
- [Invariant 18](../architecture/invariants.md): deterministic concatenation relies only on disjoint
  canonical hash partitions and does not weaken grouped equality or merge semantics.

## Validation plan

A two-partition local case admits streams in reverse order, transfers the sealed destination, and
proves output remains partition-ID ordered with exact metrics and sticky terminal. Negative tests
reject empty, consumed, and invalid-memory inputs. Allocation injection sweeps exclusive destination
index, working-resource, and PImpl construction. Header self-containment, warning-as-error
ASan/UBSan suites, formatting, changed-source clang-tidy, and diff review are required.

The warning-as-error ASan/UBSan build, all 295 cluster tests, and all 53 cluster
allocation-failure tests pass. Changed C++ files pass LLVM 18 formatting. The repository-wide
format check reaches one unchanged pre-existing grouped-query TLS header violation. Changed-source
clang-tidy reaches only the known LLVM 18/macOS 26 libc++ builtin incompatibility without a
ChronosDB-source finding. Whitespace and scope review pass.

## Migration or rollback considerations

No durable or wire bytes change. Rollback removes the exclusive gatherer and destination output
metrics; callers must not concatenate partition outputs without equivalent complete-coverage and
exclusive-consumption checks.

## Unresolved questions

- Carry partition results across authenticated process boundaries.
- Feed the gathered stream through final projection, global ORDER BY/LIMIT, and Native encoding.
- Jointly own source, destination, deadline, cancellation, and result lifecycles.

## References

- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
