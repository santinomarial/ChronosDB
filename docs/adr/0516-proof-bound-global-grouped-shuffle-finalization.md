# ADR 0516: Proof-bound global grouped shuffle finalization

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB distributed-query and query maintainers
- **Extends:** [ADR 0511](0511-proof-bound-grouped-shuffle-destination-selection.md),
  [ADR 0515](0515-exclusive-authority-ordered-grouped-shuffle-result-gathering.md)

## Context

The result gatherer produced every disjoint partition, but attaching caller-assembled plan or result
schema metadata could apply the wrong projection, ORDER BY, LIMIT, or Native schema. Gathered chunks
also retained their destination reducer resource identities, while global physical operators require
one query resource authority.

## Decision

Add a move-only finalization authority derived from the same complete validated mutable fragment set
as the shuffle. It re-derives source and sorted serving-node destination authority, exact-compares
query/hash/source/destination identity with the borrowed shuffle authority, and owns the immutable
logical plan, result schema, projection width, and tablet identity. Finalization additionally
requires the gatherer to borrow that exact shuffle authority object.

Before publication, the gatherer now canonical-materializes every selected chunk into owned columns
charged to its bounded global query resource context. This explicit bridge permits the existing
physical pipeline to validate one resource identity and then apply checked projection, global sort,
global limit, and atomic Native batch encoding. Materialization failure is sticky because the child
chunk has been consumed. The public query utility copies every selected cell with the existing
finite vector-chunk limits and query reservation contract.

The existing grouped finalization implementation is reused through a private adapter; raw and
projected shuffle entry points therefore retain its authority-shape checks, no-prefix publication,
output row/batch/byte limits, and schema-bearing empty result.

## Consequences

An in-process completed shuffle can now reach the same global SQL and Native result boundary as the
portable grouped coordinator without invented metadata or mixed query accounting. Authenticated
cross-process partition-result transport remains open. [ADR 0517](0517-owned-end-to-end-grouped-shuffle-lifecycle.md)
subsequently owns the complete post-worker lifecycle through this finalizer.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): fragments, shuffle authority, gathered results,
  plan, schema, and final output share one proof-derived identity.
- [Invariant 10](../architecture/invariants.md): fragment and shape validation precede allocation-
  driving global execution and Native encoding.
- [Invariant 11](../architecture/invariants.md): destination chunks are synchronously borrowed,
  copied, and re-owned by one global query context before downstream retention.
- [Invariant 15](../architecture/invariants.md): materialized chunks and final rows, batches, bytes,
  sort state, and query memory remain bounded.
- [Invariant 18](../architecture/invariants.md): the established checked global projection,
  ORDER BY, LIMIT, and Native pipeline is reused unchanged.

## Validation plan

A proof-derived mutable fragment creates its shuffle and finalization authorities, two raw groups
cross the destination/gatherer resource boundary, and the fragment's global LIMIT produces one
Native row with the exact result schema. Serving-node drift fails authority derivation. Allocation
injection covers finalization-authority derivation and sticky cross-context materialization.
Header self-containment, warning-as-error ASan/UBSan suites, formatting, changed-source clang-tidy,
and final diff review are required.

The warning-as-error ASan/UBSan build, all 296 cluster tests, all 53 cluster allocation-failure
tests, and all 427 query tests pass. Changed C++ files pass LLVM 18 formatting. The repository-wide
format check reaches one unchanged pre-existing grouped-query TLS header violation. Changed-source
clang-tidy reaches only the known LLVM 18/macOS 26 libc++ builtin incompatibility without a
ChronosDB-source finding. Whitespace and scope review pass.

## Migration or rollback considerations

No durable or wire bytes change. Rollback removes the shuffle finalization entry points and public
chunk materialization bridge; gathered chunks must not enter global operators under a different
query resource identity.

## Unresolved questions

- Carry partition results over an authenticated, bounded cross-process protocol.
- Integrate the packaged post-worker lifecycle with worker execution and Native SQL selection.
- Add node/receipt loss, skew, multi-process differential, and performance qualification.

## References

- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
