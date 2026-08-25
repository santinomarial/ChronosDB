# ADR 0490: Proof-revalidated mutable grouped sufficient-state worker

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB query, ingest, replicated-service, and distributed-query maintainers
- **Extends:** [ADR 0429](0429-distinct-proof-bound-mutable-vector-fragment.md),
  [ADR 0471](0471-shared-mergeable-grouped-state-owner.md), and
  [ADR 0474](0474-proof-revalidated-grouped-sufficient-state-worker-v2.md)

## Context

The scalable grouped worker could scan only immutable Manifest/CSEG publications. Packaged Native
queries bind the distinct mutable fragment, whose authority names an exact committed/applied
`TabletSnapshot` and deliberately contains no Manifest generation. Sending that fragment to the
Fragment-v2 worker or transport would silently mix snapshot identities. Keeping only the row-backed
coordinator was correct but left no sufficient-state execution boundary over current heads.

## Decision

Add a distinct mutable grouped worker request that borrows one mutable fragment, exact
`TabletSnapshot`, schema lineage, committed placement, Raft group, local node, optional local
linearizable barrier, and finite grouped-worker limits. Binding and execution independently:

1. structurally validate the mutable fragment and finite limits;
2. require grouped mode and exact local route, placement epoch, active schema, table/tablet,
   Raft commit source/group/applied position, and read-barrier authority;
3. derive the projected physical shapes and exact key/aggregate authority from the mutable
   fragment's plan and raw result schema; and
4. reject every mismatch before scanning or publishing state.

Execution creates the established query-accounted full-`TabletState` source, applies the exact
event-time predicate and projection, and passes that operator to the same grouped table and
canonical frame-encoding pipeline used by the Manifest/CSEG worker. Worker-local order and limit
remain ignored because they are global coordinator semantics. Results own only a complete bounded
canonical stream, including the established empty terminal; no aggregate is finalized locally.

A request-local replicated service adapter reacquires one coherent context for both binding and
execution. It deliberately implements no existing grouped transport interface: that interface
accepts Fragment-v2 and therefore carries Manifest authority. A future mutable carrier must be
distinct and versioned before this worker is connected to the all-tablet Native scheduler.

**Retrospective (2026-08-25):** [ADR 0491](0491-distinct-mutable-grouped-sufficient-state-transport.md)
adds that distinct endpoint by pairing the exact `CHDMREQ1` mutable request with the
authority-agnostic `CHDVGRP2` response. The worker adapter now implements only the mutable grouped
service interface; it still cannot be passed to the Fragment-v2 grouped endpoint.

## Consequences

Current committed head publications can now produce the same multi-key/all-type sufficient state
as immutable CSEG publications without duplicating aggregate semantics or weakening authority.
The common grouped pipeline has one ownership rule: the physical operator and query resources live
through synchronous accumulation, the table owns accounted keys/states, and returned frames own
their encoded bytes.

The worker and adapter are synchronous and thread-affine. They add no shared-memory publication or
new memory-ordering rule; `TabletSnapshot` continues to rely on the existing head release/acquire
publication contract. A distinct mutable grouped wire carrier, retries, all-tablet scheduling,
Native integration, computed pre-group programs, and shuffle routing remain separate work.

## Affected invariants

- [Invariant 4](../architecture/invariants.md) and
  [Invariant 5](../architecture/invariants.md): the worker admits only the exact committed/applied
  Raft publication.
- [Invariant 6](../architecture/invariants.md): snapshot, active schema, placement, group, barrier,
  projection, and result authority are revalidated as one coherent boundary.
- [Invariant 10](../architecture/invariants.md) and
  [Invariant 14](../architecture/invariants.md): output remains the versioned, checksummed grouped
  exchange format; no mutable fragment is reinterpreted as Fragment-v2.
- [Invariant 11](../architecture/invariants.md): the owning context pins every head generation
  through synchronous execution.
- [Invariant 15](../architecture/invariants.md): query memory, scan/projection shapes, group table,
  retained configuration, frames, and encoded bytes remain finite.
- [Invariant 18](../architecture/invariants.md): mutable and immutable workers share grouping
  semantics without weakening their distinct snapshot proofs.

## Validation

Focused query tests bind and execute a two-row exact mutable publication with a nullable STRING
key, decode both canonical COUNT(*) state frames, and reject stale barriers and an undersized group
bound. The request-local service test proves fresh context acquisition for authority binding and
execution. Deterministic allocation injection walks the mutable scan, group table, state, and frame
construction until complete success, requiring every earlier failure to classify as resource
exhaustion.

The complete normal query, query-allocation, cluster, cluster-allocation, service, and
service-allocation suites pass 424, 62, 247, 31, 108, and 5 tests respectively. The focused query,
allocation, and service cases pass under ASan/UBSan with leak detection disabled. The full build,
changed-file LLVM 18 formatting, and whitespace validation pass. LLVM 18 static analysis reports no
remaining project-local finding but cannot complete because the installed macOS 26 libc++ requires
compiler builtins unavailable to LLVM 18. The repository-wide format check reports only the
pre-existing violation in the unchanged grouped-TLS header self-containment test.

## Migration and rollback

This is an additive pre-alpha in-memory API and changes no durable or network bytes. Rollback
removes the mutable grouped request, entry points, and service adapter while retaining the row
worker and Manifest/CSEG grouped lifecycle.

## Unresolved questions

- Mutual-TLS/TCP ownership for the distinct mutable grouped request/response policy.
- All-tablet scheduler composition and atomic Native selection between row-backed and state paths.
- Computed pre-group program ownership and partitioned shuffle/skew policy.

## References

- [Distributed Mutable Vector Fragment v1](../formats/distributed-mutable-vector-fragment-v1.md)
- [Distributed Vector Grouped Aggregate Exchange v1](../formats/distributed-vector-grouped-aggregate-exchange-v1.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
