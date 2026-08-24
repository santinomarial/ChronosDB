# ADR 0451: Bounded mutable-row global aggregate finalization

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB query, cluster, distributed-query, and Native Protocol maintainers
- **Extends:** [ADR 0379](0379-bounded-global-vector-row-finalization-v2.md),
  [ADR 0386](0386-native-vector-aggregate-result-finalization-v2.md),
  [ADR 0450](0450-schema-bound-distributed-global-aggregate-sql-lowering.md)

## Context

The replicated mutable query plane already returns complete, proof-revalidated row streams from
every current tablet. The older distributed aggregate carrier executes over immutable
Manifest/CSEG snapshots and is not owned by that plane. Native aggregate SQL therefore needed
either a second packaged endpoint and authority model or a bounded bridge over the current mutable
row carrier.

## Decision

`finalize_distributed_vector_aggregate_rows_v2` consumes one completed all-tablet mutable-row
execution and one independently validated ungrouped aggregate Plan Intent. Its input must be an
unlimited, unordered, fully visible identity row projection. Before accumulation it validates the
input and aggregate schemas, one nonnil query identity, canonical tablet segments, contiguous
message sequences, terminal closure, unique tablets, Native batch header shapes, and explicit
message, row, encoded-byte, batch, query-memory, and working-memory limits.

Each Native canonical cell is exposed as a one-row physical column view and accumulated by the
existing `MergeableVectorAggregateState` kernel. `COUNT(*)` advances without an input cell. The
shared kernel therefore retains the existing exact integer/decimal sum behavior, floating average
and variance state, NULL rules, and bounded query-accounted variable-width extrema. Batches are
decoded one at a time; source rows are never retained as a second coordinator row set. Only after
all batches accumulate successfully are the scalars moved into the existing Native aggregate
finalizer, which applies global LIMIT and emits one all-or-none schema-bearing payload.

This is a transitional execution strategy, not worker-side aggregate pushdown. It deliberately
reuses the current replicated authority and authenticated mutable-row endpoint. The row, byte, and
memory ceilings make the tradeoff explicit until the sufficient-state carrier is composed with the
same mutable snapshot authority.

## Consequences

Work is `O(rows * aggregates)` and peak additional decoded-batch memory is bounded independently of
total row count. Retained aggregate memory is `O(aggregates + retained variable extrema)`. The
function is synchronous and thread-affine; no inter-thread memory-ordering argument applies. It
adds no durable or network format and changes no acknowledged-write guarantee.

Malformed stream identity or sequencing is `INVALID_ARGUMENT`; corrupted batch bytes, descriptor
drift, NULLability, or canonical-cell violations are `CORRUPTION`; configured exhaustion and owned
allocation failure are `RESOURCE_EXHAUSTED`. No scalar or Native output is published on failure.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): the row and aggregate schemas are exact authority
  inputs and are revalidated before execution.
- [Invariant 10](../architecture/invariants.md): publication waits for every tablet stream to close.
- [Invariant 14](../architecture/invariants.md): aggregate truth is computed by the shared canonical
  kernel, not a transport-specific reimplementation.
- [Invariant 15](../architecture/invariants.md): every row, message, byte, batch, state, and output
  allocation has a finite caller and hard bound.
- [Invariant 18](../architecture/invariants.md): nonidentity or partially finalized input fails
  closed instead of changing SQL semantics.

## Validation

Focused tests cover multiple tablets including an empty tablet, NULL-aware COUNT, exact SUM, AVG,
population variance, variable-width MIN, LIMIT zero, incomplete streams, descriptor corruption,
duplicate tablets, nonidentity input, row/working-memory exhaustion, and every injected
owned-allocation failure. All 194 cluster tests and all 26 cluster allocation-failure tests pass.
The three focused correctness cases and the allocation-injection case pass under ASan/UBSan with
macOS leak detection disabled. Formatting, diff checks, LLVM 18 analysis of the changed production
source, and the installed external-consumer test pass. TSan is not applicable because the finalizer
is synchronous and has no shared state.

## Migration and rollback

The API is additive. Rollback removes the bridge and its callers without changing on-disk or wire
bytes. Native integration must keep the row fetch unlimited; applying SQL LIMIT before aggregation
would be a correctness bug.

## References

- [Distributed aggregate SQL lowering](../learning/distributed-aggregate-sql-lowering.md)
- [Mutable-row aggregate finalization](../learning/mutable-row-aggregate-finalization.md)
- [Native Protocol v1](../protocol/native-v1.md)
