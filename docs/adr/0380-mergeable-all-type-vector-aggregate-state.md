# ADR 0380: Mergeable all-type vector aggregate state

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and distributed-query maintainers
- **Extends:** [ADR 0040](0040-streaming-ungrouped-vector-aggregates.md),
  [ADR 0049](0049-query-accounted-variable-width-extrema.md)

## Context

The local ungrouped and grouped operators retained private aggregate state. Distributed vector
workers cannot exchange final output cells as partials: averaged values lose their contributing
counts, variance loses mean and M2, and exact integer or DECIMAL sums may temporarily exceed the
declared result width. Reimplementing those kernels in the distributed layer would create two
oracles for NULL, overflow, NaN, extrema, and allocation behavior.

## Decision

`MergeableVectorAggregateState` is the single move-only, thread-affine state kernel behind both
local aggregate operators. It validates one exact operation/input definition at construction,
accumulates COUNT(*) or one validated physical cell, merges another identically defined state, and
finalizes once into the existing scalar output contract.

COUNT merges checked unsigned partial counts but retains the SQL INT64 ceiling. Exact numeric SUM
keeps the existing signed-magnitude 256-bit accumulator and adds a checked accumulator-to-
accumulator merge. FLOAT32/FLOAT64 SUM and AVG combine their retained sums; AVG also combines the
contributing count. Variance uses the parallel Welford/Chan formula over count, mean, and M2. MIN
and MAX use the shared scalar total order for every current logical type. Empty and NULL behavior
remain unchanged.

Variable-width extrema preserve the existing per-state payload limit and query reservation. A
winning merge reserves before copying, verifies retained capacity, and replaces the old value only
after success. Fixed-width state merge does not allocate. Definitions must match exactly, including
input ordinal, type parameters, and nullability.

This task exposes no state bytes and changes no durable or network format. A separately versioned
codec must encode the non-final state before distributed aggregate execution is enabled.

## Consequences and validation

Local aggregation and future worker/coordinator aggregation now share one semantic implementation.
Partition merge order must be deterministic because floating SUM, AVG, and variance retain normal
IEEE rounding behavior. Exact numeric and COUNT overflow fail closed. The kernel has one caller
thread, so no inter-thread memory-ordering argument applies.

Focused tests merge disjoint partitions for COUNT, COUNT(*), exact SUM, AVG, MIN, MAX, population
variance, and sample variance; cover NULL, exact UINT64 final overflow, bounded variable-width
replacement, definition mismatch, and allocation-failure atomicity. The unchanged local
ungrouped/grouped suites remain differential coverage because both now use this kernel.

Versioned state encoding, partial-I/O ownership, grouped-key exchange, worker execution, and global
aggregate result shaping remain separate tasks. No Phase 16 exit gate is claimed.

Invariants 5, 6, 11, 14, 15, and 18 apply.

## References

- [Streaming aggregate guide](../learning/streaming-ungrouped-aggregates.md)
- [Bounded grouped aggregate guide](../learning/bounded-grouped-aggregates.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Architecture invariants](../architecture/invariants.md)
