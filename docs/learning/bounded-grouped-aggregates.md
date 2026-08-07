# Bounded Grouped Vector Aggregates

## Purpose and boundary

The twenty-fifth Phase 9 increment adds the first data-dependent vector operator state. It groups a
finite input stream by exact physical key cells, updates the shared aggregate kernels, and emits
canonical query-accounted rows. It is physical substrate: bound SQL GROUP BY lowering, ORDER BY,
variable-width extrema, hash tables, partial merge, scheduling, and spill remain separate work.

## Public interface

[`aggregate.hpp`](../../include/chronos/query/aggregate.hpp) exports:

- `VectorGroupKeyDefinition`, carrying one ordinal/type/nullability assertion;
- `GroupedAggregateLimits`, bounding groups, keys, aggregates, variable key bytes, retained
  configuration, and output chunks; and
- `GroupedAggregateOperator::create(input, keys, definitions, limits)`.

[`physical_plan.hpp`](../../include/chronos/query/physical_plan.hpp) adds
`GroupedAggregateStage`. Plan creation validates each key and aggregate input against the shape at
that exact stage. Its output shape is every key in caller order followed by every aggregate result.

The operator uniquely owns its child and retained state and remains thread-affine. Input cells are
borrowed only during one synchronous pull. Returned chunks own their canonical bytes independently.

## Group semantics and state

Each group owns an ordered `ScalarValue` key and one existing fixed aggregate state per definition.
Fixed-width values use the scalar total comparison; variable values compare exact bytes without a
temporary allocation. NULL equals NULL for grouping. STRING and SYMBOL retain their distinct types
even when bytes match. NaN equality follows the deterministic scalar total order used by grouping
and sort tie-breaks, rather than ordinary SQL comparison truth.

The first occurrence establishes a group's output position. SQL without ORDER BY does not promise
that order, so consumers must treat results as a multiset. Empty grouped input has no group and
emits no row, unlike global aggregation's one implicit empty group.

COUNT, exact/floating SUM, AVG, fixed-width MIN/MAX, and both variances reuse the global kernels.
The same NULL skipping, widened final overflow, NaN, and sample-cardinality rules therefore apply.
Variable-width MIN/MAX are rejected because their winning payload can be replaced and grow.

## Memory, pull lifecycle, and failure

The first selected row reserves a conservative maximum-group slot array before allocation. A new
group then computes its key payload bytes from borrowed cells, checks the per-group byte limit,
reserves key/state/container credit, and only then copies values. Each reservation stays with its
group. Input chunks retain their own credit during this work, so the query counter represents the
true simultaneous peak.

After the child ends, each pull moves one group's key into typed constant positions, finalizes its
aggregates, and uses `ColumnOutputOperator` to create one canonical row. Temporary position storage
has separate query credit. The retained group reservation remains live until materialization
finishes; then both temporary and group state are released while canonical output credit transfers
to the caller. After the last row, slot credit is released and end-of-stream is sticky.

All dynamic limits and checked arithmetic fail with `RESOURCE_EXHAUSTED`. Shape, ownership, and
malformed configuration failures are `INVALID_ARGUMENT`; counter overflow is `OUT_OF_RANGE`;
cooperative cancellation is `CANCELLED`. Any terminal failure destroys the child and all groups
before returning, so query credit does not wait for caller destruction of the failed operator.

## Complexity and performance evidence

For `R` selected rows, `G` groups, `K` key columns, and `A` aggregate definitions, this baseline is
`O(R * G * K + R * A)` time, `O(G * (K + A) + key bytes)` retained state, and `O(K + A)` temporary
output state. Successful lookup allocates nothing. New groups and emitted rows allocate bounded
owned storage.

`bounded_grouped_aggregates` measures 32,768 INT64-key rows with COUNT/SUM at 16 and 256 groups,
2,048-row input chunks, and complete output drain. Source construction is excluded. It reports
rows, groups, chunk width, and measured pull allocations. The linear-cardinality slope is evidence
for the next hash-table decision, not a product throughput claim.

## Correctness evidence

Examples cover exact variable key bytes, NULL coalescing, sparse input, COUNT/SUM results,
nullability metadata, empty input, group limits, state cleanup, and physical-plan execution. A
fixed-seed property generates 257 nullable keys and values, three uneven chunks, and a sparse
selection, then compares first-seen keys and results with an independent model. Allocation failure
enumerates configuration, slots, variable payload, group state, temporary output, and canonical
output. The plan fuzzer exercises hostile and valid grouped stages under sanitizers. Public-header,
installation, external-consumer, and static-analysis gates protect the exported boundary.

## Tradeoffs and next steps

The operator intentionally avoids a hash function and batch output builder. A production hash table
needs exact canonical hashing for every logical type, finite load/bucket growth, collision proof,
and pre-allocation credit. Batched output should reduce per-group allocation only after it preserves
credit transfer and failure atomicity. SQL lowering can now materialize group expressions and
aggregate arguments into this stage; ORDER BY remains a later operator.

## Likely review questions

**Why are variable grouping keys supported but variable extrema are not?** A group key is copied
once after its exact size is known. A winning extremum can be replaced repeatedly and needs a safe
reservation-resize protocol.

**Why linear lookup?** It is allocation-free after admitted group creation and uses the scalar total
comparison directly. It gives a trusted semantic baseline for a measured hash implementation.

**Why one output row per chunk?** It reuses the already proved canonical constant materializer while
keeping group reservation transfer explicit. Batching is an optimization with a separate peak-
memory argument.

**Why does failure destroy the operator state immediately?** Query credit represents live retained
bytes. A failed pipeline has no valid continuation, so retaining groups until caller destruction
would delay admission recovery for no semantic benefit.
