# Bounded Grouped Vector Aggregates

## Purpose and boundary

The twenty-fifth Phase 9 increment adds the first data-dependent vector operator state. It groups a
finite input stream by exact physical key cells, updates the shared aggregate kernels, and emits
canonical query-accounted rows. The twenty-sixth increment connects bound single-source GROUP BY
through that substrate. Later increments added ORDER BY, query-accounted variable-width extrema,
and canonical query-accounted hash lookup. Group states now use the shared in-memory partial merge
kernel; versioned transport, scheduling, and spill remain separate work.

## Public interface

[`aggregate.hpp`](../../include/chronos/query/aggregate.hpp) exports:

- `VectorGroupKeyDefinition`, carrying one ordinal/type/nullability assertion;
- `GroupedAggregateLimits`, bounding groups, keys, aggregates, variable key/extremum bytes,
  retained configuration, and output chunks; and
- `MergeableVectorGroupedAggregateTable`, the move-only shared accumulator exposing borrowed
  first-seen key/sufficient-state spans for synchronous distributed encoding; and
- `GroupedAggregateOperator::create(input, keys, definitions, limits)`.

[`physical_plan.hpp`](../../include/chronos/query/physical_plan.hpp) adds
`GroupedAggregateStage`. Plan creation validates each key and aggregate input against the shape at
that exact stage. Its output shape is every key in caller order followed by every aggregate result.

The operator uniquely owns its child and one shared grouped table; both remain thread-affine. The
table accepts only query-accounted chunks from its exact resource context. Input cells are borrowed
only during synchronous accumulation. Key/state spans borrow the table until its next mutation,
while returned local chunks and encoded distributed frames own their canonical bytes independently.
The same table can synchronously merge borrowed scalar-key sufficient states. Its first retained
allocation binds it to one query context; a merge failure destroys all groups, and first-seen
materialization seals the table against further mutation.

## Group semantics and state

Each group owns an ordered `ScalarValue` key and one existing fixed aggregate state per definition.
Fixed-width values use the scalar total comparison; variable values compare exact bytes without a
temporary allocation. NULL equals NULL for grouping. STRING and SYMBOL retain their distinct types
even when bytes match. NaN equality follows the deterministic scalar total order used by grouping
and sort tie-breaks, rather than ordinary SQL comparison truth.

Lookup uses a pre-sized power-of-two open-addressed table at no more than one-half load. Hash input
frames every key with its type parameters, NULL marker, and payload length. Canonical physical bytes
cover integer, decimal, temporal, UUID, and variable domains. Boolean uses its logical value;
floating keys normalize signed zero and all NaN encodings to match group equality. A matching hash
always performs exact retained-key comparison, so collisions cannot merge groups. Buckets store
only stable ordinals into the separately reserved group vector.

The first occurrence establishes a group's output position. SQL without ORDER BY does not promise
that order, so consumers must treat results as a multiset. Empty grouped input has no group and
emits no row, unlike global aggregation's one implicit empty group.

COUNT, exact/floating SUM, AVG, all-type MIN/MAX, and both variances reuse the global kernels.
The same NULL skipping, widened final overflow, NaN, and sample-cardinality rules therefore apply.
Each variable-width MIN/MAX state reserves its winning payload independently before copying it.
Replacement holds old and new credit until the new value is complete, then releases the old owner.
The same state can merge an identically defined partition: exact accumulators remain wide, AVG
retains count, variance combines count/mean/M2, and a variable-width winning partial is copied only
after new query credit is reserved. The nested state now has canonical bounded bytes, but group-key
and stream correlation still require a separate grouped exchange envelope.

## Memory, pull lifecycle, and failure

The first selected row reserves conservative credit for the maximum-group slot array and a fixed
bucket table before allocating either. A new group then computes its key payload bytes from
borrowed cells, checks the per-group byte limit, reserves key/state/container credit, and only then
copies values. Each reservation stays with its group. Input chunks retain their own credit during
this work, so the query counter represents the true simultaneous peak. No lookup or insertion
rehashes or allocates bucket storage.

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

For `R` selected rows, `G` groups, `K` key columns, `B >= 2G_max` buckets, and `A` aggregate
definitions, expected time is `O(R * (K + A))`; an adversarial collision set remains bounded by
`O(R * B * K + R * A)`. Retained state is
`O(B + G * (K + A) + key and extremum bytes)`, with `O(K + A)` temporary output state. Successful
lookup allocates nothing. New groups and emitted rows allocate bounded owned storage.

`bounded_grouped_aggregates` measures 32,768 INT64-key rows with COUNT/SUM at 1, 16, 256, and 4,096
groups, 2,048-row input chunks, and complete output drain. Source construction is excluded. It
reports rows, groups, chunk width, and measured pull allocations. These cardinality profiles
measure the hash decision; debug-build values are not product throughput claims.

## Correctness evidence

Examples cover exact variable key bytes, NULL coalescing, sparse input, COUNT/SUM results,
nullability metadata, signed-zero and NaN canonicalization, deliberate hash collisions, empty input,
group limits, state cleanup, and physical-plan execution. A
fixed-seed property generates 257 nullable keys and values, three uneven chunks, and a sparse
selection, then compares first-seen keys and results with an independent model. Allocation failure
enumerates configuration, slots, buckets, variable payload, group state, temporary output, and
canonical output. A dedicated execution fuzzer compares arbitrary nullable FLOAT64 keys, including
NaN payloads and signed zeros, with an independent first-seen model. The plan fuzzer exercises
hostile grouped stages under sanitizers. Public-header, installation, external-consumer, and
static-analysis gates protect the exported boundary.

## Tradeoffs and next steps

The operator intentionally retains one-row output materialization even though lookup is hashed.
Batched output should reduce per-group allocation only after it preserves credit transfer and
failure atomicity. Correlated group/state exchange, parallel scheduling, and partitioned spill need
separate ownership decisions.

## Likely review questions

**Why is each variable extremum bounded independently?** Each aggregate state has an independently
replaceable winner. Per-state bounds compose with the query-wide budget without coupling otherwise
independent aggregate definitions.

**Why does a hash match still compare the key?** Hashes only narrow candidates. Exact comparison is
the semantic authority and makes collisions harmless.

**Why allocate buckets from the maximum group count?** Fixed capacity makes admission exact and
removes failure-prone rehashing from row consumption. The tradeoff is conservative memory for
queries whose actual group count is small.

**Why one output row per chunk?** It reuses the already proved canonical constant materializer while
keeping group reservation transfer explicit. Batching is an optimization with a separate peak-
memory argument.

**Why does failure destroy the operator state immediately?** Query credit represents live retained
bytes. A failed pipeline has no valid continuation, so retaining groups until caller destruction
would delay admission recovery for no semantic benefit.
