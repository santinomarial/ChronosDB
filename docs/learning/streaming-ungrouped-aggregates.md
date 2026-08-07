# Streaming Ungrouped Vector Aggregates

## Purpose and boundary

The twenty-third Phase 9 increment adds the first vector aggregate: one bounded streaming global
group. It consumes `AccountedVectorChunk` inputs, updates fixed state, releases each input chunk,
and emits one query-accounted canonical row.

Bound single-source SQL now lowers global aggregate expressions through this substrate. It does not
implement GROUP BY, variable-width MIN/MAX, ordering, joins, partial-state merge, spill, parallel
scheduling, or storage visibility composition.

## Public interface

[`aggregate.hpp`](../../include/chronos/query/aggregate.hpp) exposes:

- `VectorAggregateOperation`, covering all eight SQL v1 aggregate operations;
- `VectorAggregateInput`, an exact physical ordinal/type/nullability assertion;
- `VectorAggregateDefinition` and `vector_aggregate_output_shape()`;
- `UngroupedAggregateLimits`, bounding width, retained configuration, and output chunks; and
- `UngroupedAggregateOperator::create()`, returning the ordinary uniquely owned physical-operator
  interface.

[`physical_plan.hpp`](../../include/chronos/query/physical_plan.hpp) adds
`UngroupedAggregateStage`. Plan validation resolves each definition against the shape at that exact
stage and replaces it with the ordered aggregate result shape.

The operator is uniquely owned and thread-affine. Definitions and state are owned by the operator;
input chunks are borrowed only during one synchronous `next()` call. The returned accounted chunk
owns all result bytes independently.

## State and semantics

Every definition has one `AggregateState`:

| Operation | Retained state | Result |
| --- | --- | --- |
| `COUNT(*)`, `COUNT(expr)` | checked INT64 count | nonnullable INT64 |
| exact `SUM` | signed-magnitude 256-bit accumulator | declared integer/DECIMAL type |
| floating `SUM` | native FLOAT32 or FLOAT64 sum | same floating type |
| `AVG` | FLOAT64 sum and contributing count | nullable FLOAT64 |
| `MIN`, `MAX` | optional fixed-width scalar | nullable input type |
| variance | count, mean, and Welford `M2` | nullable FLOAT64 |

`COUNT(*)` counts selected rows. `COUNT(expr)` reads only the cell's NULL bit, so STRING, SYMBOL,
and BINARY do not create owned payloads. All other operations skip NULL cells. Empty global input
still forms one group: COUNT returns zero and all other results are NULL. `VAR_SAMP` requires two
contributing values.

Exact SUM accumulates beyond the declared result width and validates only at finalization, matching
the scalar oracle. This means intermediate INT8 overflow does not fail if later values bring the
final result back into range. Floating SUM and AVG preserve input order. Variance uses the same
Welford update as the reference engine. MIN/MAX use the shared scalar comparison, whose floating
order places ordinary numbers before NaNs and compares NaN payloads deterministically.

## Pull, memory, and cancellation

The first pull drains the sequential child. A chunk is shape- and query-owner-checked, consumed,
and destroyed before the next child pull. Thus peak input ownership remains the child's existing
chunk contract; the aggregate never retains a row set.

Width is capped at 4,096 definitions by default. Checked capacity accounting covers the retained
fixed state vector, including its copied definitions, with a default 2 MiB configuration bound.
The caller's definition vector is read synchronously and is not retained. This bounded coordinator-
owned memory is not charged again to `QueryResourceContext`. Result
materialization creates a one-row cardinality chunk and uses `ColumnOutputOperator`, so selection
and column buffers are admitted and charged through the existing exact output path. Nullable
aggregate definitions remain physically nullable even when their result is present: the one-row
column carries an all-valid bitmap and zero nulls. COUNT outputs remain nonnullable.

Cancellation is checked before draining, by every child, and every 256 selected rows during the
aggregate loop. A foreign query chunk, runtime shape mismatch, child error, state error, or output
failure requests shared cancellation. Move-only input/output owners then return reservations during
normal stack unwinding. Successful output is emitted once; later pulls return stable end.

## Failure behavior

- `INVALID_ARGUMENT`: malformed definitions/limits, unsupported input type, variable-width
  extrema, source shape mismatch, or a chunk owned by another query;
- `RESOURCE_EXHAUSTED`: width/configuration/output limits or classified allocation failure;
- `OUT_OF_RANGE`: an aggregate count or internal running-count domain is exhausted; and
- `CANCELLED`: cooperative cancellation observed before or during work.

Final integer/DECIMAL representability uses the existing scalar constructors and exact accumulator,
so their checked status is preserved. No partial result is returned after any failure.

## Complexity and performance evidence

For `R` selected rows, `A` definitions, and `C` chunks, execution is `O(R * A + C)` time and `O(A)`
retained state. Output construction is `O(A)`. No successful fixed-width row update allocates.

`chronos_query_benchmarks` includes `streaming_ungrouped_aggregates`, which executes COUNT, SUM,
AVG, MIN, MAX, and VAR_POP together over dense and stride-four selections at 256- and 2,048-row
chunk sizes. Source/chunk construction is paused outside timing. The benchmark reports selected
rows, physical rows, selection density, chunk size, and allocations during the measured pull. It is
an operator baseline, not an end-to-end SQL throughput claim.

## Correctness evidence

Deterministic examples cover all operations, empty/NULL/NaN rules, exact UINT64 and DECIMAL sums,
variable-width COUNT, result overflow, physical-plan integration, ownership, cancellation, and
runtime shape failures. A fixed-seed 257-row model crosses three unequal chunk boundaries and sparse
selections while independently computing count, exact sum, average, extrema, and variance.

Allocation-failure tests enumerate operator construction and output materialization failures and
assert zero leaked query credit. The physical-plan fuzzer creates hostile aggregate definitions and
also executes a valid aggregate pipeline. Public-header self-containment, installation/external-
consumer compilation, ASan/UBSan, TSan, static analysis, and repository-wide regression guard the
remaining boundaries.

## Tradeoffs and next steps

The state update currently visits definitions inside each selected row. This is simple and shares
the scalar conversion/comparison rules, but a column-at-a-time specialized kernel may be faster.
Such a change needs a profile and differential tests for error order, floating behavior, and
cancellation latency.

The next aggregate decisions are dynamic state ownership: grouped key encoding, variable-width
extremum payloads, finite group admission, and spill/merge behavior. Bound SQL uses this stage for
ungrouped queries with exact source-span aggregate identity, optional expression-input
materialization, and final one-row vector expressions.

## Likely interview questions

**Why does an empty input emit a row?** SQL global aggregation forms one implicit group. Grouped
aggregation instead has no group and emits no rows.

**Why reject variable-width MIN/MAX but allow COUNT(STRING)?** COUNT needs only NULL presence.
MIN/MAX must retain a winning payload whose dynamic bytes need an explicit query-accounting policy.

**Why not retain chunks and call the scalar engine?** That would make memory proportional to input,
violate streaming ownership, and turn the scalar oracle into the production vector path.

**Why materialize through the column-output operator?** It centralizes canonical buffer layout,
limit calculation, allocation classification, and query-credit ownership.
