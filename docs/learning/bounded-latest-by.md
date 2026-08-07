# Bounded Physical LATEST BY

## Purpose and public interfaces

`LatestByOperator` selects one exact current-visible winner per SQL `LATEST BY` key tuple. Its
`VectorLatestByDefinition` names grouping columns, one `TIMESTAMP_NS` helper column, the schema's
physical-ordering columns, and the first column of the shared row-version suffix. `LatestByLimits`
bounds both key vectors and delegates row, state, and output bounds to `SortLimits`.

The operator is move-owned and thread-affine through `PhysicalOperator`. `LatestByStage` makes the
same operation available in an immutable checked `PhysicalPipelinePlan`. Bound SQL lowering adds
that stage automatically for the accepted single-source surface.

## Data flow and exact winner order

The implementation turns winner selection into a complete sort:

1. LATEST group keys ascending with NULL last;
2. timestamp descending with NULL last;
3. every schema physical-ordering key descending with NULL first;
4. WAL ID, record sequence, and row ordinal descending.

The first row in each adjacent group is consequently the scalar-oracle winner. NULL timestamps lose
to every non-NULL timestamp. When timestamps tie, “greatest physical key” uses the scalar comparator's
NULL-last total order, so reversing it requires NULL first. The final three keys make version ties
explicit. Scan arrival and merge-sort stability cannot change a winner.

After sorting, the operator walks the canonical output once and overwrites the selection vector
with the first row of each new exact typed group. The allocation's capacity and query reservation
stay unchanged; only logical selection bytes shrink. Cancellation is polled during the walk.

## SQL lowering and hidden columns

LATEST executes before WHERE. Lowering therefore requests user columns plus the four-column suffix,
adds one timestamp-expression output, runs LATEST, and removes that helper immediately. WHERE then
sees the normal user/suffix layout. Aggregate preparation may discard the suffix; nonaggregate
ORDER BY may reuse it for presentation ties. The final output stage and checked subset ensure the
timestamp, suffix, order expressions, and identity keys never reach clients.

The full unary stage sequence is:

`source -> timestamp preparation -> LATEST -> helper removal -> WHERE -> scalar/aggregate
preparation -> aggregation -> final/order preparation -> ORDER BY -> hidden removal -> LIMIT`.

Stages absent from a query are omitted without changing the relative order.

## Bounds, ownership, and failure behavior

The sort reserves its conservative state before retaining input and owns every buffered accounted
chunk until it materializes one independent output. LATEST adds bounded configuration vectors but
no data-dependent allocation after sort. Plan construction validates exact timestamp and suffix
shapes; runtime repeats the source-dependent checks. Any upstream, comparison, shape, allocation,
or cancellation failure requests cancellation, destroys the sort subtree, and returns its status
without emitting a partial winner set.

Complexity is `O(R log R * K + R * G)` time and bounded sort/output memory, where `R` is selected
rows, `K` is complete sort-key width, and `G` is LATEST group-key width. The adjacent compaction is
allocation-free. A future hash or index plan must be selected by evidence and match these semantics.

## Tradeoffs and interview questions

The sort-based implementation does more comparison work than a winner hash table, but it reuses a
fully query-accounted, cancellation-safe materialization boundary. It is a clean correctness
baseline and keeps the winner proof visible in the physical key list.

**Why is physical-key NULL placement reversed?** The scalar tie chooses the greatest value under a
NULL-last total order. Presenting greatest first means descending non-NULL values while NULL remains
first.

**Why retain the operation suffix column?** All vector sources expose one uniform four-column
row-version shape. LATEST uses only the first three; operation visibility/resolution is a separate
versioning concern.

**Can sort stability be removed?** Yes for LATEST correctness: every winner tie is explicit. The
underlying sort may remain stable for its own operator contract.

**What is unordered LATEST output order?** A multiset. The current group-key order is an
implementation detail; only SQL `ORDER BY` creates a client-visible order contract.
