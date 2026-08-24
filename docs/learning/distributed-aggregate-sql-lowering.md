# Distributed aggregate SQL lowering

## Purpose and public boundary

ChronosDB's local aggregate plan can evaluate expressions before and after aggregation. The
distributed sufficient-state worker intentionally accepts a smaller schema-neutral vocabulary.
`lower_bound_sql_select_to_distributed_vector_aggregate` is the checked bridge from a bound SQL
query to that vocabulary.

Its owned `DistributedVectorAggregateSqlPlan` contains an exact unlimited identity `input_rows`
plan plus the ungrouped aggregate intent and named client result schema. The input plan owns table
and schema IDs, unique source projection ordinals, source descriptors, and the optional event-time
predicate. A separate optional coordinator predicate owns the checked general Boolean expression
program. The aggregate intent alone owns global LIMIT. The product contains no tablet list, query
ID, Manifest pin, Raft read proof, route, TLS context, socket, or final Native payload. Later owners
must add and independently validate those authorities.

## Projection and definitions

Each aggregate call nested in a SELECT output becomes one internal aggregate definition. `COUNT(*)`
carries no input; every other supported operation names one direct source column. Without a source-
dependent coordinator predicate, first use appends the source ordinal to the fragment projection.
Repeated inputs point at the same projected index, while repeated aggregate occurrences remain
separate internal states. A source-dependent predicate instead causes the input row plan to carry
the complete source schema in ordinal order, and aggregate inputs refer to those full-row positions.

For:

```sql
SELECT count(*) AS n, sum(value) AS total, avg(value) AS mean_value,
       sum(value) AS total_again
FROM metrics;
```

the projection contains only `value`; the intent contains four ordered definitions with input
indices `none, 0, 0, 0`. The identity result schema retains all four SQL names. If every operation is
`COUNT(*)`, lowering projects the table's event-time column solely to satisfy the current nonempty
authority-bound fragment projection contract. That column is not an aggregate input or result.

`input_rows` emits each unique projected column exactly once in projection order. Its row outputs
are the identity vector `0..N-1`; it has no hidden visibility mapping, order, grouping, aggregate,
or LIMIT. These properties let the mutable-row carrier provide exact aggregate inputs without
accidentally applying the client LIMIT before accumulation. A future sufficient-state worker can
reuse the same projection and event predicate while ignoring this transitional row intent.

For each definition, lowering asks the shared vector aggregate kernel for its output type and
nullability, compares those values with binding, then validates the complete distributed result
schema. This prevents COUNT, AVG, variance, exact sums, and variable-width extrema from being
reconstructed from result descriptors alone.

## Predicate, cardinality, and finalization

The WHERE subset is identical to distributed row lowering. An AND tree made entirely of direct
event-time/TIMESTAMP comparisons or positive inclusive BETWEEN remains exact row truth at every
worker, not only storage pruning. Every other checked Boolean expression is retained as one
coordinator program and evaluated before any aggregate state sees the row. TRUE admits a row;
FALSE and NULL discard it. Mixed event-time/general predicates currently remain wholly at the
coordinator. A source-independent predicate such as `FALSE` keeps the sparse aggregate projection
and the real COUNT(*) row-count anchor.

LIMIT remains a coordinator operation after final aggregate construction. A global aggregate has
one semantic row even for empty or fully filtered input; LIMIT zero removes that row while retaining
its schema.

If visible outputs differ from the raw aggregate vector, post-aggregate lowering maps aggregate call
spans to internal result ordinals and makes source columns unavailable. The coordinator projection
then evaluates the checked scalar surface used by row SQL. It owns the exact client schema; the
internal schema is never published. Evaluation precedes LIMIT, preserving errors even for LIMIT
zero.

ORDER BY may name a selected output because that expression is already evaluated and one global row
cannot be reordered. Hidden ORDER BY expressions fail closed rather than adding unseen aggregate
state or silently discarding possible errors. GROUP BY now uses the separate bounded row-backed
coordinator physical-pipeline path; multi-key sufficient-state transport remains future work.
Computed global aggregate inputs still require a separately accepted worker vocabulary.

## Ownership, limits, and failures

The returned vectors and strings own their storage. The bound select and catalog may be released
after lowering. Projection width, aggregate width, and result-name bytes are caller bounded and
never exceed the network hard limits. Owned allocation and container-growth failures become
`RESOURCE_EXHAUSTED`; invalid limit configurations become `INVALID_ARGUMENT`; unsupported SQL keeps
the relevant source span and returns `NOT_SUPPORTED`. No partial product escapes.

Work is `O(source columns + aggregate calls + output and WHERE instructions)` and retained memory is
`O(source columns + unique inputs + aggregate calls + expression configuration)`. One thread
constructs the immutable product.

## Tradeoffs and interview questions

**Why not call local physical lowering?** Local plans contain expression programs and execution
policy objects that are neither stable network vocabulary nor implemented by the remote worker.

**Why preserve repeated aggregates?** SQL output identity includes position and alias. Deduplicating
execution state would require a separate proved output mapping; the current protocol bounds width
and preserves the simpler exact contract.

**Why does COUNT(*) need a projection?** It is an explicit compatibility anchor for the existing
fragment authority format, not a semantic dependency. A future format can represent a zero-column
scan only with a versioned decision and worker evidence.

**What prevents partial distributed results?** This lowering has no execution side effects. The
replicated service discards failed whole attempts and accumulates only after every exact tablet
stream terminates; Native finalization then publishes one all-or-none result.

**Why carry a full row for a predicate?** Expression input ordinals are bound to the source schema.
Keeping that exact ordinal mapping avoids a second rewrite language. This is a bounded correctness
baseline; versioned worker predicate pushdown can later reduce transfer volume without changing
truth.

**Why evaluate final expressions after merge?** AVG, variance, extrema, and exact sums are globally
meaningful only after all tablet states merge. Mapping aggregate spans to that one final vector keeps
worker exchange bytes schema-neutral and prevents tablet-local finalization errors.
