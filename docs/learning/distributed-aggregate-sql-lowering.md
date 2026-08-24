# Distributed aggregate SQL lowering

## Purpose and public boundary

ChronosDB's local aggregate plan can evaluate expressions before and after aggregation. The
distributed sufficient-state worker intentionally accepts a smaller schema-neutral vocabulary.
`lower_bound_sql_select_to_distributed_vector_aggregate` is the checked bridge from a bound SQL
query to that vocabulary.

Its owned `DistributedVectorAggregateSqlPlan` contains the exact table and schema IDs, unique source
projection ordinals, an optional event-time predicate, an ungrouped distributed vector intent, and
the named result schema. It contains no tablet list, query ID, Manifest pin, Raft read proof, route,
TLS context, socket, or final Native payload. Later owners must add and independently validate those
authorities.

## Projection and definitions

Each SELECT output must be one aggregate call. `COUNT(*)` carries no input; every other supported
operation names one direct source column. First use appends the source ordinal to the fragment
projection. Repeated uses point at the same projected index but remain separate aggregate outputs.

For:

```sql
SELECT count(*) AS n, sum(value) AS total, avg(value) AS mean_value,
       sum(value) AS total_again
FROM metrics;
```

the projection contains only `value`; the intent contains four ordered definitions with input
indices `none, 0, 0, 0`. The result schema retains all four SQL names. If every operation is
`COUNT(*)`, lowering projects the table's event-time column solely to satisfy the current nonempty
authority-bound fragment projection contract. That column is not an aggregate input or result.

For each definition, lowering asks the shared vector aggregate kernel for its output type and
nullability, compares those values with binding, then validates the complete distributed result
schema. This prevents COUNT, AVG, variance, exact sums, and variable-width extrema from being
reconstructed from result descriptors alone.

## Predicate, cardinality, and finalization

The WHERE subset is identical to distributed row lowering: an AND tree of direct event-time versus
TIMESTAMP comparisons or positive inclusive BETWEEN. It is exact row truth at every worker, not
only storage pruning. LIMIT remains a coordinator operation. A global aggregate has one semantic
row even for empty input; LIMIT zero removes that row while retaining its schema.

ORDER BY currently fails closed. Although ordering cannot reorder one global row, evaluating a
computed order expression could fail, so silently dropping the clause would broaden semantics.
GROUP BY needs its keyed state protocol, and final scalar expressions need a coordinator expression
stage; neither is represented as a direct ungrouped aggregate definition.

## Ownership, limits, and failures

The returned vectors and strings own their storage. The bound select and catalog may be released
after lowering. Projection width, aggregate width, and result-name bytes are caller bounded and
never exceed the network hard limits. Owned allocation and container-growth failures become
`RESOURCE_EXHAUSTED`; invalid limit configurations become `INVALID_ARGUMENT`; unsupported SQL keeps
the relevant source span and returns `NOT_SUPPORTED`. No partial product escapes.

Work is `O(source columns + outputs + WHERE leaves)` and retained memory is
`O(source columns + unique inputs + outputs)`. One thread constructs the immutable product.

## Tradeoffs and interview questions

**Why not call local physical lowering?** Local plans contain expression programs and execution
policy objects that are neither stable network vocabulary nor implemented by the remote worker.

**Why preserve repeated aggregates?** SQL output identity includes position and alias. Deduplicating
execution state would require a separate proved output mapping; the current protocol bounds width
and preserves the simpler exact contract.

**Why does COUNT(*) need a projection?** It is an explicit compatibility anchor for the existing
fragment authority format, not a semantic dependency. A future format can represent a zero-column
scan only with a versioned decision and worker evidence.

**What prevents partial distributed results?** This lowering has no execution side effects. Later
coordinators merge sufficient states only after every exact tablet stream terminates and Native
finalization publishes one all-or-none result.
