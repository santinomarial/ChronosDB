# Bound SELECT Physical Lowering

## Purpose and interface

`lower_bound_sql_select()` is the first bridge from SQL v1 binding to vector execution. It accepts a
borrowed immutable `BoundSqlSelect` for one synchronous call and returns an owning, move-only
`PhysicalPipelinePlan`. The returned plan retains no pointer into the bound SQL object; callers must
separately keep the bound plan when they need output names or catalog identities.

The unordered physical input is every primary-source column in exact schema ordinal, logical type,
and nullability order. An ordered nonaggregate plan additionally requires the shared four-column
row-version suffix. Aggregate ordering consumes ordinary source shape because its deterministic
identity is produced by the aggregate stage. A future scan planner can project or remap storage,
but it must satisfy the selected shape or introduce an explicit checked mapping stage.

A LATEST plan also requires the suffix, regardless of later aggregation or ordering. It prepares
the bound timestamp expression before WHERE, selects exact winners using physical and row-version
ties, and removes only that timestamp helper before normal lowering continues.

## Lowering algorithm

For WHERE, lowering first outputs every source column plus the predicate. The predicate may be a
source Boolean, constant, or checked program. A Boolean filter consumes its final position. The
final output stage follows bound SELECT order: expanded stars use recorded schema ordinals, direct
columns remain source positions, top-level literals become typed constants, and other supported
expressions become immutable vector programs. LIMIT is appended last.

For a global aggregate query, WHERE keeps the same first position. When every aggregate argument is
a direct column, definitions refer to current source ordinals without an extra copy. If any argument
is computed, one output stage materializes every non-star argument in aggregate traversal order;
the aggregate stage then consumes only those positions. Bound aggregate source spans are mapped to
the resulting one-row columns, so final expressions such as `sum(value + 2) + count(*)` compile as
ordinary vector programs over aggregate results. LIMIT remains last.

For a grouped query, one preparation stage materializes GROUP BY expressions in declared order and
then every non-star aggregate argument. The grouped stage consumes that exact shape and emits keys
followed by aggregate results. Aggregate calls map by exact bound span. Equivalent SELECT and GROUP
BY expressions have distinct spans, so group keys map by recursive bound structure, including
resolved source and column ordinals. Final expressions can use either leaf; an ungrouped source
column cannot cross the grouped boundary. Empty input emits no group.

ORDER BY adds its keys after visible output positions, so expressions need not be projected. A
binder-resolved alias is re-lowered from the aliased SELECT expression because positions in one
output stage cannot refer to sibling outputs. Order-only aggregates join aggregate traversal before
input preparation. Explicit keys retain declared direction and SQL default or explicit NULL
placement.

Base-row ties append schema DEDUP KEY columns, then WAL ID, record sequence, and row ordinal from
the shared suffix. These keys are ascending and NULL-last. WAL ID plus record sequence is the
accepted logical commit position; scan arrival and stable-sort fallback are not SQL identity.
Schemas with no DEDUP KEY use a generated logical identity that current vector sources do not
expose, so their base ORDER BY lowering fails explicitly. Grouped result ties append group-key
columns in declared order; a global aggregate has only one group. After sort, a checked subset
removes every order and identity helper before LIMIT is applied to client-visible columns.

BETWEEN lowers to `value >= lower AND value <= upper`; IN lowers to an OR chain of equality nodes.
The searched value is one shared DAG instruction, and NOT applies one three-valued Boolean node.
Untyped NULL candidates receive the bound peer type. This preserves UNKNOWN behavior without a new
instruction family.

## Bounds, ownership, and failures

`PhysicalSelectLoweringLimits` carries expression, ungrouped-aggregate, grouped-aggregate, sort,
output-chunk, and plan limits. Program storage is reserved at the caller's instruction limit so
allocator growth cannot make an exactly sized program fail the public spare-capacity rule. All
returned stages own their vectors, constants, definitions, and programs.

Checked numeric/decimal/temporal casts, lazy COALESCE, `time_bucket`, STRING/SYMBOL casts, and
ASCII LOWER/UPPER plus text comparisons/NULL predicates lower directly into validated programs.
Unsupported semantic surfaces return
`kUnsupportedSyntax`; capacity
exhaustion returns `kResourceLimit`; inconsistent bound metadata returns `kExecutionFailure`. No
unsupported node is routed through the scalar evaluator. Construction catches allocation and
container-length failure and publishes no partial plan.

## Current boundary and next steps

This baseline supports one source, ordinary WHERE, ordered projection, global and bounded grouped
aggregates, exact bounded ORDER BY where authoritative identity is available, and LIMIT using the
current numeric, Boolean, temporal, UUID, and variable-width output kernels. It rejects generated-
identity base ORDER BY, ASOF, SUBSCRIBE, and EXPLAIN modes. Exact bounded LATEST BY now lowers
through the shared suffix and executes before WHERE. Variable-width MIN/MAX lower through the
query-accounted aggregate state. Grouped execution uses canonical, collision-checked,
query-accounted hashing. Aggregate common-subexpression elimination and spill remain later
decisions.

Lowering complexity is linear in source columns, output syntax, aggregate calls, and generated
instructions. WHERE currently copies the complete source shape before filtering. Mixed direct and
computed aggregate inputs also copy direct arguments into one uniform materialized shape. Later
projection pruning or fusion requires measured evidence and exact peak-memory accounting.
