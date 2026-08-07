# Bound SELECT Physical Lowering

## Purpose and interface

`lower_bound_sql_select()` is the first bridge from SQL v1 binding to vector execution. It accepts a
borrowed immutable `BoundSqlSelect` for one synchronous call and returns an owning, move-only
`PhysicalPipelinePlan`. The returned plan retains no pointer into the bound SQL object; callers must
separately keep the bound plan when they need output names or catalog identities.

The physical input is every primary-source column in exact schema ordinal, logical type, and
nullability order. A future scan planner can project or remap storage, but it must satisfy this shape
or introduce an explicit checked mapping stage.

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

BETWEEN lowers to `value >= lower AND value <= upper`; IN lowers to an OR chain of equality nodes.
The searched value is one shared DAG instruction, and NOT applies one three-valued Boolean node.
Untyped NULL candidates receive the bound peer type. This preserves UNKNOWN behavior without a new
instruction family.

## Bounds, ownership, and failures

`PhysicalSelectLoweringLimits` carries expression, ungrouped-aggregate, grouped-aggregate,
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
aggregates, and LIMIT using the current numeric, Boolean, temporal, UUID, and variable-width output
kernels. It rejects ORDER BY, LATEST, ASOF, SUBSCRIBE, EXPLAIN modes, and variable-width MIN/MAX.
Hash grouping, aggregate common-subexpression elimination, and spill remain later decisions.

Lowering complexity is linear in source columns, output syntax, aggregate calls, and generated
instructions. WHERE currently copies the complete source shape before filtering. Mixed direct and
computed aggregate inputs also copy direct arguments into one uniform materialized shape. Later
projection pruning or fusion requires measured evidence and exact peak-memory accounting.
