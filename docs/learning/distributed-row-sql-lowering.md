# Distributed row SQL lowering

## Purpose and boundary

ChronosDB has two different physical query vocabularies. The local engine owns a rich
`PhysicalPipelinePlan`; the distributed row protocol owns the smaller, schema-neutral
`DistributedVectorPlanIntent`. Sending a local plan over the network would freeze implementation
details into the protocol, while accepting all locally executable SQL would promise worker
capabilities that do not exist. Distributed row SQL lowering is the explicit checked bridge.

The input is a `BoundSqlSelect`. Binding has already pinned an immutable catalog/schema snapshot,
resolved every column reference, inferred types and nullability, expanded stars, and assigned
output names. The output is an owned `DistributedVectorRowsSqlPlan` containing:

- the exact table and destination schema identities;
- unique destination source-column ordinals;
- an optional exact worker event-time predicate;
- the canonical row-mode intent with output mapping, final order, and final limit; and
- the exact worker result schema; and
- when needed, a coordinator-only source/constant/expression projection with an optional Boolean
  predicate and the final named result schema.

It owns no query ID, tablet set, Raft proof, route, TLS context, socket, or Native response. Those
belong to later authority and lifecycle layers.

## Projection mapping

Fragment projections must be unique, but SQL outputs may repeat. Lowering therefore scans SELECT
outputs in order. The first use of a source ordinal appends it to
`destination_column_ordinals`; every output appends the corresponding projected index to
`row_output_indices`.

For `SELECT value, ts, value`, the result is:

```text
destination source ordinals: [value, ts]
row output indices:          [0,     1,  0]
```

Workers read each source column once and reproduce SQL output order exactly. Result schema entries
remain one per SQL output, so aliases and repeated values retain distinct visible identities.

## Coordinator outputs

A SELECT output whose complete bound tree contains no source column, star, aggregate, or relational
dependency is evaluated once by the scalar oracle. Lowering retains its typed canonical bytes in a
bounded coordinator projection. Direct outputs in the same query retain worker-output indices in
that projection. If no output reads a source column, the event-time column is projected solely as a
real row-count anchor; it is never exposed to the client.

The coordinator validates source shapes and canonical bytes after every tablet stream closes. It
evaluates any retained WHERE predicate, sorts and applies LIMIT against the surviving real worker
outputs, then injects constants while encoding the final Native batches. Predicate TRUE retains a
row; FALSE and NULL discard it. A selected constant ORDER BY key is removed because it cannot
distinguish rows. Constant output evaluation errors are planning errors.

A row-dependent SELECT output is now lowered through the same checked `VectorExpression` program
used by the local physical engine. Because that program addresses schema ordinals, the worker
projection carries the complete source schema in exact ordinal order whenever at least one such
output exists. This is a deliberate transitional bandwidth cost: it avoids remapping programs or
defining a wire bytecode. The coordinator validates every program leaf against the worker schema
and evaluates only the globally ordered, limited rows.

Expression materialization uses an exact sizing pass followed by one contiguous canonical output
arena. Fixed values write through the nonallocating canonical scalar encoder. STRING/SYMBOL values
remain borrowed with an identity/lower/upper transform until their final bytes are copied. Runtime
errors abort the complete result before publication.

## WHERE execution and event-time specialization

General bound Boolean WHERE expressions lower through the same checked vector program. A
source-dependent predicate causes the complete source schema to cross the worker boundary so its
schema ordinals remain exact. A source-independent predicate retains the otherwise sufficient
worker projection. Predicate evaluation happens over every decoded row after all streams close and
before global ordering or LIMIT. This preserves three-valued SQL truth and ensures LIMIT counts only
matching rows.

The existing event-time range form remains a worker-side specialization. Its accepted grammar is
an AND tree whose leaves either compare the schema's declared event-time column with a `TIMESTAMP`
literal or use positive `event_time BETWEEN lower AND upper` with two `TIMESTAMP` literals.
Comparison operand order is normalized first. `BETWEEN` contributes inclusive
bounds and never reorders a reversed range. Lower bounds retain the greatest endpoint; upper bounds
retain the least endpoint. Equal endpoints combine inclusivity with logical AND, so any strict
comparison keeps the combined endpoint strict. `NOT BETWEEN` cannot be represented by one generally
contiguous range, so it bypasses this specialization and executes as a general coordinator program.

No endpoint is incremented or decremented. `INT64_MIN` and `INT64_MAX` are therefore safe. Reversed
or equal-open bounds are retained as valid empty predicates and evaluated exactly by the worker's
timestamp-range filter. Metadata pruning may consume the same values, but it is not accepted as a
substitute for row-level truth.

## Global order and limit

ORDER BY keys index complete worker outputs. When a direct source reference is absent from the
SELECT list, lowering adds that source ordinal to the fragment projection, appends one hidden worker
output, and records the original SELECT outputs in Plan Intent 1.1's visible-row vector. The global
finalizer sorts against the complete schema and encodes only those visible positions. Repeated
references reuse the same helper and repeated direct order keys are removed: once two rows compare
equal on that value, comparing the identical value again cannot distinguish them.

If any ORDER BY key is computed, lowering retains every nonconstant key as a coordinator expression
in SQL key order and workers carry the complete source schema. Moving the whole key list avoids
splitting precedence between worker-output indices and coordinator programs. The finalizer
evaluates fixed or borrowed variable-width values inside its stable merge comparator, honors NULL
placement independently of direction, and applies LIMIT afterward. Successful comparisons allocate
no per-row or per-comparison storage; two reusable canonical source-row views are bounded up front.

Direction and explicit NULL placement are retained. Without `NULLS FIRST/LAST`, ascending uses
NULLS LAST and descending uses NULLS FIRST, matching local physical lowering. ORDER BY and LIMIT
are never applied independently by tablet workers; the coordinator finalizer applies them after all
fragment streams succeed.

## Failure behavior and complexity

Unsupported SQL returns a source-spanned `NOT_SUPPORTED` diagnostic. Invalid limits return
`INVALID_ARGUMENT`; exceeded caller bounds and owned-allocation failures return
`RESOURCE_EXHAUSTED`. No partial product is returned.

The implementation is single-threaded. Lowering is linear in source width, outputs, WHERE leaves,
expression instructions, and order keys. Finalization adds one checked predicate evaluation per
input row, `O(rows log rows * order-expression work)` for computed ordering, and two checked output-
expression evaluations per selected row, plus transformed output bytes. Retained memory is linear
in projected columns, worker and visible outputs, expression configuration, selected computed
cells, one canonical payload arena, two comparison row views, and order keys, all under caller-
configurable bounds.

## Tradeoffs and likely interview questions

**Why not serialize `PhysicalPipelinePlan`?** It contains implementation variants, resource-policy
objects, and process-sized ordinals. The canonical intent keeps the protocol stable and small.

**Why carry the full source row for one computed output or predicate?** The existing checked program
uses bound schema ordinals. Preserving those ordinals makes the coordinator baseline simple and
auditable while leaving Plan Intent unchanged. Projection remapping or worker execution is
worthwhile only with a versioned contract and evidence that the extra bandwidth matters.

**Why evaluate WHERE before LIMIT?** LIMIT counts rows after SQL filtering. Applying it first could
discard later matching rows and return too few or the wrong rows.

**Why evaluate visible expressions after LIMIT?** Order expressions run during the global stable
sort, but visible expressions are evaluated only for selected rows. Rows discarded by LIMIT cannot
affect the visible result, so materializing them would waste transformed-byte capacity.

**Why are ORDER BY and LIMIT global?** A tablet-local limit can discard a row that should win after
merging another tablet. Global finalization is the correctness boundary.

**What owns schema stability?** The bound SELECT retains the catalog snapshot used for lowering;
the product copies durable identities and descriptors, and later fragment binding independently
matches them against pinned TabletState publications.
