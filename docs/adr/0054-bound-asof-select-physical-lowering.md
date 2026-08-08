# ADR 0054: Bound ASOF SELECT physical lowering

- **Status:** Accepted
- **Date:** 2026-08-07

## Context

SQL v1 binds ASOF conditions as Boolean syntax plus checked metadata. Equality and timestamp
operands may be computed, equality may use lossless numeric widening, later joins may reference any
prior source, and ASOF LEFT widens the right source's effective nullability. Physical lowering must
retain every source's logical and row-version identity through final ordering without exposing
helpers to clients.

## Decision

`lower_bound_sql_asof_select` lowers one or more bound ASOF joins into `PhysicalAsofPlan`.

- Every source input is its schema-ordinal columns followed by the shared four-column row-version
  suffix.
- Primary-source LATEST BY runs before the first join. Each join condition is decomposed according
  to the binder-accepted shape. Its left/prior and right-only operands are materialized in separate
  checked preparation pipelines. Lossless integer and floating equality operands are explicitly
  cast to their bound common type.
- Join output retains all prior source columns, the new right source columns, every row-version
  suffix, and one match-presence bit. Temporary key and timestamp columns are excluded.
- ASOF LEFT right columns, bound references, star outputs, expression inputs, grouping keys, and
  aggregate inputs use widened nullable shapes.
- WHERE runs after every ASOF join. Aggregation follows WHERE. Final expressions are prepared next,
  ORDER BY sorts before LIMIT, and the visible-column subset removes every identity, match, and
  ordering helper.

For ordered base rows, each source must expose a DEDUP KEY. The total tie identity is source order:
for each non-primary source, match presence (absent before present), then that source's DEDUP key;
after all logical identities, each source contributes WAL ID, record sequence, and row ordinal.
NULL-extended values are considered only after presence has distinguished absence. Aggregate ties
remain the encoded group keys established by ADR 0046.

Unsupported or inconsistent bound shapes fail with a span-bearing diagnostic. Arrival order and
sort stability are never semantic tie breakers.

## Consequences

The vector path now covers the accepted multi-source ASOF SELECT surface over caller-provided
physical sources, including LATEST, WHERE, grouped/global aggregation, alias and non-projected
ORDER BY keys, and LIMIT. Snapshot source instantiation remains a separate storage boundary.

The lowerer duplicates some unary final-stage construction while the unary public API remains
stable. A future internal refactor may share that construction only if it preserves diagnostics and
allocation-failure classification.

## Alternatives rejected

- Evaluating the complete ON predicate after a weaker join would choose the wrong right winner.
- Using bound schema nullability after ASOF LEFT would violate checked physical shapes.
- Sorting joined ties by scan arrival or operator stability would violate SQL identity semantics.
- Dropping source suffixes before final ORDER BY would make commit-position ties unavailable.
