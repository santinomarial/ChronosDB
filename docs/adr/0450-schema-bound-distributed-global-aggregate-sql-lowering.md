# ADR 0450: Schema-bound distributed global aggregate SQL lowering

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB SQL, query-planning, distributed-query, and service maintainers
- **Extends:** [ADR 0041](0041-bound-global-aggregate-physical-lowering.md),
  [ADR 0352](0352-canonical-distributed-vector-plan-intent.md),
  [ADR 0383](0383-owned-cross-tablet-vector-aggregate-definitions.md),
  [ADR 0439](0439-schema-bound-distributed-row-sql-lowering.md)

## Context

The distributed vector stack already binds, executes, transports, merges, and Native-encodes
ungrouped sufficient aggregate state. Native SQL could not construct its schema-neutral plan,
projection, predicate, or result descriptors, so an embedding still had to manufacture those values.
Reusing local physical lowering would admit computed inputs and final expressions that the remote
aggregate worker does not execute.

## Decision

`lower_bound_sql_select_to_distributed_vector_aggregate` accepts one already bound current-table
global aggregate query. Every visible output must be exactly one of `COUNT(*)`, `COUNT(column)`,
`SUM(column)`, `AVG(column)`, `MIN(column)`, `MAX(column)`, `VAR_POP(column)`, or
`VAR_SAMP(column)`. Non-star inputs must be direct columns from that source. Projection ordinals are
deduplicated in first-input order, while aggregate definitions and result descriptors retain SQL
output order and repetition.

The product carries exact table/schema identity, the same normalized event-time predicate accepted
by row lowering, ungrouped Plan Intent, global LIMIT, and binder-owned names/types/nullability.
Lowering independently derives each aggregate kernel shape and validates the complete descriptor
vector against projected source shapes. A query containing only `COUNT(*)` projects the event-time
column as a bounded fragment anchor because the existing authority-bound fragment format requires a
nonempty projection; the count definition still has no input.

GROUP BY, ORDER BY, historical reads, LATEST/ASOF/multiple sources, computed aggregate inputs,
computed final expressions, and arbitrary predicates fail closed. ORDER BY is not silently removed
even though one global group ordinarily produces at most one pre-LIMIT row, because evaluating an
order expression may itself have observable failure semantics.

## Consequences

Bound SQL can now produce the exact authority-neutral input consumed by the implemented compatible
aggregate snapshot and transport stack. This change does not yet route Native aggregate requests or
package the aggregate carrier in `chronosd`; that process integration remains a separate lifetime
and endpoint decision.

Work and retained memory are linear in source width, aggregate outputs, and WHERE leaves. Projection,
aggregate, and UTF-8 result-name widths have positive caller-configurable bounds below protocol hard
limits. The function is single-threaded, so no memory-ordering argument applies.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): table/schema identity and descriptor shape are copied
  from one bound catalog snapshot and later require fresh authority binding.
- [Invariant 13](../architecture/invariants.md): event-time endpoints preserve exact nanosecond and
  inclusive/exclusive semantics.
- [Invariant 15](../architecture/invariants.md): projection, aggregate, name, and allocation growth is
  bounded before publication.
- [Invariant 18](../architecture/invariants.md): unsupported local-only semantics return a
  source-spanned error instead of weakening the distributed query.

## Validation

Focused tests cover mixed and repeated aggregate inputs, `COUNT(*)` anchoring, inclusive event-time
filtering, LIMIT zero/one, result identity, unsupported semantics, and every caller bound. Allocation
injection classifies every owned failure as resource exhaustion. All 404 query tests and all 53 query
allocation-failure tests pass normally; all 404 query tests plus both changed allocation paths pass
under ASan/UBSan. Formatting, LLVM 18 static analysis with no diagnostics in the changed target, and
the installed external-consumer test pass. TSan is not applicable because lowering is a pure
single-threaded construction with no shared state.

## Migration and rollback

The API is additive and changes no durable or network bytes. Rollback removes the lowering entry
point without changing local SQL or existing hand-built distributed aggregate execution.

## References

- [Distributed Vector Plan Intent v1](../formats/distributed-vector-plan-intent-v1.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Distributed aggregate SQL lowering](../learning/distributed-aggregate-sql-lowering.md)
