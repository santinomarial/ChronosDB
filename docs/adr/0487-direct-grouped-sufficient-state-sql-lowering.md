# ADR 0487: Direct grouped sufficient-state SQL lowering

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB SQL, query, distributed-execution, and Native protocol maintainers
- **Extends:** [ADR 0459](0459-bounded-row-backed-distributed-grouped-sql.md) and
  [ADR 0486](0486-replicated-grouped-sufficient-state-preparation.md)

## Context

The grouped sufficient-state worker, transport, scheduler, and Native finalizer accepted a typed
`DistributedVectorPlanIntent`, but schema-bound SQL had no safe way to produce that intent. The
existing grouped SQL lowerer intentionally sent complete rows to the coordinator and preserved all
computed SQL semantics through the ordinary physical pipeline. Reconstructing a sufficient-state
intent from that physical plan outside the binder would lose source-column identity and could move
fallible expressions across the grouping boundary.

## Decision

Add a separate schema-bound lowerer for the direct sufficient-state subset. It accepts exactly one
current table with nonempty `GROUP BY` and requires:

1. every group key is one unique direct source column;
2. those keys are the first selected outputs in exact `GROUP BY` order;
3. every remaining output is one direct COUNT, SUM, AVG, MIN, MAX, VAR_POP, or VAR_SAMP call;
4. aggregate inputs are direct source columns, except COUNT(*);
5. WHERE is absent or is an exact event-time range;
6. ORDER BY names selected key or aggregate outputs; and
7. projection, key, aggregate, order, and result-name widths remain within caller and wire bounds.

The result owns the exact table/schema identity, unique source projection, remapped key and
aggregate indices, event-time predicate, global order/limit intent, and typed named result schema.
Kernel output shape and the complete result schema are independently validated before success.

Computed, reordered, omitted, or hidden expressions return `NOT_SUPPORTED`. That result is a
deliberate routing signal: the caller may invoke the established row-backed grouped lowerer, which
remains the semantic oracle. No implicit fallback occurs inside either lowerer.

[ADR 0488](0488-coherent-replicated-grouped-sql-preparation.md) subsequently consumes this product
and derives its complete table plan from one catalog, Manifest, and acquired authority publication.

## Consequences

Direct grouped SQL can now enter the existing multi-key/all-type sufficient-state execution path
without embeddings reconstructing bound column ordinals or output types. Repeated aggregate inputs
and keys share one projected source column while result outputs retain SQL order and names.

The restricted contract exchanges less data than the row-backed baseline, but this decision makes
no performance claim. Computed pre-group expressions and computed final projections need an owned
two-stage split with preserved failure timing; mutable-TabletState execution and partitioned
shuffle routing remain separate work.

The lowerer is synchronous and publishes no shared state, so no new memory-ordering argument
applies. It adds no durable or network format.

## Validation

Focused tests cover direct nullable multi-key plans, COUNT(*), SUM and MIN, exact event-time range,
duplicate selected-output ordering, global LIMIT, projection reuse, and the complete typed result
schema. Rejection tests cover computed/reordered keys, computed aggregate inputs, final
expressions, non-event predicates, hidden order expressions, and every caller width bound.
Allocation-failure injection classifies every owned allocation point as resource exhaustion.

The complete query suite passes 422 tests and the query allocation-failure suite passes 62 tests.
Focused ASan/UBSan, formatting, static analysis, and repository-diff evidence are recorded in the
implementing change rather than inferred here.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): table, schema, projection, plan, and result shape are
  one schema-bound product.
- [Invariant 13](../architecture/invariants.md): direct-input sufficient states preserve the same
  grouped SQL identities as the row-backed oracle.
- [Invariant 14](../architecture/invariants.md): the result targets the existing versioned vector
  plan and result-schema formats without changing their bytes.
- [Invariant 15](../architecture/invariants.md): all allocation-driving widths are validated before
  plan ownership grows.
- [Invariant 18](../architecture/invariants.md): unsupported expressions fail closed instead of
  being moved across a semantic boundary.

## Migration and rollback

This is an additive pre-alpha in-memory API. Rollback removes the direct lowerer and leaves the
row-backed grouped SQL path unchanged.

## References

- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Distributed Vector Fragment v2](../formats/distributed-vector-fragment-v2.md)
- [Distributed Vector Grouped Aggregate Query Transport v2](../formats/distributed-vector-grouped-aggregate-query-transport-v2.md)

## Retrospective note (2026-08-25)

[ADR 0489](0489-owned-grouped-sufficient-state-final-projection.md) extends this direct-input
contract with a checked coordinator-owned final projection. Direct group keys and aggregate inputs
remain required, while computed, reordered, and omitted final SELECT outputs no longer require the
row-backed path.
