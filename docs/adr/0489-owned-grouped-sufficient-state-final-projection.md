# ADR 0489: Owned grouped sufficient-state final projection

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB SQL, query, distributed-execution, replicated-service, and Native protocol
  maintainers
- **Extends:** [ADR 0484](0484-bounded-grouped-sufficient-state-native-finalization.md),
  [ADR 0487](0487-direct-grouped-sufficient-state-sql-lowering.md), and
  [ADR 0488](0488-coherent-replicated-grouped-sql-preparation.md)

## Context

The sufficient-state path could exchange and merge exact raw group keys and aggregate values, but
its SQL lowerer required those raw columns to be the client result in the same order. Expressions
such as `SUM(value) + 1`, reordered or omitted keys, and expressions over multiple aggregate
results therefore fell back to complete-row exchange even though they execute safely after global
group merging. Applying them at workers would be incorrect because aggregate final values do not
exist until all tablet states have merged.

## Decision

Split grouped sufficient-state output into two owned contracts:

1. the versioned distributed intent and raw schema remain keys followed by sufficient aggregate
   values;
2. an optional in-memory coordinator projection owns one checked `VectorExpression` per visible
   output, the client result schema, selected-output order keys, and global limit;
3. raw plans with a coordinator projection carry no order keys or limit, preventing either
   operation from running against the wrong column namespace;
4. finalization validates every expression input ordinal and physical shape against the raw schema,
   materializes the complete projected group set under query and output limits, then applies global
   sort and limit before Native encoding; and
5. the scheduler and replicated SQL constructor move that projection with the pinned execution
   owner through all-tablet closure and atomic result publication.

SQL lowering still requires direct, unique group keys and direct aggregate inputs. It collects the
raw key/aggregate vector independently of visible SELECT order, lowers visible expressions only
against those finalized columns, and continues to reject hidden ORDER BY expressions. Computed
pre-group keys and aggregate inputs remain on the row-backed correctness path.

## Detailed rationale

The split preserves SQL failure timing: arithmetic, casts, and scalar functions over aggregate
results execute once per globally merged group, not once per source row or partial tablet state. It
also reuses the checked physical expression, projection, sort, limit, accounting, and Native batch
machinery instead of adding a second scalar evaluator or changing the grouped wire format.

Keeping the projection in memory is sufficient for the current coordinator-owned lifecycle. It is
never interpreted by a remote worker, so adding it to a durable or network format would increase
compatibility surface without enabling a current use.

## Alternatives considered

- **Evaluate final expressions on each worker:** rejected because nonlinear expressions over
  partial aggregates do not generally compose and would change error timing.
- **Extend the grouped exchange with visible rows:** rejected because raw sufficient states are the
  merge authority and the final projection is coordinator-local.
- **Continue complete-row fallback for every final expression:** correct but needlessly prevents
  the implemented sufficient-state path from serving expressions that depend only on finalized
  keys and aggregates.
- **Defer the decision:** rejected because the existing raw/final boundary and shared checked
  operators make the ownership rule explicit without constraining computed pre-group work.

## Consequences

Computed, reordered, and omitted final outputs can use the sufficient-state path. Ordering and
limit now address the client projection exactly. The raw result schema remains internal and may use
non-client names. Projection materialization adds bounded coordinator work and memory proportional
to the merged group count; no performance improvement is claimed without measurement.

Computed pre-group expressions, hidden ORDER BY expressions, mutable `TabletState` sufficient-state
workers, and partitioned shuffle routing remain separate work. The owner is single-thread-affine;
this change introduces no shared-memory algorithm or new memory-ordering requirement.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): raw schema, checked expression inputs, and client
  schema are validated as one owned product.
- [Invariant 13](../architecture/invariants.md): final expressions run only after complete global
  sufficient-state merge.
- [Invariant 14](../architecture/invariants.md): no durable or network bytes change.
- [Invariant 15](../architecture/invariants.md): expression, projection, sort, output-row, batch, and
  encoded-byte bounds fail before result publication.
- [Invariant 18](../architecture/invariants.md): unbound source references and mixed raw/projected
  ordering fail closed.

## Validation plan

Focused lowering tests cover computed aggregate expressions, reordered keys, omitted raw outputs,
selected-output ordering, limit ownership, identity-plan preservation, unsupported computed inputs,
and every new limit. Finalizer tests evaluate checked arithmetic before projected sort/limit and
decode the exact Native schema and cells. A loopback mutual-TLS scheduler test proves all-tablet
merge precedes the moved projection. Replicated construction tests accept a valid owned projection
and reject a raw plan that also attempts to own limit. Allocation injection sweeps computed SQL
lowering; sanitizer, full-suite, formatting, static-analysis, and diff evidence are recorded with
the implementing change.

## Migration or rollback considerations

This is an additive pre-alpha in-memory API. Existing identity plans retain their raw result schema,
order, and limit contract. Rollback removes optional projection ownership and restores explicit
row-backed routing for non-identity final outputs; no stored or transmitted data needs conversion.

## Unresolved questions

Partition ownership and skew policy for a future shuffle remain unresolved. Computed pre-group
program transport must choose a separately versioned worker contract before implementation.

## References

- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Distributed Vector Plan Intent v1](../formats/distributed-vector-plan-intent-v1.md)
- [Distributed Vector Grouped Aggregate Query Transport v2](../formats/distributed-vector-grouped-aggregate-query-transport-v2.md)
