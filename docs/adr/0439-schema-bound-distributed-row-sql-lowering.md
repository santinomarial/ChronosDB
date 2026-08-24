# ADR 0439: Schema-bound distributed row SQL lowering

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB SQL, query-planning, distributed-query, and service maintainers
- **Extends:** [ADR 0036](0036-bound-select-to-physical-pipeline-lowering.md),
  [ADR 0352](0352-canonical-distributed-vector-plan-intent.md),
  [ADR 0438](0438-committed-mutable-query-route-composition.md)

## Context

Proof-bound mutable fragments, multi-tablet execution, global row finalization, and committed route
composition accepted a canonical `DistributedVectorPlanIntent`, projection ordinals, event-time
predicate, and result schema. Native SQL had no checked translation into those values. Reusing the
local `PhysicalPipelinePlan` would silently admit expressions, aggregation, joins, and other
operators that the mutable row worker cannot execute, while serializing that implementation plan
would violate the schema-neutral network contract.

## Decision

`lower_bound_sql_select_to_distributed_vector_rows` lowers one already bound, current, single-table
row SELECT into an owned `DistributedVectorRowsSqlPlan`. It accepts:

- direct source-column outputs, including star expansion and repeated columns;
- an optional conjunction of `<`, `<=`, `=`, `>=`, or `>` comparisons between the schema's exact
  event-time column and bound `TIMESTAMP` literals, in either operand order;
- `ORDER BY` keys that resolve to visible direct-column outputs; and
- an optional global 64-bit `LIMIT`, including zero.

The lowering deduplicates the projected source ordinals in first-output-use order and maps repeated
outputs back through row-output indices. Event-time comparisons normalize to the tightest exact
open/closed lower and upper bounds without endpoint arithmetic. Repeated order keys are removed
because later comparisons of the same value cannot refine the earlier key. Default NULL placement
matches local physical lowering. The result schema retains the binder's exact names, logical types,
and nullability and is independently revalidated against the canonical intent and projected source
shape before publication.

Explain/subscribe modes, historical reads, LATEST BY, ASOF or multiple sources, grouping,
aggregation, computed output expressions, arbitrary predicates, and ORDER BY expressions absent
from the visible output fail with source-spanned `NOT_SUPPORTED` diagnostics. There is no local
fallback inside this API. Caller limits can lower projection, output, order-key, and result-name
bounds; allocation and container failures are classified as `RESOURCE_EXHAUSTED`.

## Consequences

The schema identity, projection, filter truth, final order/limit, and result descriptors can now
flow directly into replicated mutable fragment binding without reconstructing SQL semantics. Query
identity, read policy, tablet/group authorities, route/TLS authority, scheduler ownership, and
Native response publication remain service-layer responsibilities.

Lowering is single-threaded and performs `O(source columns + outputs + WHERE leaves + order keys)`
work with bounded owned vectors. It introduces no synchronization algorithm, dependency, durable
format, or network byte change.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): the product retains the exact bound table/schema
  identity and validates output shape before publication.
- [Invariant 13](../architecture/invariants.md): event-time bounds preserve exact open/closed
  nanosecond semantics without overflow-prone endpoint adjustment.
- [Invariant 15](../architecture/invariants.md): every retained collection and result name has a
  positive hard and caller-configurable bound.
- [Invariant 18](../architecture/invariants.md): unsupported SQL fails closed instead of weakening
  the query or invoking a scalar fallback.

## Validation

Focused tests prove repeated projection mapping, exact predicate normalization in both operand
orders, default and explicit NULL placement, duplicate order-key removal, LIMIT, star expansion,
unsupported semantic rejection, and caller-bound failures. A complete allocation sweep requires
every owned-allocation failure to return a resource diagnostic. Header self-containment, installed
external consumption, sanitizers, formatting, static analysis, and the full query suite are release
gates.

## Migration and rollback

The API is additive and not yet installed in the Native request lifecycle. Rollback removes the
lowering product and function without changing accepted SQL syntax, durable data, or network bytes.

## References

- [Bound SELECT physical lowering](../learning/bound-select-physical-lowering.md)
- [Distributed Vector Plan Intent v1](../formats/distributed-vector-plan-intent-v1.md)
- [Bounded global vector row finalization v2](0379-bounded-global-vector-row-finalization-v2.md)
