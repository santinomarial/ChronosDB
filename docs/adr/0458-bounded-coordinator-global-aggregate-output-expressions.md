# ADR 0458: Bounded coordinator global-aggregate output expressions

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB SQL, query, cluster, replicated-service, and Native Protocol maintainers
- **Extends:** [ADR 0035](0035-bounded-checked-vector-expression-programs.md),
  [ADR 0386](0386-native-vector-aggregate-result-finalization-v2.md),
  [ADR 0450](0450-schema-bound-distributed-global-aggregate-sql-lowering.md), and
  [ADR 0456](0456-bounded-coordinator-global-aggregate-predicates.md)

## Context

Distributed global aggregate SQL could return only direct aggregate calls. The merge-state and
Native finalizers already produced exact owned scalars, while the checked vector-expression engine
already implemented the scalar arithmetic, NULL, text-transform, and short-circuit semantics needed
for final expressions. Sending final expression programs to workers would be incorrect because no
worker owns the globally merged values and would add a new wire format without a measured need.

## Decision

Lowering collects every supported aggregate call nested in the visible SELECT expressions and
retains one exact internal definition and output shape per occurrence. Aggregate inputs remain
direct source columns or COUNT(*); computed aggregate inputs still fail closed. A new post-aggregate
lowering entry maps those aggregate expression spans to finalized internal column ordinals while
making base-table columns unavailable. Each nonidentity visible output becomes one checked
`VectorExpression` plus the exact client result schema.

The aggregate finalizer first revalidates the raw Plan Intent, definitions, internal schema, and
owned scalar shapes. It then canonical-encodes the single raw row, validates every expression source
against that schema, evaluates fixed-width outputs into owned scalars, and copies transformed
STRING/SYMBOL/BINARY bytes into owned results. Projection evaluation happens before LIMIT, so a
runtime error remains observable even when LIMIT is zero. Only the complete projected row is passed
to Native encoding.

Selected-output ORDER BY is accepted because it cannot reorder the one global row and its selected
expression is already evaluated. Hidden ORDER BY expressions remain unsupported because they would
need additional state or separately observable expression evaluation.

Expression instructions, aggregate count, result width and names, raw canonical bytes, transformed
outputs, schema copies, vector capacities, and Native payloads remain under finite lowering and
finalization limits. Owned allocation failure and arithmetic overflow abort before publication. No
durable, wire, authority, retry, cancellation, or acknowledged-write contract changes.

## Consequences

Replicated distributed SQL can execute outputs such as `count(*) + 1`, `sum(value) * 2`, and
`upper(coalesce(min(tag), 'none'))`, including a general coordinator WHERE predicate and a
selected-alias ORDER BY. Repeated aggregate occurrences retain separate state, matching the existing
definition identity contract rather than adding an unproved deduplication mapping.

Work is `O(rows * aggregate calls + aggregate calls + final expression instructions)` and final
projection memory is linear in aggregate/output width plus variable payload bytes. The coordinator
path is synchronous and thread-affine; no new memory-ordering argument applies.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): aggregate bindings, internal shapes, and visible
  expressions derive from one bound catalog snapshot.
- [Invariant 11](../architecture/invariants.md): canonical raw and borrowed transformed bytes remain
  owned through synchronous evaluation, and published outputs own their payloads.
- [Invariant 14](../architecture/invariants.md): the in-memory projection changes no versioned
  fragment, aggregate-state, exchange, or Native format.
- [Invariant 15](../architecture/invariants.md): definitions, programs, schemas, canonical storage,
  projected values, and encoded output have explicit finite bounds.
- [Invariant 18](../architecture/invariants.md): checked scalar semantics and pre-LIMIT errors are
  preserved instead of silently dropping final expressions or ORDER BY clauses.

## Validation

Lowering tests cover mixed numeric/text final expressions, aggregate span binding, direct inputs,
selected-output ORDER BY, hidden-order rejection, expression bounds, and allocation injection.
Finalizer tests cover fixed and transformed variable outputs, source-shape validation, runtime
division failure before LIMIT zero, combined row predicate and output projection, working memory,
header consumption, and allocation injection. The two-tablet service integration requires identical
computed aggregate Native payloads through remote mutual TLS and co-located workers.

The normal query, cluster, and service suites pass with 410, 204, and 106 tests; their allocation-
failure suites pass with 56, 27, and 3 tests. Focused ASan/UBSan runs pass lowering, allocation
injection, raw and row-backed projection, and the remote/local service integration with leak
detection disabled. All five changed production sources pass LLVM 18 clang-tidy without user-code
diagnostics using the LLVM 18 libc++ headers required by the macOS 26 SDK combination. Formatting
and diff checks and the installed public-target consumer pass.

## Migration and rollback

The new bindings and coordinator projection are pre-alpha in-memory APIs. Rollback rejects computed
final outputs and removes the projection overloads without changing stored or network bytes. Direct
aggregate outputs retain their previous identity path.

## References

- [Distributed aggregate SQL lowering](../learning/distributed-aggregate-sql-lowering.md)
- [Mutable-row aggregate finalization](../learning/mutable-row-aggregate-finalization.md)
- [Checked vector expression programs](../learning/vector-expression-programs.md)
