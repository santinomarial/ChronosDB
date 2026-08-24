# ADR 0449: Hidden distributed row order outputs

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB SQL, query, cluster, and distributed-protocol maintainers
- **Extends:** [ADR 0352](0352-canonical-distributed-vector-plan-intent.md),
  [ADR 0439](0439-schema-bound-distributed-row-sql-lowering.md)

## Context

SQL permits ordering by a direct source column that is absent from the visible `SELECT` list. The
distributed row protocol previously indexed order keys only into visible worker outputs, so it
rejected this query shape. Applying an uncarried order key at the coordinator is impossible, while
publishing the helper column would violate the SQL result schema.

## Decision

Distributed Vector Plan Intent 1.1 adds an optional bounded visible-row-output vector. Row outputs
continue to describe the complete worker stream. A nonempty visible vector contains unique indices
into that stream in final client-column order; an empty vector retains minor-0 all-visible
semantics. The existing 16-bit reserved header field becomes the visible count in minor 1, and the
descriptors follow row outputs. Minor-0 bytes are unchanged and both minors decode.

Distributed SQL lowering may append an unselected direct `ORDER BY` source column to the unique
fragment projection, worker row outputs, and worker result schema. It records the original SELECT
positions as visible and points the global order key at the helper output. Repeated order references
reuse one helper. Computed and relational order expressions remain unsupported.

The global row finalizer validates and decodes the complete worker schema, sorts and limits against
complete outputs, then sizes and encodes only the visible vector. Zero-row results likewise publish
only the visible schema. Output limits apply to visible columns; input and working limits continue
to cover hidden bytes and cells.

## Consequences

Queries such as `SELECT label FROM events ORDER BY ts` now execute across all tablets without
exposing `ts`. Worker execution, authentication, snapshot proofs, and result-exchange bytes retain
their existing contracts; nested plan/fragment maximum sizes grow by the bounded descriptor vector.
Minor-0 peers reject minor 1 as unsupported rather than misinterpreting it.

This does not add computed order keys, arbitrary expressions or predicates, aggregation, grouping,
joins, or historical reads. Hidden columns still consume projection, worker-output, transport,
coordinator, and memory budgets.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): hidden order data is carried under the same exact
  snapshot, schema, and fragment authority as visible rows.
- [Invariant 10](../architecture/invariants.md): minor 1 has fixed-width descriptors, bounded counts,
  and both header and full-frame integrity checks.
- [Invariant 14](../architecture/invariants.md): minor-0 bytes remain exact and unknown minors fail
  before interpretation.
- [Invariant 15](../architecture/invariants.md): helper projections and visible descriptors share
  existing hard and caller-configurable bounds.
- [Invariant 18](../architecture/invariants.md): hidden cells cannot appear in the client schema and
  no tablet-local order or limit is inferred.

## Validation

Codec tests round-trip minor 0 and minor 1 and reject duplicate or out-of-range visibility.
Lowering tests prove helper projection, duplicate-key reuse, visible schema identity, and output
limits. Finalization tests order by a hidden value while publishing one visible column. The real
two-tablet replicated Native test executes the query through the production mutable worker and
observes only the selected descriptor. The normal query, cluster, service, and allocation-failure
suites pass (401, 192, 106, 52, 25, and 3 tests respectively). ASan/UBSan passes all 401 query and
192 cluster tests plus the changed allocation-failure paths and the real two-tablet service path.
Formatting, static analysis, the installed external-consumer test, and the `chronosd` CLI test pass;
LLVM 18 reports only the repository's existing missing-field-initializer warnings.

## Migration and rollback

Writers emit minor 1 only when hidden row outputs are required. Rolling back preserves all existing
minor-0 plans but makes such SQL fail closed again. No stored database or Raft format changes.

## References

- [Distributed Vector Plan Intent v1](../formats/distributed-vector-plan-intent-v1.md)
- [Distributed row SQL lowering](../learning/distributed-row-sql-lowering.md)
- [Bounded global vector row finalization v2](0379-bounded-global-vector-row-finalization-v2.md)
