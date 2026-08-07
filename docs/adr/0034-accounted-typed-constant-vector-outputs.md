# ADR 0034: Accounted Typed-Constant Vector Outputs

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-execution and resource-accounting maintainers

## Context

ADR 0033 established an owned, query-accounted output boundary for reordered and duplicate source
columns. SQL result positions also contain constants. Sending those constants through the scalar
reference executor once per row would allocate variable-width scalar storage on the vector data
path, hide the exact output size until construction, and duplicate the canonical physical encoding
rules. The physical plan additionally needs to propagate the exact type and nullability of a mixed
source/constant output list before pulling its source.

## Decision

- `ColumnOutputPosition` is a closed variant of `SourceColumnOutputPosition` and
  `ConstantColumnOutputPosition`. The latter owns one `ScalarValue` and must be typed; untyped NULL
  is invalid at both operator and plan construction.
- `ColumnOutputOperator` accepts a caller-ordered, bounded position vector. Source positions retain
  ADR 0033 reorder/duplicate semantics. A non-NULL constant produces a nonnullable physical column;
  a typed NULL produces an all-NULL nullable column of its exact logical type.
- Constants are expanded directly into canonical Boolean, fixed-width little-endian, UUID,
  validity, offset, and value buffers. Variable payload multiplication and 32-bit terminal offsets
  are checked before reservation. No scalar object or heap allocation is created per row.
- Nonempty sparse selections compact to one identity output domain. Empty selections remain progress
  over the input physical domain, so constant columns use that same domain while exposing zero rows.
- Planning computes the complete output buffers, owners, selection, and conservative allocation
  overhead with checked arithmetic. Output credit is admitted while input credit remains live and
  before output allocation.
- The position vector, including nested string/binary constant capacity, is finitely bounded.
  `ColumnOutputStage` carries the same representation in `PhysicalPipelinePlan`; shape propagation
  gathers source shapes and derives constant shape directly from the typed scalar.
- The specialized `SourceColumnOutputOperator` and `SourceColumnOutputStage` remain supported. This
  increment does not rewrite ADR 0033 or force existing consumers through the broader interface.

This decision adds no durable representation, SQL lowering, arithmetic/function expression,
optimizer rule, aggregation, join, scheduling, spill, or hidden row-version behavior.

## Detailed rationale

A typed scalar is a compact immutable description of a repeated vector. One canonical buffer build
is easier to audit than repeated scalar evaluation and lets the admission pass know exact bytes.
Keeping source and constant positions in one ordered variant matches SQL result ordering without
inventing column identities for computed data. Typed NULL is required because a physical vector
cannot derive a type from `std::monostate`.

## Alternatives considered

- **Evaluate a constant through the scalar executor for every row:** rejected because it adds
  avoidable per-row work and variable-width allocation while obscuring pre-allocation bounds.
- **Add constants to `SourceColumnOutputOperator`:** rejected because its narrow API and accepted
  decision remain useful and source-compatible; a separate general operator makes the new contract
  explicit.
- **Borrow one scalar payload as a dictionary vector:** deferred because the canonical vector model
  currently has no dictionary encoding or shared scalar backing and its lifetime/credit would need
  a separate decision.
- **Permit untyped NULL and infer from another stage:** rejected because physical plans must carry
  exact shape independently of surrounding SQL syntax.

## Consequences

Mixed result positions now have deterministic owned physical bytes and exact plan shapes. Repeated
variable constants copy their payload once per materialized row, so memory use is proportional to
the canonical output rather than the scalar description. Input and output reservations coexist for
the pull. General computed expressions and bound-SQL lowering remain separate work.

## Affected invariants

This decision supports invariants [6, 11, and 18](../architecture/invariants.md): every output is
derived from one checked input chunk and typed constant list, ownership and resource lifetime are
explicit, and the new transformation has deterministic, allocation-failure, fuzz, sanitizer, and
measurement evidence.

## Validation plan

- Unit and deterministic property tests cover mixed order, sparse and empty selections, all 18
  frozen logical types, typed NULL, exact scalar round trips, invalid configuration, foreign query
  ownership, limits, and physical-plan shapes.
- Allocation-failure injection exhausts factory and mixed fixed/variable/NULL pull allocations and
  requires `RESOURCE_EXHAUSTED`, cancellation, and complete credit release.
- `chronos_physical_plan_fuzz` drives hostile typed/untyped constant stages and valid end-to-end
  mixed output execution.
- `materialize_mixed_source_and_typed_constants` measures fixed, string, typed-NULL, and source
  positions with source construction excluded.
- Header, installation, and external-consumer checks cover the public API.

## Migration or rollback considerations

There is no persisted or network state. Rollback removes the general operator/stage while leaving
ADR 0033 source-only output intact. A replacement must preserve canonical bytes, exact types,
empty-progress semantics, bounded configuration, pre-allocation admission, and cleanup behavior.

## Unresolved questions

Computed expressions, vector expression fusion, bound-SQL lowering, alias/dictionary outputs,
zero-row physical vectors, reservation transfer, hidden row versions, complete part/head merge,
parallel scheduling, and spill remain later Phase 9 decisions.

## References

- [ADR 0020](0020-bounded-vector-chunk-representation.md)
- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0022](0022-pull-based-physical-operator-lifecycle.md)
- [ADR 0023](0023-bounded-physical-pipeline-plan.md)
- [ADR 0033](0033-accounted-source-column-output-materialization.md)
- [Typed-constant output guide](../learning/typed-constant-output-materialization.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
