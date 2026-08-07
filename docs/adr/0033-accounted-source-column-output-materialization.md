# ADR 0033: Accounted Source-Column Output Materialization

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-execution and resource-accounting maintainers

## Context

The existing stable column-subset operator can discard an ordered unique set of columns without
allocating, but SQL output positions may reorder or repeat source columns. Those operations cannot
move-compact one set of owners in place: each duplicate output needs a distinct stable physical
owner, and a sparse input selection must become a canonical result domain before later computed
expressions can write positionally.

The physical pipeline also requires a strict memory boundary. The input chunk and its reservation
remain live while output sizes are inspected and copied, so output credit must be admitted before
the first output allocation and must cover the simultaneous peak. This increment establishes that
ownership and accounting baseline without prematurely choosing a general expression bytecode,
constant representation, or bound-SQL lowering.

## Decision

- `SourceColumnOutputOperator` accepts one unique child, a caller-ordered vector of input physical
  ordinals, and finite `VectorChunkLimits`. Ordinals may be reordered and repeated; an empty output
  list preserves row cardinality.
- The retained ordinal configuration is bounded by 4,096 entries, including spare vector capacity.
  Each pull validates every ordinal against the actual chunk before output allocation.
- A nonempty input selection is stable-compacted into a new physical row domain with an identity
  selection. Every selected source cell is copied into canonical owned buffers while preserving
  logical type parameters, nullability, NULL state, row order, and cell bytes or Boolean value.
- An empty input selection remains a progress chunk over its original nonzero physical domain.
  Output columns copy that complete domain and the output selection stays empty. This preserves the
  vector invariant that physical row domains are nonzero and avoids inventing inaccessible column
  contents.
- Repeated output ordinals create independent `OwnedPhysicalColumn` buffers. No output borrows the
  input chunk, and no durable `ColumnId` enters the physical representation.
- Planning uses checked arithmetic to compute selection, validity, offset, value, container, and
  conservative allocator costs. Logical and retained limits are checked and query output credit is
  reserved before any output buffer or selection allocation. The input credit remains live until
  output construction succeeds or fails.
- Allocation and container-limit failures are `RESOURCE_EXHAUSTED`; missing runtime ordinals are
  `OUT_OF_RANGE`; invalid limits, null children, and foreign query ownership are rejected. Any child
  or local pull error requests cooperative cancellation, and RAII releases both input and partial
  output credit.
- `SourceColumnOutputStage` integrates the operation into `PhysicalPipelinePlan`, whose sequential
  validation propagates exact reordered and duplicate type/nullability shapes.

This decision adds no durable bytes, schema identity, dependency, concurrency algorithm, constant
or computed expression, SQL lowering, optimizer rule, aggregation, join, scheduling, or spill.

## Detailed rationale

Deep materialization is the simplest correct ownership baseline. Alias views would require a new
shared-backing ordinal map and would keep the complete input reservation alive, while later
computed expressions still need owned output buffers. Copying selected rows also converts arbitrary
sparse selections into one positional domain suitable for a future typed output builder.

Planning variable-width values by reading validated immutable cells costs one extra pass but makes
the exact allocation bound known before reservation. It avoids speculative growth and makes
budget rejection independent of allocator behavior. Conservative allocator allowances may
overcharge, but `AccountedVectorChunk` verifies that the charge never undercounts retained buffers.

## Alternatives considered

- **Extend stable subset projection to reorder and duplicate owners:** rejected because one move-only
  owner cannot occupy multiple output positions and reordering would invalidate the in-place stable
  compaction contract.
- **Return aliasing views into the input chunk:** deferred because it needs an explicit shared pin
  and credit-transfer contract and does not establish writable owned outputs for expressions.
- **Implement the complete typed expression builder now:** deferred until expression evaluation,
  NULL propagation, output type derivation, and bound-plan lowering are specified together.
- **Drop empty progress chunks:** rejected because doing so can truncate later input chunks.
- **Construct zero-row vectors for empty selections:** rejected because the accepted vector model
  requires a nonzero physical row domain.

## Consequences

Physical plans can now produce arbitrary source-column order and duplicate positions with exact
owned lifetimes. Sparse nonempty outputs improve locality, but every requested cell is copied and
duplicate outputs amplify bytes. During a pull, input and output reservations coexist by design.

Empty selected chunks may copy values that no selected-row API can observe. This is intentionally
conservative and expected to be uncommon; a future explicit zero-row vector representation would
require a separate decision across all operators.

## Affected invariants

This decision supports invariants [6, 11, and 18](../architecture/invariants.md). Output cells are
copied only from one owning input snapshot chunk; the input reservation and backing remain live
through the last read; and the materialization transformation is covered by scalar-cell properties,
failure injection, sanitizers, fuzzing, and isolated measurement before any optimization replaces
it.

## Validation plan

- Unit tests cover reorder, duplication, independent buffers, fixed/Boolean/variable and nullable
  values, sparse compaction, empty progress, zero-column cardinality, sticky end, limits, foreign
  ownership, and exact physical-plan shape propagation.
- A deterministic property covers all 18 frozen logical type codes and compares selected cells
  under reverse-order and duplicate output.
- Allocation-failure injection exhausts factory and pull allocations and requires
  `RESOURCE_EXHAUSTED`, shared cancellation, and complete credit release.
- The physical-plan fuzzer varies hostile output ordinals and limits and executes duplicate outputs
  after filtering and LIMIT under sanitizers.
- `materialize_reordered_duplicate_source_columns` measures dense and sparse selected rows, multiple
  widths, output bytes, and pull allocations with source construction excluded.
- Self-contained header, installation-layout, and external-consumer tests cover the public API.

## Migration or rollback considerations

There is no persisted or network state. Rollback removes the operator and stage. A replacement must
retain exact cell/type/null semantics, empty-progress behavior, pre-allocation query admission,
independent lifetime or an equally explicit shared-pin proof, and deterministic cleanup.

## Unresolved questions

Typed constants and computed expressions, bound-SQL lowering, expression fusion, alias-backed
outputs, zero-row physical vectors, reservation transfer/shrinking, hidden row versions, complete
part/head merge, parallel scheduling, and spill remain later Phase 9 decisions.

## References

- [ADR 0020](0020-bounded-vector-chunk-representation.md)
- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0022](0022-pull-based-physical-operator-lifecycle.md)
- [ADR 0023](0023-bounded-physical-pipeline-plan.md)
- [Physical operator foundation](../learning/physical-operator-foundation.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
