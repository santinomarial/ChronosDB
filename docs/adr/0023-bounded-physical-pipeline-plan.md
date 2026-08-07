# ADR 0023: Bounded Physical Pipeline Plan

- **Status:** accepted
- **Date:** 2026-08-06
- **Owners:** ChronosDB query-planning and execution maintainers

## Context

ADR 0008 requires ChronosDB to own physical planning and to differentially validate vectorized
plans. ADRs 0020 through 0022 establish bounded vector chunks, query resource control, and a
thread-affine pull lifecycle. The first operators implement Boolean selection, stable column
subsets, and global LIMIT, but callers can compose them with ordinals that are invalid after an
earlier projection and can supply chunks whose physical type or nullability differs from the
planner's assumption.

Phase 9 needs a first plan object and a validation boundary before storage scan ownership, typed
expression output, optimization, or parallel scheduling are defined. This plan must remain narrow:
turning the current operators into a general logical or SQL planner would claim semantics and
resource contracts the repository does not yet implement.

## Decision

- `PhysicalPipelinePlan` owns an ordered sequence of the currently implemented unary stages:
  Boolean filter, stable unique column subset, and global UINT64 LIMIT. Stage order is execution
  order. The plan performs no reordering, fusion, cost choice, or other optimization.
- Plan input and output shapes contain exact `LogicalType` parameters and nullability without
  durable column identity. Creation validates each stage against the shape produced by its
  predecessor and records the exact final shape. Boolean predicates require BOOL; subsets must be
  in range, unique, and strictly increasing; LIMIT preserves shape.
- Input width, stage count, individual subset width, and retained configuration capacity are
  caller-bounded. Retained bytes include vector capacities for input/output shapes, stages, and
  subset ordinals. Arithmetic overflow and limit violations return `RESOURCE_EXHAUSTED`; semantic
  plan errors return `INVALID_ARGUMENT`; factory-internal allocation failure is translated to
  `RESOURCE_EXHAUSTED`.
- Instantiation uniquely owns a non-null source and inserts a source-shape operator before the
  planned stages. Every produced chunk must belong to the supplied query resource identity and
  match the exact planned column count, type parameters, and nullability. A mismatch destroys the
  chunk and its credit, requests cooperative cancellation, and returns `INVALID_ARGUMENT`.
- An empty stage list is valid and still enforces the source shape. Zero-column shapes retain row
  cardinality through the existing chunk contract.
- A plan is move-only and exposes immutable state. Concurrent const inspection and independent
  instantiation are safe. Each instantiated pipeline remains uniquely owned and thread-affine as
  required by ADR 0022; the plan adds no synchronization or publication primitive.
- Plan/configuration memory is coordinator-owned, finitely bounded build-time state and is not
  charged to `QueryResourceContext`. Instantiated operator-object/configuration allocations are
  also not yet query-budget charged. This is an explicit incomplete allocation-accounting boundary,
  not a claim of fully memory-bounded query execution.

This decision changes no durable or network format and adds no dependency.

## Detailed rationale

Validating shapes once during construction catches ordinal/type mistakes before input is pulled,
while checking the source on every chunk prevents a buggy or future scan adapter from violating the
validated assumption at runtime. Physical shapes deliberately omit `ColumnId`: durable identity
belongs to binding and scan mapping, while intermediate vectors may represent no catalog column.

An explicit ordered pipeline is sufficient to exercise multi-stage, chunk-boundary semantics
against an independent scalar model. Keeping it unary and non-optimizing avoids freezing the later
expression program, scan morsel, join graph, scheduler, or cost model.

## Alternatives considered

- **Continue composing operators directly:** keeps the API small but leaves no checked shape
  transition or object that randomized plan tests can instantiate.
- **Trust source chunks after plan construction:** avoids one per-chunk shape walk, but a scan or
  test source could silently supply a different type/nullability and make operator assumptions
  invalid. Measurement may later justify a safely equivalent validation strategy.
- **Lower bound SQL immediately:** would connect more of the stack, but typed vector expression
  output, storage scans, aggregate operators, and result materialization do not exist yet.
- **Introduce a general operator graph and optimizer:** could model future joins and exchanges, but
  would be speculative before their ownership, scheduling, and cost contracts are known.
- **Charge plan and operator objects to the query budget now:** is the desired end state, but the
  existing reservation API has no operator-state ownership class or growth protocol. Finite limits
  make the present gap explicit without inventing that policy here.

## Consequences

The query library can now validate, retain, inspect, and independently instantiate the first real
multi-stage physical plan. Randomized differential tests can vary stage order, selections, and
chunk boundaries through one supported plan boundary. Runtime source mismatches fail closed and
release accounted chunks.

This is not a SQL physical planner. It has no bound-expression lowering, optimizer, storage scan,
snapshot pin, output builder, aggregate, join, ordering, scheduler, spill, or result sink. Its
configuration and operator objects are finitely bounded but not query-budget charged.

## Affected invariants

This decision supports invariants [6, 9, 10, 11, and 18](../architecture/invariants.md). Planned
types and nullability are enforced at execution input; configuration and stage counts are finite;
invalid shapes cannot become unchecked column access; chunk credit unwinds on failure; and
immutable plan inspection does not publish thread-affine pipelines.

## Validation plan

- Unit tests cover shape propagation, invalid predicate/subset stages, width/stage/configuration
  limits, empty plans, exact runtime source shape, query identity, cancellation, credit release, and
  eager LIMIT release.
- A fixed-seed differential test compares composed filter/subset/LIMIT pipelines with an independent
  scalar row model over randomized values, selections, stage order, limits, and chunk boundaries.
- A coverage-guided harness drives hostile stage configurations and valid end-to-end plan execution
  under the ordinary sanitizer configuration.
- ASan/UBSan, ThreadSanitizer, public-header, installation, and external-consumer checks cover the
  new API. Microbenchmarks measure validation and instantiation across 1, 8, 64, and 256 stages
  without making query-throughput claims.

## Migration or rollback considerations

There is no persisted or network state. The API is pre-alpha and can evolve with source changes.
Replacing the plan requires preserving exact stage order and shapes, failure-driven cancellation,
accounted chunk release, and scalar differential equivalence. Later graph or optimizer plans may
lower into or supersede this unary representation.

## Unresolved questions

Bound-SQL lowering, typed expression programs and output allocation, scan/page ownership and
snapshot pins, full operator-state accounting, plan graphs, optimization rules, metrics,
scheduling, parallel error arbitration, aggregation/join algorithms, ordering, and spill remain
later Phase 9 decisions.

## References

- [ADR 0008](0008-custom-sql-and-vectorized-execution.md)
- [ADR 0020](0020-bounded-vector-chunk-representation.md)
- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0022](0022-pull-based-physical-operator-lifecycle.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
