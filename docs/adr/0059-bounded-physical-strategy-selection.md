# ADR 0059: Bounded Physical Strategy Selection

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-planning and execution maintainers

## Context

Phase 9 has exact in-memory and external sort operators plus a bounded parallel merge for
independent pipelines. Selecting them blindly would weaken correctness or admission: external sort
needs finite row/chunk/disk/record bounds and an owned directory, while parallel merge changes
arrival order and is invalid anywhere that order affects SQL truth, floating accumulation, LIMIT,
LATEST, ASOF, or an incomplete tie.

ChronosDB does not yet have trustworthy catalog cardinality statistics. A selector must therefore
distinguish caller-supplied authoritative upper bounds from speculative estimates and preserve the
checked physical plan to which every decision applies.

## Decision

- `OptimizedPhysicalPipelinePlan` consumes and owns one exact `PhysicalPipelinePlan`. Strategy
  decisions cannot be applied to a different stage graph or shape.
- `PhysicalExecutionStatistics` supplies finite source/task work and one exact ordered
  `PhysicalSortStageEstimate` for every `SortStage`. These are admission upper bounds supplied by a
  trusted planner; runtime operator limits remain authoritative if the bounds are wrong.
- In-memory sort is selected whenever the row, logical-output, retained-output, plan, and policy
  bounds all fit. It is the baseline with no I/O. External sort is selected only when in-memory is
  inadmissible and an exact stage-indexed spill capability covers total rows, maximum input chunk,
  spill bytes, record bytes, and keys. No compatible choice returns `RESOURCE_EXHAUSTED`.
- External runtime directories and prefixes are not retained in the reusable plan. Instantiation
  must consume exactly one open target for each selected external stage. The checked stage keys are
  always reused; a target cannot replace ordering semantics.
- Multiple same-shape sources are composed in task order by the query-accounted serial merge.
  Parallel merge is considered only when the caller explicitly declares the complete downstream
  pipeline order-independent, source rows/work exceed finite policy thresholds, bounded workers are
  available, and `ceil(work/workers) + workers*overhead` is below serial work. Ordered work stays
  serial even when parallel capacity exists.
- Sort cost reports saturating comparison units. External selection additionally reports twice the
  declared spill-byte upper bound for write/read I/O. These are deterministic policy units, not
  fabricated time predictions. Thresholds are explicit and benchmarkable.
- Source, sort, capability, scheduler, strategy-configuration, and serial-merge limits are finite.
  Invalid coverage, duplicate stages, null sources, foreign query credit, or missing targets fail
  before weaker execution escapes.
- The snapshot connector accepts an optimized one-source plan. This enables exact bound-SQL
  external ORDER BY over a complete tablet snapshot. It does not split one tablet into parallel
  tasks; such splitting requires a separate exact morsel/ordering contract.

This decision changes no SQL, durable, spill-format, or network semantics and adds no dependency.

## Consequences

ChronosDB now has a small physical strategy selector rather than a general relational rewrite
optimizer. It integrates existing exact operators without reordering predicates, aggregation,
joins, output, or LIMIT. The conservative preference for in-memory sort is appropriate for the
current baseline external merge; later profiles may add another accepted rule without changing SQL
ties or ownership.

The order-independence declaration is a proof obligation, not a hint. SQL lowering or a higher
planner may set it only when arbitrary source arrival cannot affect observable values/errors or a
downstream authoritative total order removes that dependence.

## Affected invariants

This decision supports invariants [9, 10, 11, 14, and 18](../architecture/invariants.md): every
selected strategy remains finitely admitted, failures/cancellation unwind all operators and files,
ordering is never inferred from scheduling, and optimization claims carry differential and profile
evidence.

## Validation plan

- Boundary/hostile tests cover exact in-memory thresholds, spill fallback, incompatible/missing
  capabilities, duplicate estimates, target/source mismatches, and ordered-versus-independent
  source selection.
- Execution tests compare serial, parallel-plus-total-sort, and external strategies with an
  independent sorted model. Snapshot integration executes lowered SQL ORDER BY/LIMIT through the
  selected external path and removes hidden identity columns.
- Allocation injection sweeps strategy construction and serial/runtime instantiation with zero
  leaked query credit. Existing scheduler and spill allocation sweeps cover their delegated paths.
- A strategy fuzzer varies tasks, bounds, policy, capabilities, and order requirements under
  ASan/UBSan. Applicable TSan runs cover selected parallel execution.
- Microbenchmarks report 1/8/64-stage selection cost and 1/4-source selected instantiation cost;
  existing sort/spill/scheduler benchmarks remain the operator profiles.

## Unresolved questions

Statistics derivation, histograms, selectivity, join ordering, partial aggregation, top-N, parallel
tablet morsels, worker-pool admission, and device-aware I/O costing remain future work. They are not
required to preserve the currently checked stage order.

## References

- [ADR 0023](0023-bounded-physical-pipeline-plan.md)
- [ADR 0044](0044-query-accounted-bounded-physical-sort.md)
- [ADR 0056](0056-shared-query-credit-and-bounded-parallel-scheduling.md)
- [ADR 0057](0057-bounded-checksummed-external-sort.md)
- [Architecture invariants](../architecture/invariants.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
