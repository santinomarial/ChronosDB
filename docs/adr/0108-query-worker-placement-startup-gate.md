# ADR 0108: Query-worker placement startup gate

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB query and runtime maintainers

## Context

Phase 12 introduced `ThreadPlacement` as an optional portable CPU/NUMA configuration boundary, but
the query scheduler did not apply it to the worker threads it owns. Applying placement after a
pipeline starts would make early work run on an unintended CPU. Returning an operator before every
worker reports placement would turn unsupported or invalid topology into a later, schedule-dependent
query failure. One worker must also never start a pipeline while another worker is still discovering
that its requested placement cannot be honored.

## Accepted decision

`ParallelMergeOperator::create` accepts either no worker placements or exactly one
`ThreadPlacement` per selected worker. An empty set preserves the existing correctness-neutral
behavior. Any other count is rejected before reserving resources or starting threads.

Worker construction uses a two-stage startup gate under the scheduler mutex:

1. every worker applies its own placement and reports its status and ordinal;
2. after registration is complete and all registered workers have reported success, workers may
   claim physical pipelines.

The creator waits for the same boundary before returning the operator. If any placement fails, no
pipeline is pulled, all workers are stopped and joined, configuration credit is released, and the
exact error is returned from `create` without cancelling the caller's query resource context. When
multiple placements fail concurrently, the lowest worker ordinal wins deterministically.

The mutex and condition variable establish the only cross-thread publication relationship for the
startup counters, terminal flag, and failure. The operating-system affinity effect itself is local
to the worker that calls `apply_current_thread_placement`; no relaxed atomic participates in startup
publication. Destruction or thread-construction failure sets the existing stop flag and wakes the
startup condition so a partially created scheduler cannot deadlock while joining.

## Consequences

- Query-worker CPU placement is now attached to the actual owning thread rather than a caller hint.
- Unsupported NUMA placement and portable-platform CPU placement fail before query execution.
- Exact placement adds one bounded startup barrier per parallel merge; it does not affect serial
  operators or scheduling after startup.
- Reactor and shard workers remain externally owned/caller-driven in the current packaged runtime;
  they may use the generic current-thread hook at their thread entry until those owners are packaged.
- NUMA memory policy still requires a future provider and is not inferred from CPU affinity.

## Alternatives considered

- **Apply placement in the creator:** changes the wrong thread.
- **Let each worker fail through the normal task-error path:** can execute other pipelines first and
  exposes a placement/configuration failure only after operator construction.
- **Accept a shorter placement vector and inherit unspecified workers:** makes topology intent
  ambiguous and difficult to reproduce.
- **Continue with best-effort placement:** silently changes the requested execution configuration.

## Affected invariants

Invariant 18 applies: placement is an optional optimization/configuration mode and cannot affect
query correctness. The startup gate also preserves the scheduler's existing thread-affine pipeline
ownership and bounded cancellation/join behavior.

## Validation

Focused tests reject mismatched placement counts, force a portable `NOT_SUPPORTED` NUMA request,
prove that a successfully placed worker executes zero pipeline pulls when a peer placement fails,
verify query credit and cancellation state after failure, and run two workers through the explicit
empty-placement success path. TSan, real Linux CPU sets, invalid/offline CPU behavior, NUMA policy,
and topology/performance experiments remain in the Phase 18 ledger.
