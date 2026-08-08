# ADR 0056: Shared Query Credit and Bounded Parallel Scheduling

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-execution maintainers

## Context

ADR 0021 provides one query-wide memory budget and move-only reservations. ADR 0022 makes physical
operators thread-affine pull owners. Parallel execution needs a bounded handoff between independent
pipelines, and immutable scheduler or pin state may be retained by several workers without charging
the same allocation once per copy. Neither contract previously defined that shared-credit lifetime
or scheduler ownership.

## Decision

- `QuerySharedMemoryReservation` is a copyable RAII token for one admitted immutable allocation or
  lifetime. `reserve_shared(bytes)` charges once; copies retain the same obligation; the last copy
  returns the exact credit. It does not make mutable state safe and does not publish data.
- `ParallelMergeOperator` accepts a finite nonempty vector of independent physical pipelines,
  nonzero task/worker/ready-queue/configuration limits, and one query resource context.
- A worker claims a complete pipeline and remains that pipeline's only pulling thread. Work is not
  split within an operator. The root is the sole consumer of a fixed-capacity ring of complete
  `AccountedVectorChunk` owners.
- Mutex unlock/lock around the ring is the release/acquire publication edge. Query-resource atomics
  remain relaxed control counters and are not used to publish tasks or chunks.
- Scheduler configuration, adopted task-vector capacity, queue slots, worker handles, and
  conservative allocation overhead are admitted before worker creation under one shared credit.
  Chunks retain their existing independent query reservations.
- Output is deliberately unordered. This operator may be used only where SQL permits a multiset or
  where a downstream operator establishes the complete required order. Scan arrival order and queue
  order are never SQL tie-breaks.
- On multiple task failures, a non-cancellation status wins over cancellation, then the lowest task
  ordinal wins. Buffered chunks are released, siblings are cooperatively cancelled, every worker is
  joined, and only then is the chosen status returned.
- Destruction before terminal end requests cancellation, wakes blocked producers, joins all owned
  threads, and releases queue/task/configuration ownership. Normal completed destruction does not
  cancel the query. Worker-allocation exceptions are classified as resource exhaustion; worker
  creation failure is unavailable.

The increment creates bounded workers per merge instance. A reusable process-wide pool, ordered
merge, morsel splitting, work stealing, spill, optimizer selection, and parallel storage I/O remain
separate decisions.

## Consequences

Independent unordered pipelines can execute concurrently without unbounded ready output, migration
of thread-affine operators, double charging shared configuration, or detached cleanup. Queue
backpressure can intentionally leave workers blocked until the consumer pulls or cancellation wakes
them.

Per-instance threads have measurable creation cost and are not an optimizer default. Publication
pins may adopt shared reservations when their complete shared allocation is identified, but this
ADR does not deduplicate current repeated-tablet source pins automatically.

## Alternatives considered

- **Copy move-only credit into each worker:** either does not compile or invents multiple release
  obligations for one charge.
- **Reserve once per worker:** overcharges one shared allocation and makes admission depend on worker
  count rather than retained bytes.
- **One unbounded concurrent queue:** violates finite query influence and hides backpressure.
- **Pull one pipeline from multiple workers:** violates existing physical-operator thread affinity.
- **Use queue arrival as deterministic order:** scheduling and wakeups are nondeterministic and are
  not part of the SQL contract.
- **Return the first racing error:** makes observable diagnostics depend on thread scheduling.

## Affected invariants

This decision supports invariants [6, 11, and 18](../architecture/invariants.md): shared owners retain
their admitted lifetime until the last reference, cancellation joins all worker ownership, and
parallel execution cannot weaken snapshot or ordering semantics.

## Validation plan

- Force two pipelines to start concurrently; verify whole-pipeline thread affinity, a one-slot
  queue, multiset equivalence, metrics, normal end, and early-destruction cleanup.
- Coordinate simultaneous failures and verify deterministic arbitration independent of arrival.
- Reject zero, excessive, null, foreign-query, and configuration-overflow shapes.
- Inject every caller-thread allocation failure, throw allocation failure from a worker, fuzz
  hostile scheduler limits and task failures, and measure one/four task lifecycle and merge costs.
- Cover the installed public API plus ASan/UBSan, TSan, static analysis, and repository checks.

## Migration or rollback considerations

There is no durable or network migration. Rollback removes the scheduler and shared token; existing
move-only reservations and serial operators are unchanged.

## Unresolved questions

Process-wide worker-pool admission, optimizer strategy selection, ordered merge, spill, shared
publication-pin deduplication, and mapped/asynchronous storage providers remain separate work.

## References

- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0022](0022-pull-based-physical-operator-lifecycle.md)
- [Bounded parallel query scheduling](../learning/bounded-parallel-query-scheduling.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
