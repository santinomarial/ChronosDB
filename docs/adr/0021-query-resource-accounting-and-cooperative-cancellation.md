# ADR 0021: Query Resource Accounting and Cooperative Cancellation

- **Status:** accepted
- **Date:** 2026-08-06
- **Owners:** ChronosDB query-execution and scheduling maintainers

## Context

Phase 9 must keep analytical work within an admitted memory budget and must release buffers,
snapshot pins, and operator state after cancellation. ADR 0008 requires memory and cancellation
limits before bounded morsels are scheduled. ADR 0020 provides locally bounded chunks, but its
buffer counters do not coordinate multiple chunks, operators, or tasks and do not reserve credit
before allocation.

Parallel scheduling needs one process-local resource identity that can be copied into tasks without
copying the budget. A cancellation mechanism that owns arbitrary callbacks would create difficult
reentrancy, lock-order, and lifetime problems. A memory counter used as an allocator or task
publication primitive would similarly blur accounting with ownership synchronization.

## Decision

The initial query-wide resource contract is:

- `QueryResourceContext::create(maximum_memory_bytes)` creates one nonzero memory budget and one
  cooperative cancellation state. Context copies share exactly that state and are the intended task
  handoff handle.
- `reserve(bytes)` atomically acquires a nonzero byte credit or returns `RESOURCE_EXHAUSTED` without
  exceeding the configured maximum. Callers reserve a conservative charge before allocating or
  retaining the associated memory.
- A successful reservation returns a move-only `QueryMemoryReservation`. Its destructor or explicit
  `release()` returns the exact credit. Move construction and move assignment transfer the release
  obligation; assignment releases the destination's old credit first.
- A reservation retains the shared resource state, so credit can be returned even after all context
  handles have been destroyed. Destroying a context does not implicitly cancel work.
- The context exposes current, available, and monotonic peak reserved bytes. These are process-local
  observability values, not SQL results or durable fields.
- `request_cancel()` is idempotent and identifies only the first state transition. Polling through
  `is_cancelled()` or `check_cancelled()` reports the shared state; the latter returns `CANCELLED`.
- Cancellation runs no user callbacks, does not revoke a reservation, and does not reclaim a pin or
  buffer from another task. Owners poll at bounded work boundaries and unwind through RAII. A
  reservation already racing with cancellation may complete; it observes cancellation at its next
  poll point.
- Reservation, peak, and cancellation atomics use relaxed ordering. They publish only numeric
  control state and do not make query data, chunks, or owner lifetimes visible. Future scheduler
  queues must provide their own release/acquire synchronization for task and chunk handoff.
- This API does not instrument the global allocator. Every allocating subsystem must define the
  conservative charge it reserves. Uncharged allocations are correctness gaps, not permission to
  claim that the query is fully memory bounded.

This decision changes no durable or network format and adds no dependency.

## Detailed rationale

One shared counter gives all tasks a single admission boundary. Compare/exchange performs the
limit check and charge as one linearized state change, so concurrent contenders cannot overcommit.
RAII makes exceptional and cancellation unwinding return credit without a separate coordinator.
The peak counter is monotonic and may be read independently of the current counter; a pair of reads
is an observational snapshot, not a transactional metrics record.

Cooperative cancellation preserves ordinary ownership rules. A thread never destroys storage that
another thread may still dereference, and cancellation code never invokes an operator while holding
hidden resource locks. Poll points can later be placed between chunks, morsels, hash-table growth,
spill I/O stages, and scheduler dequeues according to bounded-latency requirements.

## Alternatives considered

- **Use allocation failure as admission:** can exceed the intended per-query share, gives no stable
  overload boundary, and cannot distinguish query policy from process exhaustion.
- **Cancel by running registered callbacks:** can reduce response latency but creates callback
  lifetime, reentrancy, deadlock, and partial-cleanup hazards before operator ownership exists.
- **Revoke reservations immediately on cancel:** makes accounting lie while owners still retain the
  bytes and could allow a second query to overcommit the process.
- **Use a mutex for every counter operation:** is correct, but the state transition is a small
  independent integer check. Atomics make the concurrency invariant explicit without publishing
  data or claiming lock freedom.
- **Adopt `std::stop_token` as the whole API:** provides useful cancellation primitives but does not
  supply memory accounting and encourages callbacks that this first ownership contract excludes.
- **Implement operator-child budgets immediately:** could improve attribution but precedes the
  physical operator tree and spill policy. The query-wide root is the current required invariant;
  hierarchical attribution remains a later compatible extension.

## Consequences

Future builders can reserve before allocating chunks, hash tables, sort runs, queue entries, and
spill buffers. Scheduler tasks can share cancellation and memory state while independently owning
their reservations and pins. Admission failures and cancellation have distinct status codes.

The API alone cannot prove bounded execution. Operators must include allocator/container overhead
in their charge, avoid retaining memory without credit, and release ownership at every exit. There
is no cancellation deadline or forced preemption, so every potentially long-running operator needs
documented polling granularity.

## Affected invariants

This decision supports invariants [9, 11, 16, and 18](../architecture/invariants.md): one query cannot
claim unbounded memory, cancellation does not reclaim referenced storage, resource ownership stays
bounded under concurrency, and atomic control changes do not weaken data visibility semantics.

## Validation plan

- Unit tests cover invalid limits, exact saturation, overflow-sized requests, move assignment,
  explicit release, idempotent cancellation, and cancellation with live reservations.
- A deterministic reference-model property test compares thousands of reserve/release operations
  with exact current and peak counters.
- Concurrent saturation proves that exactly the admitted number of reservations can survive, and
  concurrent cancellation polling runs under ThreadSanitizer.
- Public-header, installation, and external-consumer tests compile and exercise the API.
- Microbenchmarks track reserve/release and uncancelled poll overhead without making throughput or
  latency claims.

## Migration or rollback considerations

There is no persisted state. The pre-alpha API can evolve when physical operators define charge
classes or hierarchical accounts. Any replacement must preserve pre-allocation admission, exact
RAII release, live-owner accounting after cancellation, shared task visibility, and distinct
`CANCELLED` versus `RESOURCE_EXHAUSTED` outcomes.

## Unresolved questions

Operator-child attribution, allocator-overhead policy, spill credit transfer, reservation growth,
deadline propagation, poll granularity, scheduler queue ownership, cancellation of blocking I/O,
and externally visible query metrics remain for later Phase 9 decisions.

## References

- [ADR 0008](0008-custom-sql-and-vectorized-execution.md)
- [ADR 0012](0012-correctness-testing-and-performance-evidence.md)
- [ADR 0020](0020-bounded-vector-chunk-representation.md)
- [Architecture invariants](../architecture/invariants.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
