# Query Resource Control

## Purpose and phase boundary

`QueryResourceContext` gives one in-process query a shared memory-admission boundary and a
cooperative cancellation signal. It is the resource-ownership prerequisite for Phase 9 physical
operators and scheduling; it does not execute a plan, allocate a chunk, own a snapshot pin,
schedule a task, spill data, or enforce a cancellation deadline.

## Public interfaces

`chronos/query/resource_context.hpp` exposes two types:

- `QueryResourceContext` is a copyable handle to one shared state. Its factory requires an explicit
  nonzero maximum. Copies observe the same current/peak counters and cancellation state.
- `QueryMemoryReservation` is a move-only RAII credit. `reserve(bytes)` creates it after atomically
  charging the shared budget. Destruction or `release()` returns the credit.

`reserve(0)` and a zero maximum are `INVALID_ARGUMENT`. A request beyond available credit is
`RESOURCE_EXHAUSTED`. `check_cancelled()` and reservations attempted after observed cancellation
return `CANCELLED`.

## Memory-admission invariant

The shared current counter is always between zero and the configured maximum. Reservation uses a
compare/exchange loop: given current `C` and request `R`, it rejects when `R > maximum - C`; otherwise
it changes current to `C + R` atomically. Subtraction in the test avoids addition overflow. Only a
successfully created reservation may return the same `R` bytes.

The peak counter records the greatest successfully charged current value. Current, available, and
peak are individually safe concurrent observations. Reading several accessors does not produce one
transactional metrics snapshot because other tasks can reserve or release between reads.

A byte credit is an admission promise, not an allocation. Callers must reserve before allocating and
must charge a conservative amount that covers the retained allocation they control. The context
does not intercept `new`, container growth, allocator metadata, snapshot pins, or file mappings.
Physical operators must document how each of those is charged.

## Ownership and lifetime

Context copies share the state through reference-counted ownership. A reservation also retains that
state, allowing its destructor to return credit after all contexts disappear. The default-constructed
reservation is an invalid empty holder for optional/container use; releasing it is harmless.

Moving a reservation transfers its state and byte count. Move assignment first releases any credit
already owned by the destination. No copy operation exists, so one credit cannot acquire two release
obligations.

## Cooperative cancellation

`request_cancel()` atomically changes a Boolean once. It returns true to the first requester and
false thereafter. `is_cancelled()` is the cheap poll; `check_cancelled()` creates the stable
`CANCELLED` status used at an API boundary.

Cancellation never calls an operator, frees another task's buffer, revokes credit, or unpins a
snapshot. Each task checks at documented bounded-work points and exits normally. Its local chunk,
reservation, pin, and operator owners then unwind in dependency-safe order. Consequently current
reserved bytes may remain nonzero after cancellation until all owners observe it.

A reservation racing with a cancellation request may succeed. Cooperative cancellation promises
eventual observation at poll points, not instantaneous preemption or a total order across distinct
atomic variables.

## Concurrency and memory ordering

Current bytes, peak bytes, and cancellation are independent atomic control values. They do not
publish a constructed chunk, transfer an owner, or guard mutable query data, so their operations use
relaxed ordering. Reference counting keeps the state object alive but is not a task handoff protocol.
A future scheduler queue must release-publish a complete task/owner and acquire-observe it before
execution.

Concurrent reservation may use an internal lock on platforms where the standard atomic is not lock
free; the API makes no lock-free or wait-free claim. Cancellation has no bounded wall-clock latency
until every operator defines its poll granularity and blocking-I/O behavior.

## Failure behavior and complexity

Context creation allocates one shared state and maps allocation failure to `RESOURCE_EXHAUSTED`.
Reservation and release allocate nothing and are expected `O(1)` atomic operations, though
contention can retry compare/exchange. Cancellation request and polling are `O(1)`. Failure changes
no durable or external state.

## Verification and measurement

Tests cover exact counters, peak behavior, over-limit and maximum-sized requests, move ownership,
idempotent release/cancellation, live credit after cancellation, shared contexts, a deterministic
2,000-step reference model, concurrent exact saturation, and concurrent polling. The race cases run
under ThreadSanitizer in addition to the ordinary sanitizer matrix.

`chronos_query_benchmarks` tracks a complete reserve/destruct cycle for 64-byte, 4-KiB, and 1-MiB
credits and the uncancelled polling path. Those measurements identify overhead trends; they do not
establish query throughput, scheduler scalability, or a product latency claim.

## Tradeoffs and next steps

One root budget is auditable but does not attribute bytes to individual operators or decide when to
spill. Callback-free cancellation is ownership-safe but requires explicit poll coverage. The next
Phase 9 boundary can define the physical operator/task protocol over `VectorChunk` and this context,
including end-of-stream, backpressure, pin ownership, and failure propagation. Hierarchical accounts
and spill credit should follow actual operator needs rather than precede them.

## Likely review questions

**Why is cancellation not releasing memory immediately?** The cancellation flag does not own the
memory. Releasing its credit while another task still holds the bytes would permit overcommit and
could lead to use-after-free if reclamation were forced.

**Why are the atomics relaxed?** They exchange only control values. Task queues, not counters, must
publish chunks and owners. Adding acquire/release here would not repair a missing queue handoff.

**Can a query still exceed its budget?** Yes, if an operator allocates before reserving or
undercharges its ownership. The context supplies the invariant; each operator must connect every
retained allocation to a reservation.

**Why allow context copies to request cancellation?** Any worker detecting a terminal failure must
be able to stop sibling work. The operation is idempotent and does not perform cleanup itself.
