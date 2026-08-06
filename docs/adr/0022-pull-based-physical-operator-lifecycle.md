# ADR 0022: Pull-Based Physical Operator Lifecycle

- **Status:** accepted
- **Date:** 2026-08-06
- **Owners:** ChronosDB query-execution and scheduling maintainers

## Context

ADR 0020 defines immutable bounded vector chunks and explicit selections. ADR 0021 defines shared
query memory credit and cooperative cancellation. Phase 9 now needs an operator ownership boundary
that connects those substrates before physical planning or parallel scheduling can safely retain,
transfer, or cancel work.

The first operator must exercise real vector semantics rather than provide an empty interface.
SQL `WHERE` Boolean truth is a useful boundary: TRUE retains a selected row, while FALSE and NULL
remove it. It also tests selection composition and chunk boundaries against the scalar oracle
without requiring expression output builders or new type semantics.

## Decision

- `AccountedVectorChunk` couples one `VectorChunk` to one move-only `QueryMemoryReservation`. The
  reservation charge must be at least the chunk's retained canonical-buffer and selection capacity.
  A conservative larger charge is valid for object or allocator overhead. Construction binds that
  reservation to the same `QueryResourceContext` identity, and every operator rejects a chunk paid
  for by another query.
- Callers acquire credit before allocating or retaining the chunk. The accounting wrapper validates
  coverage when ownership is joined; it cannot retrospectively prove allocation order.
- `PhysicalOperator::next(resources)` is a thread-affine pull boundary. One call returns one owning
  `PhysicalOperatorStep` containing an accounted chunk, explicit end-of-stream, or a status error.
  Downstream demand therefore bounds in-flight output without a hidden push queue.
- An operator instance is never called concurrently. A future scheduler may move a whole pipeline
  between tasks or serialize access, but must not race `next()` calls.
- Successful end-of-stream is sticky. Empty selected chunks are valid progress and are not confused
  with end-of-stream.
- `BooleanFilterOperator` uniquely owns its input operator. Every pull checks cancellation, pulls at
  most once from the child, applies SQL WHERE truth to the configured BOOL column, and returns the
  same chunk owner and memory credit.
- Filtering consumes and compacts the existing selection allocation in place. It retains only
  selected rows whose predicate cell is non-null TRUE. Physical rows, column buffers, order, and
  selection capacity remain unchanged; logical selection bytes may shrink.
- A child or local operator error is returned unchanged after requesting shared cancellation so
  sibling work can stop. The failing chunk and its reservation unwind before the caller regains
  control. A later pull observes `CANCELLED` unless the operator had already ended successfully.
- Operator objects contain no synchronization or publication atomics. ADR 0021's resource atomics
  remain control state only. Future scheduler queues must provide release/acquire owner publication.

This decision changes no durable or network format and adds no dependency.

## Detailed rationale

Pull gives the initial single-threaded pipeline an obvious ownership path: the child relinquishes a
chunk, the parent transforms it, and the caller owns it until destruction or the next pipeline
stage. Embedding credit beside the chunk prevents a queue or operator from retaining data after
discarding its accounting token.

In-place filter compaction avoids a temporary selection allocation and does not weaken immutability
visible to callers because the input chunk is consumed. Strictly increasing input ordinals remain
strictly increasing after stable removal. Reusing retained capacity makes the existing charge
sufficient for the result.

## Alternatives considered

- **Push callbacks:** can enable parallel pipelines but requires queue capacity, callback lifetime,
  reentrancy, and scheduler ownership decisions not yet accepted.
- **Use an empty chunk as end-of-stream:** makes a legitimate all-false predicate indistinguishable
  from completion and risks early termination.
- **Return an unaccounted `VectorChunk`:** allows queues and callers to detach memory ownership from
  query admission.
- **Allocate a new selection on every filter:** is correct but raises transient memory and requires
  an additional reservation when stable compaction suffices.
- **Filter column buffers physically:** can improve later locality but copies every surviving value,
  changes physical ordinals, and precedes evidence for materialization policy.
- **Make `next()` concurrently callable:** would force synchronization into every operator before a
  scheduler, task ownership model, or measured contention exists.

## Consequences

The query library now has one composable physical operator and explicit chunk/end/error ownership.
Memory credit follows chunk lifetime, Boolean selection is vectorized without per-row allocation,
and terminal failure cooperatively stops siblings.

The protocol is not a complete engine. It has no scan adapter, expression program, output builder,
physical plan, task/morsel scheduler, parallel queue, snapshot-pin owner, aggregate/join, spill,
deadline, or SQL execution entry point. Virtual dispatch occurs once per chunk, not per row, and is
not claimed to be optimal without profiles.

## Affected invariants

This decision supports invariants [6, 9, 10, 11, and 18](../architecture/invariants.md): Boolean and
NULL truth is exact, demand and memory are bounded, selection access remains safe, credit/pins are
released by ownership, and later scheduling cannot use resource atomics as data publication.

## Validation plan

- Unit tests cover TRUE/FALSE/NULL filtering, sparse input selections, invalid type/shape/ordinal,
  undercharged and cross-query chunks, explicit end, sticky completion, child failure,
  cancellation, and credit release.
- A fixed-seed property executes varied chunk and selection boundaries and compares every retained
  ordinal with scalar SQL WHERE truth.
- The existing vector fuzzer drives allocation-free Boolean compaction on arbitrary valid bitmaps.
- ASan/UBSan and ThreadSanitizer exercise ownership and shared cancellation; public-header,
  installation, and external-consumer tests cover the exported API.
- Microbenchmarks vary row count and TRUE density for selection compaction. Results are evidence
  inputs, not product performance claims.

## Migration or rollback considerations

There is no persisted state. A future scheduler may wrap or supersede the pull interface, but it
must preserve explicit end, owning accounted transfer, empty-chunk progress, cancellation cleanup,
and exact SQL truth. Selection materialization may change only with differential equivalence and
memory evidence.

## Unresolved questions

Physical plan representation, scan/snapshot pinning, expression bytecode, output builders, operator
metrics, pipeline fusion, task/morsel ownership, scheduler fairness, queue capacity, parallel error
arbitration, aggregate/join algorithms, and spill remain later Phase 9 decisions.

## References

- [ADR 0008](0008-custom-sql-and-vectorized-execution.md)
- [ADR 0012](0012-correctness-testing-and-performance-evidence.md)
- [ADR 0020](0020-bounded-vector-chunk-representation.md)
- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
