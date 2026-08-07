# ADR 0049: Query-Accounted Variable-Width Extrema

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-execution maintainers

## Context

SQL v1 requires `MIN` and `MAX` for STRING, SYMBOL, and BINARY. The vector aggregate kernels
previously rejected these types because a winning value may be replaced repeatedly and its payload
must outlive the input chunk. Copying such a value without query credit would make retained memory
invisible to admission and could leak credit on replacement or failure.

## Decision

- Variable-width extrema use unsigned lexicographic byte order, matching the existing scalar total
  comparison and the SQL v1 no-collation contract. STRING and SYMBOL retain their distinct logical
  types; BINARY retains exact bytes. NULL cells are ignored.
- Each variable-width aggregate state has an explicit maximum payload size. The default is 1 MiB.
  Ungrouped and grouped limits expose the same per-state bound.
- Before copying a new winner, the operator computes a checked conservative charge, reserves that
  credit from the shared `QueryResourceContext`, materializes the scalar, and verifies its actual
  dynamic capacity fits the reservation. Only then does it atomically replace the previous value
  and reservation. A non-winning cell allocates and reserves nothing.
- The reservation remains owned by the aggregate state until canonical output materialization has
  copied the value. Grouped output retains the complete group state through that operation.
- Payload-limit violations, accounting overflow, container limits, and allocation failure are
  `RESOURCE_EXHAUSTED`. Existing operator failure handling requests cancellation and destroys all
  retained state before returning.

This decision changes no durable format, SQL syntax, collation behavior, or operator thread
affinity.

## Consequences

Bound global and grouped SQL can now lower and execute STRING/SYMBOL/BINARY `MIN` and `MAX` exactly.
Memory is proportional to the retained winners rather than input cardinality and is visible to the
query-wide budget. Replacement temporarily holds old and new reservations, which makes peak credit
conservative and preserves failure atomicity.

## Alternatives considered

- **Retain the winning input chunk:** makes memory depend on chunk width and delays unrelated buffer
  reclamation.
- **Resize one reservation in place:** the reservation API intentionally has move-only acquire/release
  semantics; reserve-before-replace is simpler and failure-atomic.
- **Charge only payload length after allocation:** allocates before admission and ignores container
  capacity.
- **Use locale collation:** SQL v1 defines unsigned byte order and has no collation surface.

## Affected invariants

This decision supports invariants [11, 16, and 18](../architecture/invariants.md): dynamic retained
query memory is admitted before allocation, operator ownership remains explicit, and cancellation
or failure releases every reservation.

## Validation plan

- Unit tests cover global and grouped STRING/SYMBOL/BINARY extrema, NULL and empty payloads,
  replacement across chunks, exact byte order, finite limits, SQL lowering, and cleanup.
- Allocation-failure injection enumerates every new winner-copy allocation and checks cancellation
  plus zero leaked query credit.
- Lowering fuzzing includes variable-width extrema. Sanitizers, public-header/installation checks,
  and the repository-wide check gate cover the exported limits.
- A microbenchmark contrasts replacement-heavy and no-replacement 32-byte STRING extrema.

## Migration or rollback considerations

There is no persisted-state migration. Rollback would restore explicit unsupported diagnostics for
variable-width extrema. Any replacement must preserve reserve-before-copy, exact byte order,
failure atomicity, and credit lifetime through output materialization.

## Unresolved questions

Canonical aggregate hashing, partial-state merge, spilling, batching grouped output, and parallel
aggregation remain separate decisions.

## References

- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0040](0040-streaming-ungrouped-vector-aggregates.md)
- [ADR 0042](0042-query-accounted-bounded-grouped-aggregates.md)
- [SQL v1](../product/sql-v1.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
