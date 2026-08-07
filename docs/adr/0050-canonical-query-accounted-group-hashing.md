# ADR 0050: Canonical Query-Accounted Group Hashing

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-execution maintainers

## Context

ADR 0042 deliberately used a linear scan of retained groups as the first correctness baseline. Its
cost is `O(rows * groups * keys)`, which makes otherwise bounded grouped SQL impractical as
cardinality grows. Replacing that scan requires a hash equivalence that exactly matches grouping,
finite bucket storage admitted through the query budget, and collision handling that never treats a
hash as identity.

## Decision

- Grouped aggregation uses one pre-sized power-of-two open-addressed table with linear probing. The
  table has at least two buckets per configured maximum group, never grows or rehashes, and remains
  bounded by the existing maximum-group limit.
- The first selected input row reserves conservative query credit for both the maximum group-slot
  array and every hash bucket before either allocation. The reservation remains owned by the
  grouped operator until output completes or failure destroys the state.
- Hash framing includes each key's exact logical type parameters, NULL marker, and payload length.
  Fixed-width canonical bytes are hashed directly. Boolean values use their logical bit. FLOAT32
  and FLOAT64 canonicalize both signed zeros to zero and every NaN payload/sign to one quiet-NaN
  token, exactly matching the scalar total equality used for grouping.
- A bucket retains only a group-vector ordinal. A matching hash always performs exact key equality
  against the retained `ScalarValue` tuple; collisions therefore affect work, never semantics.
  Empty buckets terminate a lookup because the table does not delete groups.
- New groups remain appended to the separate group vector. Output consequently preserves the
  existing first-seen implementation order, while SQL without `ORDER BY` continues to promise only
  a multiset.
- The hash algorithm and bucket placement are an in-memory implementation detail, not a durable
  format or public ordering contract. Hash/equality equivalence, finite capacity, collision proof,
  and query-accounted ownership are required if the implementation changes.

This decision supersedes only ADR 0042's linear-lookup implementation choice. Its group equality,
limits, output, cancellation, and failure contracts remain unchanged.

## Consequences

Expected grouped lookup becomes `O(keys)` per row at a maximum load factor of one half; adversarial
collisions remain bounded by the finite bucket count and exact comparisons. The operator acquires a
small additional bucket reservation even for low-cardinality input because it chooses one fixed
capacity from the caller's maximum rather than allocating or rehashing during consumption.

## Alternatives considered

- **Retain linear lookup:** simplest, but its measured cardinality slope prevents Phase 9 from
  claiming a usable grouped execution path.
- **Use `std::unordered_map`:** bucket growth, implementation-dependent retained bytes, and
  allocation timing do not expose the precise reserve-before-allocate contract required here.
- **Treat the hash as identity:** collisions would merge distinct SQL groups and are never valid.
- **Hash raw floating bytes:** `-0` and `+0`, plus different NaN encodings, compare equal for
  grouping and therefore require canonical hash tokens.
- **Grow by load:** requires a separate failure-atomic rehash ownership decision. Fixed capacity is
  simpler because the accepted group-count limit is already mandatory.

## Affected invariants

This decision supports invariants [11, 16, and 18](../architecture/invariants.md): retained bucket
memory is admitted before allocation, ownership stays with one thread-affine operator, and every
failure or cancellation path releases bucket, group, key, and aggregate credit.

## Validation plan

- Unit and independent-model tests cover first-seen results, NULL, signed zero, distinct NaN
  payloads, variable keys, deliberate bucket collisions, chunk boundaries, and maximum groups.
- Allocation-failure injection traverses group-slot and bucket allocation plus all later group and
  output allocations, checking cancellation and zero leaked credit.
- A dedicated libFuzzer target compares arbitrary nullable FLOAT64 grouping against an independent
  first-seen model, including NaN payloads and signed zeros.
- Group-cardinality microbenchmarks cover 1, 16, 256, and 4,096 groups over the same 32,768 rows.
  Sanitizers, static analysis, public-header, installation, and external-consumer gates remain
  required.

## Migration or rollback considerations

There is no persisted-state migration. Rollback can restore linear lookup without changing SQL
results. A replacement must retain exact collision comparison, canonical floating equivalence,
reserve-before-allocation, and deterministic resource cleanup.

## Unresolved questions

Parallel partial-state merge, batched grouped output, partitioned spill, and optimizer selection
remain separate Phase 9 decisions.

## References

- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0042](0042-query-accounted-bounded-grouped-aggregates.md)
- [ADR 0043](0043-bound-grouped-aggregate-physical-lowering.md)
- [ADR 0049](0049-query-accounted-variable-width-extrema.md)
- [SQL v1](../product/sql-v1.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
