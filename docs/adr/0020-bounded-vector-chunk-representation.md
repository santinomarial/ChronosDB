# ADR 0020: Bounded Vector Chunk Representation

- **Status:** accepted
- **Date:** 2026-08-06
- **Owners:** ChronosDB query-execution and columnar maintainers

## Context

ADR 0008 requires vector execution over column-oriented chunks with explicit validity and
selection vectors, but deliberately defers the chunk width and selection encoding. Phase 8 now
provides an independent scalar oracle. The columnar library already validates the canonical
physical value representation needed by ingestion and CSEG pages, including every logical type,
null bitmap, variable offset, UTF-8, and decimal rule.

Phase 9 needs one in-memory currency for scans and operators before expression specialization,
physical planning, scheduling, or spill can be implemented. Reusing schema-shaped ingestion
batches directly would incorrectly require a durable `ColumnId` for computed expressions. Using
owned `ScalarValue` per cell would add per-row variable-width allocation and make the optimized
path structurally too similar to the oracle.

## Decision

The initial Phase 9 vector substrate has these contracts:

- `OwnedPhysicalColumn` owns the existing canonical physical buffers without a table or column
  identity. It is move-only and immutable after validated construction. `OwnedColumnVector`
  remains the schema-identified ingestion owner and composes the same physical owner.
- A `VectorChunk` owns zero or more identity-free physical columns over one nonzero physical row
  domain. Zero columns are valid so cardinality-only operators such as `COUNT(*)` do not retain a
  fabricated payload. Every present column has the exact same physical row count.
- Every chunk owns an explicit UINT32 selection vector. Selected ordinals are in range, unique, and
  strictly increasing, so filtering preserves input order and cannot duplicate a row. The identity
  selection is represented explicitly in this first implementation. A predicate may produce an
  empty selection, but an empty physical row domain is not a chunk or end-of-stream sentinel.
- Validity remains the canonical LSB-first bitmap within each physical column. Selection never
  changes whether a cell is NULL; it maps a selected-row ordinal to a physical-row ordinal.
- Construction checks caller-supplied row, column, logical-buffer-byte, and retained-buffer-byte
  limits before returning ownership. Retained accounting includes column and selection vector
  capacities so spare allocation cannot evade the chunk bound. C++ object, allocator, and
  container bookkeeping are not claimed by this local count and must be covered by the future
  query-wide memory-admission contract.
- The default local row limit is 2,048 and the default local logical/retained byte limit is 32 MiB.
  They are conservative starting policy, not durable bytes, public SQL semantics, or a performance
  conclusion. Operator configuration may select smaller or larger finite limits, and differential
  tests and benchmarks must vary boundaries rather than depend on the default.
- Safe selected-cell inspection returns borrowed canonical cells and checks both column and
  selected-row ordinals. The chunk stores no borrow outside its own immutable column storage.
- Concurrent const reads are safe while the chunk remains alive. This representation contains no
  synchronization, mutable append API, publication primitive, cancellation state, or lifetime pin.

This decision changes no durable or network format and adds no dependency.

## Detailed rationale

One physical representation prevents query execution from drifting away from already tested type
and null-domain rules while keeping durable identity out of computed data. A single allocation for
selection ordinals is bounded and simple to audit. Strictly increasing selections make stable
filter order inherent, give deterministic differential tests, and leave arbitrary reorder to an
explicit sort or gather operator.

The explicit identity selection costs four bytes per physical row. That cost is intentional in the
correctness-first substrate and is measured separately; a dense-range optimization may replace its
storage only after profiles and equivalent semantics. Requiring nonempty physical domains avoids
conflating “no rows selected” with end-of-stream and keeps every physical column valid under the
existing canonical validator.

## Alternatives considered

- **Use `OwnedColumnarBatch` as every query chunk:** reuses buffers but forces computed columns to
  invent durable identities and pins a table schema where intermediate expressions may have none.
- **Store `ScalarValue` per cell:** is easy to implement but allocates for variable-width values,
  loses canonical packed representation, and weakens the independence of scalar/vector
  differential testing.
- **Permit arbitrary or duplicate selection order:** could express gather and join output, but it
  would let a filter silently reorder or duplicate input. Those operations require explicit
  operator contracts.
- **Represent dense selection only as a range:** reduces memory, but creates two traversal paths
  before measurement and weakens the “explicit selection” first implementation.
- **Choose one fixed global chunk width:** is simple but turns an unmeasured tuning value into an
  architectural constraint and obstructs forced-boundary tests.

## Consequences

Scans and future vector operators can exchange a validated, schema-independent, bounded object.
Computed output schemas remain plan metadata rather than durable vector identity. The initial
selection representation consumes measurable memory even for dense input, and constructing an
identity selection performs one bounded allocation.

This substrate is not query-wide memory admission: it does not reserve before the caller allocates
input buffers, cover operator state, or transfer memory ownership across tasks. It also does not
define end-of-stream, backpressure, cancellation, scheduler queues, join row mappings, output
builders, or spill bytes. Those remain required Phase 9 decisions.

## Affected invariants

This decision supports invariants [6, 10, 11, and 18](../architecture/invariants.md). Exact physical
types and validity are retained across vector boundaries; selected access is bounds checked;
ownership and borrowed-cell lifetime are explicit; and chunk-width or future dense-selection
optimizations cannot change query truth or ordering guarantees.

## Validation plan

- Unit and deterministic property tests cover identity, sparse, empty, duplicate, reordered, and
  out-of-range selections; zero-column cardinality; row-shape mismatch; selected-cell mapping; and
  logical versus retained bounds.
- Existing canonical physical-column tests remain the value-domain oracle. The refactor must leave
  every ingestion batch and CSEG consumer passing unchanged.
- A coverage-guided harness drives hostile selection shapes and valid canonical Boolean chunks
  under the ordinary sanitizer configuration.
- Microbenchmarks vary 64, 1,024, and 4,096 physical rows and full, quarter, and one-sixteenth
  selection density for construction and checked traversal. Results are evidence inputs, not
  performance claims.
- Public-header, installation, and external-consumer tests compile and exercise the new surface.

## Migration or rollback considerations

There is no deployed state and no byte-format migration. The API is pre-alpha and can evolve with
source changes. Removing it requires first migrating every physical operator while preserving the
same canonical value, validity, selection-order, accounting, and ownership contracts.

## Unresolved questions

The query-wide hierarchical memory reservation API, pre-allocation charging, cancellation token,
task/morsel ownership, scheduler fairness, operator interface, chunk builders, join output mapping,
spill eligibility and format, and evidence-based default width remain for later Phase 9 ADRs.

## References

- [ADR 0008](0008-custom-sql-and-vectorized-execution.md)
- [ADR 0012](0012-correctness-testing-and-performance-evidence.md)
- [Columnar memory model](../learning/columnar-memory-model.md)
- [SQL v1 scalar reference engine](../learning/sql-v1-reference-engine.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
