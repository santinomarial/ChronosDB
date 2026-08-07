# Typed-Constant Output Materialization

## Purpose and boundary

`ColumnOutputOperator` builds one owned result chunk whose positions can interleave existing source
columns and typed constants in arbitrary caller order. It extends the canonical ownership and query-
credit boundary established by `SourceColumnOutputOperator`; it is not a general expression engine.
Arithmetic, casts, functions, bound-SQL lowering, aggregation, and result transport remain outside
this increment.

## Public interfaces

`chronos/query/column_output.hpp` exports:

- `SourceColumnOutputPosition`, containing one current input physical ordinal;
- `ConstantColumnOutputPosition`, owning one typed `ScalarValue`;
- `ColumnOutputPosition`, the closed source-or-constant variant;
- `kMaximumColumnOutputWidth`, the 4,096-position bound; and
- `ColumnOutputOperator::create(input, positions, limits)`.

`ColumnOutputStage` in `chronos/query/physical_plan.hpp` stores the same ordered representation and
limits. Plan creation validates source ordinals against the shape at that exact stage. A source
position copies its current shape. A non-NULL constant contributes its exact logical type and
`nullable=false`; a typed NULL contributes the same type and `nullable=true`. Untyped NULL cannot
be materialized and is rejected before source execution.

The source-only operator and stage remain available for callers whose configuration is naturally a
plain ordinal vector.

## Physical representation

Every output position owns an independent `OwnedPhysicalColumn`. Source cells use the ADR 0033
canonical copy path. Constants are written without a per-row scalar:

- BOOL repeats one bit in the value bitmap;
- signed and unsigned integers, DATE, and TIMESTAMP_NS use exact-width little-endian values;
- FLOAT32/FLOAT64 preserve their IEEE bit representation in little-endian order;
- DECIMAL repeats its 16-byte little-endian coefficient and retains precision/scale in the type;
- UUID repeats the uninterpreted 16-byte UUID order;
- STRING, SYMBOL, and BINARY repeat the payload and write monotonic 32-bit offsets; and
- typed NULL writes an all-zero validity bitmap, canonical empty/null slots, and a null count equal
  to the physical row count.

A nonempty selection is stable-compacted into an identity domain. An empty selection remains a
progress chunk over the input physical domain; its columns are physically valid, but no selected
row can observe them. An empty position list remains a cardinality-only chunk.

## Ownership, lifetime, and accounting

The operator uniquely owns its child and position configuration and follows the thread-affine pull
contract. Constant strings and binary payloads are owned inside the configuration. Returned chunks
do not borrow either the input or the configuration.

Before allocating output buffers, planning checks row/column limits, all fixed and bitmap sizes,
variable payload multiplication, the 32-bit terminal-offset limit, owner containers, selection
indices, and conservative allocator overhead. The query reserves that charge while the input chunk
and its charge remain alive, which represents the true simultaneous peak. RAII returns both charges
on every failure; successful output credit lives with the returned `AccountedVectorChunk`.

Configuration capacity is also finite. The direct factory bounds the position vector and nested
string/binary retained capacity. `PhysicalPipelinePlan` includes the stage vector and nested payload
capacities in its retained-configuration limit.

## Failure behavior

- Null children, zero limits, untyped NULL constants, and foreign query ownership are
  `INVALID_ARGUMENT`.
- Runtime source ordinals outside the actual chunk are `OUT_OF_RANGE`.
- Width, row, byte, offset, retained-credit, checked-arithmetic, allocation, and container failures
  are `RESOURCE_EXHAUSTED`.
- An impossible scalar-storage/type mismatch is `INTERNAL`.

Child and local pull failures request cooperative cancellation. End-of-stream is sticky and releases
the unique child.

## Complexity and performance evidence

For `P` output positions and `R` materialized rows, fixed-width/Boolean work is `O(P*R)` and output
memory is the exact canonical buffers plus owners and selection. A variable constant of length `L`
adds `O(R*L)` bytes and copy work. Variable source columns retain their size pass plus copy pass.
There is no dictionary sharing, aliasing, or common-subexpression elimination.

`materialize_mixed_source_and_typed_constants` measures one source INT64, one repeated INT64, one
repeated STRING, and one typed-NULL BINARY at dense and quarter-dense selections over 64, 1,024, and
4,096 physical rows. Source and operator construction are excluded; bytes, items, and pull
allocations describe only materialization and are not end-to-end SQL claims.

## Correctness evidence

Deterministic tests cover mixed ordering, sparse compaction, empty progress, exact plan shapes,
invalid ordinals/types/limits, query ownership, and resource release. A property materializes a
non-NULL representative and typed NULL for every frozen logical type code, converts every produced
cell through the independent scalar decoder, and compares type and storage. Exhaustive injected
allocation failure covers construction and mixed source/fixed/variable/NULL pulls. The physical-plan
fuzzer exercises hostile untyped/typed constants and valid execution under sanitizers. Installed
consumer and self-contained-header checks protect the public boundary.

## Tradeoffs and next steps

Repeating a variable constant spends memory bandwidth but produces the one canonical vector format
understood by all current operators. A dictionary or shared-scalar encoding could reduce bytes only
after its ownership, accounting, and consumer semantics are accepted and measured. The immediate
semantic next step is checked computed vector expressions, followed by lowering exact bound SELECT
items into ordered output positions and expressions.

## Likely review questions

**Why must NULL be typed?** A physical column must know its logical type and canonical buffer rules;
`std::monostate` alone contains neither.

**Why is a constant nonnullable?** A non-NULL constant cannot produce NULL in any row. Exact
nullability improves shape checks and avoids a redundant validity bitmap.

**Why not retain one scalar and expose it as a vector?** Current operators consume canonical
physical columns. A scalar/dictionary view would introduce a new encoding and backing lifetime.

**Why build constants for empty selections?** The current chunk model preserves the nonzero physical
domain for progress chunks and requires every column to match it. The selection still exposes zero
logical rows.
