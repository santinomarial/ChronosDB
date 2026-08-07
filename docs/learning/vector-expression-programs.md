# Checked Vector Expression Programs

## Purpose and boundary

`VectorExpression` is the first computed-value substrate in the Phase 9 physical engine. It turns
typed numeric and Boolean source/constant leaves into one canonical fixed-width output column while
preserving the scalar reference engine's checked errors, SQL NULL, IEEE, decimal, and short-circuit
rules. It is a physical in-memory program, not SQL syntax, an optimizer IR, or a durable bytecode.

This version intentionally excludes casts, BETWEEN/IN, COALESCE, text case conversion,
`time_bucket`, aggregates, joins, and bound-SQL lowering.

## Public interfaces

`chronos/query/vector_expression.hpp` exports:

- source and typed-constant leaf instructions;
- unary and binary operation enums and instruction records;
- `VectorExpressionInstruction`, a closed four-alternative variant;
- exact `VectorExpressionShape` values;
- finite instruction/configuration limits; and
- `VectorExpression::create`, immutable instruction/shape access, maximum depth, and retained-byte
  reporting.

`ComputedColumnOutputPosition` in `chronos/query/column_output.hpp` owns one validated program. It
may be interleaved with source and constant positions in `ColumnOutputOperator` and
`ColumnOutputStage`.

## Program and validation model

Instructions are postorder SSA-like nodes. A unary operand or both binary operands must reference a
smaller instruction index. This makes cycles and forward references invalid without graph search.
The result is the final instruction. Creation walks once, deriving exact type, nullability, and
depth for every node.

The hard implementation maximum is 256 instructions. Callers may choose a smaller limit, and both
logical size and spare vector capacity must fit. Retained bytes include instruction and derived-
shape capacities. A physical pipeline additionally counts every program inside its stage
configuration.

Supported leaves are signed/unsigned integers, FLOAT32/FLOAT64, DECIMAL, BOOL, DATE, and
TIMESTAMP_NS. DATE/TIMESTAMP_NS are comparison-only. Signed families widen only within signed
types, unsigned only within unsigned types, and mixed floating operands produce FLOAT64. Decimal,
Boolean, and temporal operands must have exact matching types.

## Evaluation semantics

Each output row evaluates lazily from the result. A fixed 256-slot stack array memoizes values; the
supported scalar alternatives contain no variable payload, so successful evaluation performs no
heap allocation per row. A failing row may allocate its owned diagnostic message.

Lazy evaluation is observable correctness. `FALSE AND (1 / 0 > 0)` returns FALSE without executing
the invalid divisor. TRUE similarly short-circuits OR. The remaining SQL truth tables retain UNKNOWN
for NULL. Arithmetic NULL produces a typed NULL. IS NULL and IS NOT NULL are always nonnullable.

Signed and unsigned arithmetic checks before wraparound. Integer division/remainder reject zero;
the signed minimum divided by negative one is checked. FLOAT follows IEEE behavior, including
infinity and NaN. DECIMAL delegates to the same exact widened implementation as the scalar oracle.
Ordinary NULL comparisons produce UNKNOWN, and ordered comparisons with NaN produce FALSE.

## Materialization, ownership, and accounting

Before reservation, `ColumnOutputOperator` validates every program source against the actual input
type and nullability and computes the exact validity/value bytes. Nonempty sparse selections are
compacted to an identity domain; empty selections preserve the input physical progress domain.
Evaluation reads the mapped physical source row and writes canonical little-endian, decimal, BOOL,
and validity buffers directly.

The returned column owns all bytes. Query output credit is admitted while input credit remains
live, and it covers canonical buffers, owners, selection storage, and conservative allocation
overhead. Expression memo storage is bounded automatic thread-stack state rather than retained heap
memory. Any validation, arithmetic, allocation, or child failure requests cancellation and RAII
releases input/output credit.

## Complexity and performance evidence

For `R` materialized rows and `I` reachable instructions, evaluation is `O(R*I)` with fixed output
memory plus configuration. DAG memoization evaluates a reachable instruction at most once per row.
No vector intermediate is retained.

`materialize_checked_numeric_expression` measures `(source + 42) * 3 > source` over dense and
quarter-dense selections at 64, 1,024, and 4,096 physical rows. Source and program construction are
excluded. The benchmark records output bytes, rows, instruction count, density, and pull
allocations; it is not an end-to-end SQL claim.

## Correctness evidence

Deterministic tests cover every instruction family, exact shapes, invalid references/types/limits,
NULL, short-circuit errors, sparse compaction, runtime overflow, cancellation, and plan integration.
A fixed-seed 257-row property compares signed add/subtract/multiply/divide/remainder and comparison
outputs with an independent scalar model. Allocation-failure injection requires complete credit
release. The physical-plan fuzzer includes valid and hostile computed positions. Sanitizers,
self-contained headers, installation, and external-consumer compilation protect the boundary.

## Tradeoffs and next steps

The fixed memo array favors a simple allocation proof over cache efficiency, and per-row graph
dispatch is not expected to be the final hot kernel. Column-wise specialization or fusion should be
adopted only after profiles and must remain differential with this baseline. The immediate semantic
next steps are physical casts and the remaining scalar operations, then exact lowering from bound
SQL expressions into these programs.

## Likely review questions

**Why carry type and nullability on source leaves?** The program can be validated independently and
the physical plan/runtime can reject a stale or incorrectly wired source before output allocation.

**Why are operands earlier-only?** It proves acyclicity and bounds validation/evaluation without a
recursive construction API or graph allocator.

**Why is evaluation lazy if the program is postorder?** Instruction storage order makes validation
simple; SQL error semantics still require short-circuit execution from the result.

**Why not support STRING now?** Variable results need exact sizing and transformed-byte handling
without per-row allocation. That deserves a separate reviewed contract rather than a hidden scalar
fallback.
