# Checked Vector Expression Programs

## Purpose and boundary

`VectorExpression` is the first computed-value substrate in the Phase 9 physical engine. It turns
typed source/constant leaves into one canonical physical output column while
preserving the scalar reference engine's checked errors, SQL NULL, IEEE, decimal, temporal, UUID,
and short-circuit rules. It is a physical in-memory program, not SQL syntax, an optimizer IR, or a
durable bytecode.

The current program includes the fixed-width SQL v1 scalar intersection plus variable-width
STRING/SYMBOL casts, lazy COALESCE, ASCII LOWER/UPPER, comparisons, and NULL predicates. Bound
lowering expands BETWEEN/IN, inserts checked fixed-width casts, folds lazy COALESCE chains, and
emits exact `time_bucket` nodes. Aggregates and joins remain outside this boundary.

## Public interfaces

`chronos/query/vector_expression.hpp` exports:

- source and typed-constant leaf instructions;
- unary, cast, and binary operation records;
- `VectorExpressionInstruction`, a closed five-alternative variant;
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

Supported leaves are signed/unsigned integers, FLOAT32/FLOAT64, DECIMAL, BOOL, DATE, TIMESTAMP_NS,
UUID, STRING, and SYMBOL. Signed families widen only within signed types, unsigned only within
unsigned types, and
mixed floating operands produce FLOAT64. Decimal, Boolean, temporal, and UUID binary operands must
have exact matching types. Checked casts separately admit numeric family crossings and DATE/
TIMESTAMP_NS conversion. Variable programs admit only STRING/SYMBOL casts, LOWER/UPPER, and exact-
type COALESCE. Exact-type text comparisons and NULL predicates return BOOL without converting the
borrowed operands to owned scalar strings.

## Evaluation semantics

Each output row evaluates lazily from the result through one fixed 256-slot hybrid memo. A slot
contains either a fixed-width `ScalarValue` or a borrowed byte span with NULL state and an
identity/lower/upper transform. Input spans borrow the immutable chunk; constant spans borrow the
retained program. Applying a later case operation replaces the transform because the last lower/
upper operation determines the case of ASCII letters. Successful evaluation performs no heap
allocation per row; a failing row may allocate its owned diagnostic message.

Lazy evaluation is observable correctness. `FALSE AND (1 / 0 > 0)` returns FALSE without executing
the invalid divisor. TRUE similarly short-circuits OR. The remaining SQL truth tables retain UNKNOWN
for NULL. Arithmetic NULL produces a typed NULL. IS NULL and IS NOT NULL are always nonnullable.

COALESCE uses the same lazy rule: a non-NULL left branch prevents evaluation of the right. Its
operands have already been cast to one bound common type, and its result is nullable only when both
branches are nullable. `time_bucket` checks a positive nanosecond width and floors negative points
to the lower epoch-aligned boundary.

Signed and unsigned arithmetic checks before wraparound. Integer division/remainder reject zero;
the signed minimum divided by negative one is checked. FLOAT follows IEEE behavior, including
infinity and NaN. DECIMAL delegates to the same exact widened implementation as the scalar oracle.
Ordinary NULL comparisons produce UNKNOWN, and ordered comparisons with NaN produce FALSE.
STRING/SYMBOL comparisons require one exact logical type and use lexicographic unsigned UTF-8 byte
order after applying each operand's case transform; a shared prefix sorts first. Text NULL
predicates inspect only the borrowed NULL state and are always nonnullable.
Fixed-width casts match the scalar reference range and truncation rules, including decimal
rescaling, finite float-to-integer checks, narrow integer admission, and floor conversion between
negative TIMESTAMP_NS and DATE.

## Materialization, ownership, and accounting

Before reservation, `ColumnOutputOperator` validates every program source against the actual input
type and nullability and computes exact validity/offset/value bytes. Variable output uses a borrowed
size pass, checks UINT32 offset reachability, then reevaluates rows after reservation and transforms
directly into one canonical owned value buffer. Nonempty sparse selections are
compacted to an identity domain; empty selections preserve the input physical progress domain.
Evaluation reads the mapped physical source row and writes canonical little-endian, decimal, BOOL,
and validity buffers directly.

The returned column owns all bytes. Query output credit is admitted while input credit remains
live, and it covers canonical buffers, owners, selection storage, and conservative allocation
overhead. Expression memo storage is bounded automatic thread-stack state rather than retained heap
memory. Any validation, arithmetic, allocation, or child failure requests cancellation and RAII
releases input/output credit.

## Complexity and performance evidence

For `R` materialized rows, `I` reachable instructions, and `B` variable payload bytes, fixed output
is `O(R*I)` and variable output is `O(R*I + B)` per pass. Variable output currently makes one
planning/sizing pass plus one direct writing pass during materialization; no vector or
transformed-string intermediate is retained.

`materialize_checked_numeric_expression` measures `(source + 42) * 3 > source` and
`materialize_fixed_width_cast_and_coalesce` measures `coalesce(NULL, CAST(source AS FLOAT64))` over
dense and quarter-dense selections. `materialize_variable_width_case_and_cast` measures direct
LOWER plus STRING-to-SYMBOL materialization over 16-byte values and the same densities.
`materialize_borrowed_text_predicate` measures LOWER plus
16-byte STRING equality without transformed-string allocation. All expression benchmarks run over
dense and quarter-dense selections at 64, 1,024, and 4,096 physical rows. Source and program
construction are excluded. The benchmarks record output bytes, rows, instruction count, density,
and pull allocations; they are not end-to-end SQL claims.

## Correctness evidence

Deterministic tests cover every instruction family, exact shapes, invalid references/types/limits,
NULL, short-circuit errors, casts, COALESCE, negative time bucketing, all text comparisons, sparse
compaction, runtime overflow, cancellation, and plan integration. Fixed-seed 257-row properties
compare arithmetic and transformed unsigned-byte text order with independent models and bound
fixed-width scalar programs with the scalar SQL oracle.
Allocation-failure injection requires complete credit release. Lowering fuzzing includes valid and
unsupported scalar forms. Sanitizers, self-contained headers, installation, and external-consumer
compilation protect the boundary.

## Tradeoffs and next steps

The fixed memo array favors a simple allocation proof over cache efficiency, and per-row graph
dispatch is not expected to be the final hot kernel. Column-wise specialization or fusion should be
adopted only after profiles and must remain differential with this baseline. Single-source
nonaggregate bound SQL now lowers the complete fixed-width scalar subset into these programs.
Aggregate operators are next, followed by wider relational lowering.

## Likely review questions

**Why carry type and nullability on source leaves?** The program can be validated independently and
the physical plan/runtime can reject a stale or incorrectly wired source before output allocation.

**Why are operands earlier-only?** It proves acyclicity and bounds validation/evaluation without a
recursive construction API or graph allocator.

**Why is evaluation lazy if the program is postorder?** Instruction storage order makes validation
simple; SQL error semantics still require short-circuit execution from the result.

**Why use a hybrid memo for text predicates?** Comparisons return fixed-width BOOL but consume
variable-width operands. Carrying both fixed scalars and borrowed bytes in one lazy memo preserves
AND/OR short circuit and DAG reuse without scratch strings or a second expression representation.
