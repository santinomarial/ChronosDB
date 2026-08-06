# SQL v1 Scalar Reference Engine

## Purpose and phase boundary

`chronos_query` is the Phase 8 correctness oracle for ChronosDB SQL v1. It owns a bounded custom
lexer and parser, immutable catalog snapshots, schema-version-stable binding, exact scalar values
and expression evaluation, a deliberately simple relational executor, CREATE TABLE and INSERT
binding/materialization, and stable EXPLAIN output. It exists to make SQL meaning executable before
Phase 9 introduces vector batches, parallel scheduling, spill, cancellation, or cost-based choices.

The implementation is pure in-memory code. It does not mutate a live catalog, scan CSEG files,
submit INSERT rows to the WAL, route tablets, publish mutable heads, or execute subscriptions. A
`ScalarSnapshotProvider` is the explicit boundary through which tests or a future storage adapter
supply already-visible immutable rows at an exact schema and commit boundary. `SUBSCRIBE SELECT` is
accepted syntax so Phase 11 can build on one grammar and binder, but every Phase 8 execution entry
point rejects subscription mode.

This separation matters: the scalar engine is authoritative for values and errors, not for the
eventual physical performance architecture. A Phase 9 operator is correct only when it matches this
oracle under the same bound schema, snapshots, SQL mode, and resource limits.

## Public interfaces

The public headers under `include/chronos/query/` are self-contained and installed with the
`chronos::query` target:

- `lexer.hpp` owns normalized token text and exact byte/line/column spans. `SqlLexerLimits` bounds
  input bytes, token count, and token bytes before unbounded work or allocation.
- `ast.hpp` defines move-owned statement and expression trees. Child order is semantic and source
  spans remain stable identifiers inside one parsed tree.
- `parser.hpp` exposes separate SELECT-family, CREATE TABLE, and INSERT entry points with common
  AST-node, depth, and list bounds.
- `catalog.hpp` creates an immutable, generation-tagged mapping from canonical table names to
  `shared_ptr<const TableSchema>`. A bound plan retains this snapshot.
- `binder.hpp` resolves SELECT sources, columns, types, aliases, aggregate/grouping rules, LATEST,
  and ASOF shapes. Execution never performs name lookup. The source limit is hard-capped at 64
  because the bounded plan represents referenced sources with one checked 64-bit mask.
- `literal.hpp` parses normalized timestamp, date, interval, integer, floating, and UUID payloads.
- `value.hpp` owns every SQL v1 scalar representation and implements SQL equality plus deterministic
  total ordering.
- `evaluator.hpp` evaluates a bound scalar expression against borrowed source rows, projected
  outputs, or aggregate overrides for one synchronous call.
- `snapshot.hpp` owns validated rows, logical/version identities, the exact schema, and a committed
  position. `ScalarSnapshotProvider` resolves current or system-time snapshots.
- `executor.hpp` executes one bound SELECT and optionally returns deterministic operator-work
  counters.
- `statement_binder.hpp` validates CREATE TABLE roles/policies, materializes an initial schema only
  from caller-allocated durable identities, binds INSERT target ordinals, and materializes
  source-free VALUES expressions into complete schema-ordinal rows.
- `explain.hpp` emits stable format version 1 logical/scalar-physical plan text and executes EXPLAIN
  ANALYZE once with measured counters.

All result-producing interfaces return `SqlResult<T>`. A failure contains a stable
`SqlDiagnosticCode`, the most relevant source span, and a structured `common::Status`. No public
query object borrows SQL input bytes after its parse call returns.

## Pipeline and ownership

The read path is deliberately explicit:

```text
SQL bytes
  -> owned tokens
  -> owned AST
  -> immutable catalog snapshot + exact TableSchema pointers
  -> move-owned BoundSqlSelect
  -> provider-owned immutable ScalarTableSnapshot objects
  -> owned ScalarQueryResult
```

`BoundSqlSelect` move-owns the parsed tree and retains the catalog snapshot. Its source list retains
the exact schema objects used for binding. Advancing a live `SchemaLineage` or replacing a newer
catalog generation therefore cannot alter an existing plan. The executor checks that every provider
result has a schema value exactly equal to the bound schema before reading rows.

`ScalarEvaluationContext` is the only borrowed evaluation object. Its spans and override pointers
must remain valid for the synchronous evaluation call; the evaluator stores none of them.
`ScalarQueryResult`, `MaterializedSqlInsert`, tokens, ASTs, bound plans, values, and snapshots own
their variable data.

CREATE TABLE intentionally separates semantic binding from durable identity allocation. SQL text
defines names, logical types, nullability, roles, and interval policies, but a control-plane owner
must supply nonzero `TableId`, `SchemaId`, and declaration-ordered `ColumnId` values before
`materialize_sql_v1_table_schema()` can create a `TableSchema`. The query layer never derives a
durable UUID from a name or process-local counter.

INSERT uses an internal bound constant-expression plan to reuse the SELECT binder and evaluator.
Every VALUES expression is wrapped in an internal explicit cast only after its original inferred
type is checked against the narrower assignment rule. Thus the wrapper cannot accidentally turn an
explicitly permitted numeric/text/temporal CAST into an implicit INSERT conversion. Column
references, stars, and aggregates are rejected because VALUES has no input source. Materialization
fills omitted nullable columns with typed NULL and rejects an evaluated NULL for a non-null column.

## Lexical and parsing invariants

The lexer works on arbitrary bytes and never assumes NUL termination or valid UTF-8 outside the
places the grammar requires it. Unquoted identifiers are ASCII and folded to lowercase; quoted
identifiers preserve valid UTF-8 text. Strings own their unescaped payload. Binary tokens own decoded
bytes in a string-shaped container and are copied into `std::byte` storage during scalar evaluation.

The parser is recursive only under `maximum_expression_depth`. Flat lists are bounded independently
and every AST node is charged to `maximum_ast_nodes`. Clause order is exact. One optional trailing
semicolon is accepted, while extra tokens or terminators are deterministic errors. Parsing a known
keyword does not imply later execution support: mode validation remains an execution responsibility.

The AST is not a public construction API. Private constructors ensure nodes originate from the
bounded parser, with one narrow friendship used by INSERT binding to build an internal constant
plan. That plan uses synthetic non-source spans only for its wrapper casts; original operand spans
remain the diagnostic and type-lookup identities exposed to callers.

## Binding and schema stability

Binding performs all name resolution before execution:

- table aliases replace original qualifiers;
- unqualified references fail if more than one source supplies the name;
- column references record source ordinal, schema ordinal, `TableId`, `ColumnId`, exact logical type,
  and nullability;
- select aliases are visible only to ORDER BY;
- stars expand in source/schema ordinal order, and colliding names become qualified only under the
  specified rule;
- every explicit output name is unique;
- expression records are keyed by source span and carry type, nullability, aggregate presence, and
  optional projected-output ordinal.

Implicit conversion is only lossless widening inside signed integers, inside unsigned integers, and
FLOAT32 to FLOAT64. Signed/unsigned, decimal/float, text/symbol, and date/timestamp crossings require
an explicit CAST. Untyped NULL is accepted only where context supplies a target, such as CAST,
COALESCE, comparison, or INSERT assignment. COALESCE materializes the chosen non-NULL argument at
the bound common type. The binder rejects unsupported functions, nested
aggregates, aggregate/non-grouped column mixtures, and untyped result columns.

LATEST keys resolve to primary-source schema ordinals, and its timestamp expression must be a
deterministic primary-source TIMESTAMP_NS expression. ASOF accepts a conjunction containing at least
one cross-source equality key and exactly one right timestamp not greater than a prior-source
timestamp. Each ASOF condition may reference only that right source and sources introduced before
it; later join aliases are rejected during binding. The bound plan records the right source and
right timestamp span so the executor does not rediscover temporal-join meaning.

CREATE TABLE binding requires a unique declared column set; a non-null TIMESTAMP_NS event column;
known, duplicate-free role lists; event time in physical ordering and partitioning; non-null shard
and dedup keys; shard as a subset of a present dedup key; an exact positive
`time_bucket(INTERVAL, event_time)` partition; positive event/history retention; and a nonnegative
allowed-lateness interval. The existing `TableSchema::create()` independently repeats schema-level
identity and role validation during materialization.

## Scalar values and expression semantics

`ScalarValue` stores a logical type separately from its variant payload. NULL uses `monostate` and
may be temporarily untyped only while evaluating a NULL literal. Signed temporal/date values share
`int64_t` storage where their logical type distinguishes them; unsigned, IEEE float, decimal,
string/symbol, binary, Boolean, and UUID values have explicit owned alternatives.

DECIMAL coefficients are canonical signed 128-bit little-endian bytes. Arithmetic does not depend
on a production big-integer library. Internal checked widened limbs provide exact intermediate
addition, subtraction, multiplication, division, remainder, rescaling, and aggregate accumulation.
Scale reduction truncates toward zero, and the declared precision is checked at the final boundary.
Integer operations similarly fail before wraparound. Floating operations preserve IEEE behavior,
including NaN and infinities.

SQL predicate evaluation uses TRUE, FALSE, and UNKNOWN. Ordinary NULL comparisons return UNKNOWN;
WHERE retains only TRUE. `sql_scalar_equal()` uses SQL equality, while
`compare_scalar_values()` is a separate deterministic total order for grouping and ORDER BY. NaN
sorts after positive infinity and before NULL in ascending order. Explicit NULLS FIRST/LAST is not
reversed by DESC; only non-null value comparison is reversed.

`time_bucket` requires a positive exact nanosecond interval and uses truncation toward the lower
epoch-aligned boundary, including timestamps before 1970. UTC timestamp parsing accepts at most
nine fractional digits and checks the complete signed-64-bit nanosecond range.

## Relational reference algorithm

The scalar executor favors auditability over asymptotic performance:

1. Resolve and validate every immutable source snapshot.
2. For LATEST, scan primary rows and retain one winner per typed key using timestamp, physical key,
   then WAL/record/row identity tie-breakers.
3. For each ASOF join, scan the right snapshot for every current left row, evaluate the bound
   condition, and retain the greatest eligible right timestamp with the same tie-breakers.
4. Apply WHERE using three-valued predicates.
5. Either project rows or form typed groups and evaluate aggregates. An aggregate appearing only in
   ORDER BY still selects the aggregate path and applies grouping validation to projected values.
6. Evaluate ORDER BY keys and sort with stable hidden logical/version tie-breakers.
7. Apply LIMIT and move values into an owned result.

COUNT ignores NULL except for COUNT(*). SUM uses an exact widened accumulator for integer/decimal
inputs and checks only the final declared result. AVG and variance return FLOAT64 and follow the
documented empty/NaN rules. MIN/MAX ignore NULL and use the same total order as deterministic
ordering. An empty global aggregate produces one row; an empty grouped aggregate produces none.

The executor never claims storage visibility logic. `ScalarTableSnapshot::create()` validates row
widths, types, nullability, identities, sequence fields, and commit boundaries, while the provider
is responsible for supplying rows already selected by snapshot visibility at its returned boundary.

## EXPLAIN and observability

EXPLAIN format version 1 is line-oriented and deterministic. It records mode, catalog generation,
hex-encoded exposed names and stable identities, exact schema versions, system time, logical
operator counts, output types/nullability/aggregate shape, limit, scalar engine name, and ordered
operator names. It reads no snapshot and reports no invented costs.

EXPLAIN ANALYZE accepts only a plan parsed in that mode, runs it exactly once, and returns the same
plan text plus the underlying result and measured counters:

- total source rows supplied;
- rows after LATEST;
- ASOF candidate comparisons;
- rows after WHERE;
- groups formed; and
- final output rows after LIMIT.

These counts are reproducible work observations, not wall-clock promises. Timing belongs to the
benchmark harness, where compiler, build type, hardware, data shape, and command line are recorded.

## Failure behavior and resource bounds

Lexical, parse, bind, literal, and execution failures remain distinct diagnostic classes. Hostile
bytes become lexical or parse diagnostics, not exceptions or undefined behavior. Unknown names,
ambiguity, duplicate output/target names, and type errors fail during binding. Arithmetic overflow,
division by zero, invalid runtime casts, provider mismatch, and evaluated non-null violations fail
execution/materialization. Allocation and container-length failures are translated to
`kResourceLimit` diagnostics with `kResourceExhausted` status.

Every public work entry has explicit bounds. The lexer bounds bytes and tokens; the parser bounds
nodes, depth, and list sizes; SELECT binding bounds sources, expression records, and outputs; INSERT
bounds rows and total values; snapshots and execution bound source, intermediate, output, and group
rows. A configured zero bound is invalid rather than an accidental request for unlimited work.

No failing operation partially mutates caller state. All plans/results are constructed privately
and returned only when complete. The Phase 8 code performs no durable or externally visible write.

## Complexity and tradeoffs

For SQL byte length `B`, token count `T`, and AST nodes `N`, lexing and parsing are `O(B + N)` with
owned storage proportional to normalized token/AST data. Binding uses intentionally simple linear
catalog/source scans and can be quadratic in small bounded name/output lists.

For `R` primary rows, `Q` right rows, `G` groups, `K` key width, and `J` ASOF joins:

- projection/filter work is `O(R)` times expression cost;
- LATEST is `O(R * G * K)` because the oracle uses a vector of winners;
- each ASOF join is `O(R * Q)` times condition/tie-break cost;
- grouping is `O(R * G * K)`;
- ORDER BY is `O(R log R)` comparisons; and
- result/snapshot memory is owned and linear in retained rows and values.

These costs are unsuitable as the production execution architecture. They are useful precisely
because the code is direct, bounded, and structurally different from the Phase 9 vector engine.
Replacing the vectors with hashes, indexes, SIMD, parallelism, or spill is an optimization task that
must remain differentially identical to this oracle.

## Verification and benchmark methodology

The test suite includes exact lexer/parser/EXPLAIN goldens, hostile byte and grammar failures,
source-span checks, catalog-generation/schema retention, type and alias errors, all logical value
domains, NULL/NaN ordering, integer/decimal boundary properties, aggregate reference comparisons,
system-time requests, LATEST/ASOF tie cases, INSERT/DDL failures, and resource limits. A deterministic
small-database generator independently models combined LATEST plus ASOF selection over many table
shapes. ASan/UBSan and TSan runs cover the query suite even though the Phase 8 executor itself has no
shared mutable state.

`chronos_sql_lexer_fuzz`, `chronos_sql_parser_fuzz`, and `chronos_sql_binder_fuzz` accept arbitrary
bytes under reduced hostile-input bounds. Parser fuzzing invokes every statement family. Binder
fuzzing pins a real schema and, when syntax reaches it, binds SELECT/CREATE/INSERT and materializes
valid INSERT rows. Run counts, maximum input length, seed, compiler, and sanitizer configuration
must accompany any reported campaign; a short passing run is a smoke test, not exhaustive proof.

`chronos_query_benchmarks` measures representative tokenization, parsing, parse-plus-bind,
expression evaluation, exact decimal evaluation, grouped scalar execution over a declared row set,
and multi-row INSERT bind/materialization. The scalar execution rate is a regression baseline, not a
product performance target. Benchmark results are admissible only after the same revision passes
correctness and sanitizer gates.

## Likely design and interview questions

**Why retain the catalog in a bound plan?** To make name/type resolution stable across concurrent
schema advancement. Execution consumes exact identities and ordinals, never whichever schema is
current later.

**Why keep SQL equality separate from total ordering?** SQL comparisons must return UNKNOWN for
NULL and false for ordered NaN comparisons, while grouping and ORDER BY require a deterministic
total order that can place every value.

**Why is ASOF quadratic?** This is an independent scalar oracle. A later indexed/vector merge must
match it; sharing the optimized algorithm would weaken differential evidence.

**Why can INSERT not reference target columns?** VALUES has no source row in SQL v1. Rejecting such
references avoids inventing defaults, per-row evaluation context, or UPDATE-like semantics.

**Why does CREATE TABLE take caller IDs?** Durable identity allocation is control-plane authority.
Deriving IDs inside parsing would conflate syntax with catalog mutation and make replay/coordination
unsafe.

**What does EXPLAIN ANALYZE measure?** Actual scalar operator work and the real result from one run.
It does not fabricate optimizer costs or claim nanosecond stability.

**What must Phase 9 prove?** For every supported plan, snapshot, resource/error case, NULL/NaN and
overflow boundary, batch division, and scheduling choice, the vector engine must equal this scalar
engine's multiset or deterministic ordered result and failure semantics.
