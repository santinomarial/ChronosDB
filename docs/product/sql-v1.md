# SQL v1 Contract

> **Status: implemented as the Phase 8 bounded scalar reference engine.** The custom lexer,
> parser, schema-version-stable binder, CREATE TABLE/INSERT statement binding, scalar SELECT
> execution, and EXPLAIN paths are implemented. `SUBSCRIBE` is parsed and bound but remains a Phase
> 11 execution concern; Phase 9 has bounded vector/resource substrates, a Boolean filter, and stable
> column-subset projection, but bound plans cannot yet run through a vectorized product engine. SQL
> v1 is deliberately smaller than the SQL standard. Unsupported syntax must produce a clear bind
> or parse error; it must not be accepted with different semantics.

The canonical types and table clauses are defined in the [data model](data-model.md). The custom parser, binder, scalar reference engine, and vector engine follow [ADR 0008](../adr/0008-custom-sql-and-vectorized-execution.md).

## Lexical conventions

- Unquoted identifiers match `[A-Za-z_][A-Za-z0-9_]*` and are folded to lowercase. Reserved
  keywords cannot be used unquoted. The type-name keywords `BINARY`, `BOOL`, `DATE`, `DECIMAL`,
  `FLOAT32`, `FLOAT64`, `INT8`, `INT16`, `INT32`, `INT64`, `STRING`, `SYMBOL`, `TIMESTAMP_NS`,
  `UINT8`, `UINT16`, `UINT32`, `UINT64`, and `UUID` are contextual rather than reserved and remain
  valid unquoted identifiers where the grammar expects an identifier.
- Double-quoted identifiers preserve case; embedded `"` is written `""`.
- String literals use single quotes and double an embedded quote: `'can''t'`.
- Decimal integer and floating literals are accepted; `_`, hexadecimal numbers, and locale-specific separators are not v1.
- `TRUE`, `FALSE`, and `NULL` are keywords. `NULL` has no type until context or an explicit `CAST` supplies one.
- Timestamp literals use `TIMESTAMP 'YYYY-MM-DD HH:MM:SS[.fffffffff]Z'`; UTC `Z` is required in v1 and more than nine fractional digits is rejected.
- Date literals use `DATE 'YYYY-MM-DD'`.
- Interval literals use `INTERVAL '<nonnegative integer> <unit>'`, where v1 units are `nanosecond`, `microsecond`, `millisecond`, `second`, `minute`, `hour`, and `day`, with optional plural `s`.
- Binary literals use `X'<even number of hex digits>'`; UUID values use `UUID '<canonical UUID>'`.
- `--` line comments and non-nested `/* ... */` comments are recognized.

## Expressions and precedence

From lowest to highest precedence:

1. `OR`
2. `AND`
3. prefix `NOT`
4. `=`, `<>`, `!=`, `<`, `<=`, `>`, `>=`, `IS [NOT] NULL`, `BETWEEN`, `IN`
5. `+`, `-`
6. `*`, `/`, `%`
7. unary `+`, unary `-`
8. literals, column references, function calls, `CAST`, and parenthesized expressions

V1 scalar expressions include typed literals, qualified/unqualified column references, arithmetic, comparisons, Boolean operators, `IS NULL`, inclusive `BETWEEN`, finite literal/expression `IN` lists, `CAST(expr AS type)`, `COALESCE`, `ABS`, `LOWER`, `UPPER`, and `time_bucket(interval, timestamp)`. Implicit conversion is limited to lossless widening within signed integers, within unsigned integers, and `FLOAT32` to `FLOAT64`; signed/unsigned, decimal/float, string/symbol, and timestamp/date crossings require `CAST` unless a later coercion specification says otherwise. `COALESCE` materializes its first non-NULL value at the bound common type rather than retaining a narrower argument type.

Required aggregates are `COUNT(*)`, `COUNT(expr)`, `SUM`, `AVG`, `MIN`, `MAX`, `VAR_POP`, and `VAR_SAMP`. Aggregate inputs may be filtered only through `WHERE` in v1; `FILTER`, ordered aggregates, and distinct aggregates are post-v1.

## Statement and query surface

SQL v1 requires:

- `CREATE TABLE` with the clauses used by the canonical [schemas](data-model.md#canonical-proposed-schemas);
- `INSERT INTO table [(columns)] VALUES (...) [, ...]` for correctness and small-scale use; native columnar ingest is a protocol operation with identical row semantics;
- `SELECT` with `WHERE`, `GROUP BY`, `ORDER BY`, and `LIMIT`;
- `EXPLAIN SELECT`, which returns a stable logical/physical-plan description format defined later; and
- `EXPLAIN ANALYZE SELECT`, which executes the query and reports measured operator counters without changing query semantics.

V1 analytical features include:

- `time_bucket(interval, ts)`, whose buckets are half-open `[start, start + interval)` aligned to Unix epoch UTC;
- `LATEST BY (keys) ON timestamp_expr`, which selects one current-visible row per key tuple by greatest timestamp, breaking ties by physical ordering key and then stable row-version identity;
- `ASOF [LEFT] JOIN`, whose `ON` expression may reference only the source being joined and sources
  introduced before it, and which for each left row selects the right row with matching equality
  keys and greatest right timestamp not greater than the left timestamp; a tie uses the right
  physical ordering key then row-version identity;
- `FOR SYSTEM_TIME AS OF TIMESTAMP ...`, which resolves to the greatest single-node committed position whose recorded system timestamp is not later than the literal, then runs normal snapshot visibility at that position; and
- `SUBSCRIBE SELECT`, whose eligible query subset and delivery records are governed by [live-query semantics](live-query-semantics.md).

`CREATE MATERIALIZED VIEW` is a planned post-v1 SQL phase after incremental operator semantics are validated. Distributed DDL, general updates/deletes, transactions, subqueries, CTEs, unions, window functions, arbitrary joins, recursive queries, triggers, stored procedures, user-defined functions, collations, and full SQL-standard compatibility are intentionally unsupported in v1. Corrections and tombstones use a versioned ingest operation whose final SQL spelling is deferred.

## Compact grammar

This EBNF is an orientation contract; token and clause details above remain normative.

```ebnf
statement       = create_table | insert | select | subscribe | explain ;
create_table    = "CREATE" "TABLE" ident "(" column { "," column } ")"
                  "EVENT" "TIME" ident
                  "ORDER" "KEY" "(" ident_list ")"
                  "PARTITION" "BY" expression
                  "SHARD" "KEY" "(" ident_list ")"
                  [ "DEDUP" "KEY" "(" ident_list ")" ]
                  "RETENTION" interval
                  "SYSTEM" "HISTORY" "RETENTION" interval
                  "ALLOWED" "LATENESS" interval ;
column          = ident type [ "NOT" "NULL" ] ;
insert          = "INSERT" "INTO" ident [ "(" ident_list ")" ]
                  "VALUES" row { "," row } ;
row             = "(" expression { "," expression } ")" ;
select          = "SELECT" select_item { "," select_item }
                  "FROM" source [ system_time ] [ latest_by ]
                  { asof_join } [ where ] [ group_by ] [ order_by ] [ limit ] ;
source          = ident [ "AS" ident ] ;
system_time     = "FOR" "SYSTEM_TIME" "AS" "OF" timestamp_literal ;
latest_by       = "LATEST" "BY" "(" ident_list ")" "ON" expression ;
asof_join       = "ASOF" [ "LEFT" ] "JOIN" source "ON" expression ;
where           = "WHERE" expression ;
group_by        = "GROUP" "BY" expression { "," expression } ;
order_by        = "ORDER" "BY" order_item { "," order_item } ;
order_item      = expression [ "ASC" | "DESC" ] [ "NULLS" ( "FIRST" | "LAST" ) ] ;
limit           = "LIMIT" unsigned_integer ;
subscribe       = "SUBSCRIBE" select ;
explain         = "EXPLAIN" [ "ANALYZE" ] select ;
select_item     = expression [ "AS" ident ] | "*" | ident "." "*" ;
expression      = primary | unary | expression binary_op expression ;
primary         = literal | column_ref | function_call | cast | "(" expression ")" ;
```

## Deterministic semantics

- **NULL:** ordinary comparison with `NULL` returns `UNKNOWN`, including `NULL = NULL`. `NOT UNKNOWN` is `UNKNOWN`; `WHERE` retains only `TRUE`. Use `IS NULL`. Grouping places NULLs in one group.
- **Integer and decimal overflow:** never wraps. Scalar overflow fails the statement. `SUM` uses an exact widened accumulator sufficient for the declared input precision and errors if its specified result type cannot represent the final value.
- **Decimal scale:** arithmetic preserves the operands' identical declared `DECIMAL(p,s)` type. Multiplication rescales its exact coefficient by `10^s`; division scales the dividend coefficient by `10^s`. Division, multiplication rescaling, decimal-to-integer casts, and casts to a smaller decimal scale discard fractional digits toward zero. Explicit finite floating-to-decimal casts convert the exact IEEE value and discard fractional digits toward zero. Any final coefficient outside the declared precision fails the statement.
- **Floating NaN:** follows IEEE equality (`NaN = x` is false and `NaN <> x` is true, including another NaN); ordered comparisons with NaN are false. For `ORDER BY`, NaNs sort after `+infinity` and before NULL in ascending order, reversed for descending. `SUM`/`AVG` propagate NaN; `MIN`/`MAX` use this total ordering after ignoring NULL.
- **Division by zero:** integer and decimal division/remainder fail the statement. Floating division follows IEEE 754 and may produce infinity or NaN.
- **Timestamp precision:** all timestamp arithmetic and comparison uses exact nanoseconds. Inputs with excess precision are rejected, not rounded. Overflow is an error.
- **Equal sort keys:** `ORDER BY` breaks ties by stable logical row identity, then system commit position and row ordinal. These hidden tie-breakers affect presentation only and must survive compaction. An aggregate result tie uses its encoded group key.
- **Unordered results:** without `ORDER BY`, a result is a multiset; row order may change with plan, parallelism, flush, or compaction and is not testable API behavior.
- **Empty aggregates:** `COUNT` returns zero. `SUM`, `AVG`, `MIN`, `MAX`, `VAR_POP`, and `VAR_SAMP` return NULL; grouped queries with no groups return no rows.
- **Aggregate ordering:** an aggregate expression used only by `ORDER BY` still makes the statement
  an aggregate query. Projected and ordering expressions must then satisfy the same grouping rules.
- **Aliases:** v1 requires `AS`. A select alias is visible to `ORDER BY`, not to `WHERE` or `GROUP BY`. Table aliases replace the original qualifier within the query.
- **Duplicate names:** an unqualified ambiguous reference is a bind error. Explicit select items must have unique output names; joins using `*` qualify colliding names as `alias.column`, otherwise binding fails if no unique qualifier exists.
- **ASOF and LATEST determinism:** their key, time, and tie-break expressions must be bound and deterministic; volatile functions are not part of v1.

## Planned evolution

System-position literals, richer interval/calendar semantics, time zones, streaming materialized-view DDL, post-v1 expression functions, broader joins, and distributed system-time coordination require separate specifications. Grammar acceptance does not imply an operator is implemented; phase status is reported by the repository, not by this contract.
