# Source-Column Output Materialization

## Purpose and boundary

`SourceColumnOutputOperator` turns selected cells from an existing physical chunk into a new owned
output chunk. Unlike stable projection pushdown, it supports caller order and duplicate source
columns. It is the allocation and lifetime baseline for later typed output expressions, not itself
a computed-expression engine or SQL planner.

## Public interface

`chronos/query/column_output.hpp` exports:

- `kMaximumSourceColumnOutputWidth`, the 4,096-position retained-plan bound; and
- `SourceColumnOutputOperator::create(input, ordinals, limits)`, which returns the standard unique
  `PhysicalOperator` interface.

`SourceColumnOutputStage` in `chronos/query/physical_plan.hpp` carries the same ordinal vector and
output limits. Plan validation applies it to the current physical shape in order, so duplicate and
reordered outputs retain exact logical type parameters and nullability.

## Data structures and canonicalization

The operator retains only its child, input ordinals, finite limits, and sticky-end bit. Each pull
builds an `OutputPlan` with output physical rows, whether selected rows are compacted, and a
conservative query-memory charge.

For a nonempty selection, output row `r` reads the physical source row at selection index `r` and
the output receives an identity selection. Nullable validity and Boolean values are packed
LSB-first. Fixed-width cells copy their exact canonical bytes. Variable-width cells receive
little-endian `uint32_t` offsets rebased to a newly concatenated value buffer. NULL fixed/Boolean
payload positions remain zero, while NULL variable offsets repeat the preceding boundary.

For an empty selection, the nonzero input physical domain is retained and copied, then paired with
an empty selection. For an empty output-column list, only row cardinality and selection are
materialized.

## Ownership, lifetime, and accounting

The input `AccountedVectorChunk` stays alive during both the size pass and copy pass. The size pass
performs checked arithmetic and no output allocation. It accounts for selection indices, every
canonical buffer, the owned-column container, and conservative per-allocation overhead. Both
logical-buffer and retained-byte limits are checked before requesting output credit.

Only after `resources.reserve()` succeeds are output columns and selection allocated. Output
columns own all copied bytes; duplicates never alias each other or the input. When the pull returns,
the input owner destructs and releases its credit, while the returned output owns its separate
reservation. On failure, both reservations and all partial buffers unwind through RAII.

This simultaneous input/output reservation is intentional. Releasing input credit before the last
source read would let the query budget promise memory already retained by the input backing.

## Failure behavior

- A null child or zero limit is `INVALID_ARGUMENT`.
- Too many output positions, size overflow, byte-limit excess, budget denial, `bad_alloc`, or
  container length failure is `RESOURCE_EXHAUSTED`.
- An output ordinal missing from the runtime chunk is `OUT_OF_RANGE`.
- A chunk charged to another query is `INVALID_ARGUMENT`.
- Child and local errors request shared cancellation and preserve the original status.
- Successful end is sticky and releases the child; empty selected chunks remain normal progress.

The operator is thread-affine. It adds no atomics or locks. Query resource accounting and
cancellation retain ADR 0021's existing atomic ordering argument.

## Complexity

For `P` output positions and `S` materialized rows, fixed-width and Boolean work is `O(P*S)`.
Variable-width columns make one size pass and one copy pass, `O(P*S + V)` for copied value bytes
`V`. Memory is `O(output canonical bytes + S selection indices)`. Repeating a source column repeats
both work and storage; this baseline performs no alias or common-subexpression optimization.

## Correctness and measurement

Deterministic tests compare reordered and duplicate fixed, Boolean, nullable, and variable cells;
preserve empty progress and zero-column cardinality; exercise plan shape transitions and runtime
limits; and check independent duplicate buffers. A frozen-type property covers all 18 logical type
codes. Exhaustive allocation-failure injection requires complete reservation cleanup. The physical-
plan fuzzer drives hostile stages and valid duplicate-output execution under sanitizers.

`chronos_query_benchmarks` runs `materialize_reordered_duplicate_source_columns` at 64, 1,024, and
4,096 physical rows, four or eight input columns, and dense or quarter-dense selections. Every
source column is emitted twice in reverse order. Source construction is paused; reported bytes,
items, allocation count, width, and density describe one materialization pull. They do not claim
end-to-end SQL performance or justify fusion.

## Tradeoffs and next steps

Owned copying gives simple independent lifetimes and a canonical positional domain, at the cost of
memory bandwidth and peak coexistence with the input. A later aliasing path must prove backing and
credit lifetime and should be adopted only with profile evidence. Typed constants now use the same
ownership boundary through the separate
[typed-constant output guide](typed-constant-output-materialization.md). Checked computed
numeric/Boolean expressions now share the boundary through the
[vector-expression guide](vector-expression-programs.md); remaining scalar operations and
bound-SQL lowering remain next.

## Likely review questions

**Why not use `ColumnSubsetOperator`?** It only moves a strictly increasing unique subset in place.
One move-only column owner cannot satisfy two duplicate output positions.

**Why compact sparse selected rows?** New owned output positions need a common positional domain.
Identity output improves locality and avoids retaining unselected physical values.

**Why copy every row for an empty selection?** Current vectors require nonzero physical rows and
columns whose row counts match that domain. Copying preserves exact physical contents without
inventing a zero-row exception; the selection still exposes no result rows.

**Why reserve while input credit is still held?** Both allocations coexist until copying finishes.
Charging only one would understate the query's actual peak and break bounded admission.

**Are duplicate columns allowed to share buffers?** Not in this baseline. Independent owners make
lifetime and future mutation-by-construction rules explicit; aliasing needs its own backing proof.
