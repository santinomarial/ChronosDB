# Bounded Physical ASOF Join

## Purpose and interface

`AsofJoinOperator` is the exact two-input vector primitive for SQL v1 temporal lookup joins. Its
definition records both complete input shapes, paired equality-key columns, left and right
timestamp columns, the right schema physical-ordering columns, the right row-version suffix, and
the columns retained from each side. `AsofJoinLimits` bounds both row sets, both key lists, state
credit, and canonical output.

The operator is move-owned and thread-affine. It retains both input streams only after reserving
its worst-case state charge. Each input chunk keeps its original query reservation while buffered.

## Match and winner semantics

For every left row, the nested-loop baseline visits each right row:

1. Every equality-key pair must be SQL TRUE. NULL never equals NULL here, and NaN never equals any
   floating value, including another NaN.
2. Both timestamps must be present and the right timestamp must be no greater than the left.
3. Among eligible candidates, the greatest right timestamp wins.
4. A timestamp tie selects the greatest right physical-ordering-key tuple with the scalar
   NULL-last total order, then greatest WAL ID, record sequence, and row ordinal.

The winner proof therefore contains no scan-arrival or stable-sort assumption. The direct nested
loop is also deliberately different from later indexed implementations, making it useful for
differential testing.

## Output, null extension, and presence

One canonical output chunk gathers configured left and right columns. An inner ASOF miss emits no
row. An ASOF LEFT miss emits the left columns and typed NULLs for every right column, widening
right physical nullability as needed.

The final output column is a non-null Boolean match bit. It is physical metadata for later joined
logical identity: a missing right row must sort distinctly from a present row whose projected key
or payload values happen to be NULL. Relational lowering must remove this and all row-version/helper
columns before returning client output.

## Bounds, ownership, and failures

State memory is proportional to the configured maximum left and right rows, not the observed input.
This conservative reservation happens before vector capacity is allocated. Runtime verifies actual
capacities stay within the charge. Output bytes are planned across fixed, Boolean, and variable
columns before a separate reservation and before any output allocation.

Any failure requests shared cancellation, drops both input subtrees and buffered chunks, and then
returns the original status. Successful output owns independent buffers, so destroying join state
releases all input pins and credit before the caller receives the chunk.

Complexity is `O(L * R * K)` comparison time and `O(L + R)` retained row-reference state, plus
linear output materialization. A hash/time index or merge plan can replace this only with the same
NULL/NaN, timestamp, tie, presence, accounting, and cancellation behavior.

## Interview questions

**Why does NULL equality differ from grouping equality?** SQL ON equality with NULL is UNKNOWN and
does not match. GROUP BY and LATEST key identity deliberately place NULLs in one group.

**Why is presence a separate column?** Nullable right values cannot distinguish a missing row from
a matched row containing NULL. Exact joined-row identity needs that distinction.

**Why materialize one chunk?** It gives the first implementation a finite, independently owned
boundary that composes with existing unary operators. Streaming batches and spill require their own
ownership and ordering decisions.

**Why start with a nested loop?** It is bounded, auditable, and structurally independent of an
eventual optimized index, which makes semantic differential failures easier to diagnose.
