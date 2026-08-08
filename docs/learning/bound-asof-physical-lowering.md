# Bound ASOF physical lowering

Bound ASOF lowering bridges the binder's source-aware expressions to a checked left-deep physical
plan. Each source starts as all schema columns plus the shared row-version suffix. Before a join,
the lowerer appends only the equality and timestamp expressions needed on that side. The ASOF
operator then emits the persistent source layouts and a match bit, leaving temporary expressions
behind.

Joined ordinals are deterministic. Source zero begins at zero. Every later source begins after the
complete preceding joined layout, and its match-presence column follows its source block. These
offsets drive WHERE, projection, grouping, aggregation, ordering expressions, and hidden identity
keys. ASOF LEFT uses the actual widened physical shapes when compiling expressions; it does not
trust the source schema's original nullability.

The stage order is:

1. optional primary LATEST BY;
2. ASOF joins in SQL dependency order;
3. WHERE;
4. aggregate input preparation and aggregation, when present;
5. visible and ORDER BY expression preparation;
6. sort with explicit SQL keys and identity ties;
7. hidden-column removal; and
8. LIMIT.

Base-row ordering requires a DEDUP KEY for every source. Presence precedes each optional source's
logical identity, and all source row-version triples follow the logical identities. Aggregate
ordering instead appends group-key identity. All temporary and identity columns are absent from the
final plan output.

Allocation failure is translated to a resource diagnostic. Plan and operator ownership remains
RAII: a failed instantiation or pull releases every sibling source, chunk, reservation, and helper.

Useful review questions:

- Why must join operands be prepared separately for the left and right inputs?
- How does ASOF LEFT change expression and aggregate input nullability?
- Why is presence required even when all right output values happen to be NULL?
- Which stages must occur before and after ASOF to match the scalar oracle?
