# Checked ASOF physical plans

`PhysicalAsofPlan` connects finite source operators to a left-deep chain of bounded ASOF joins.
Each join has a unary preparation pipeline for the accumulated left input and another for its new
right source. A final unary pipeline performs post-join filtering, aggregation, ordering, visible
projection, and limiting once SQL lowering supplies those stages.

The important invariant is exact shape continuity. Plan creation computes each join's output,
including ASOF LEFT nullability widening and the match-presence column, and checks that the next
consumer declares exactly that shape. No execution-time chunk is needed for planning.

The plan owns all definitions and is reusable through const access. `instantiate()` consumes a
vector of operator owners. On any construction failure, ordinary RAII destroys both accumulated
and unused sources. Once execution begins, the binary operator requests query-wide cancellation
on sibling failure and releases retained chunks and memory reservations.

Creation is linear in the number of joins plus retained key/output configuration. Instantiation is
linear in join and unary-stage count. Row execution complexity remains the responsibility of the
operators; the current ASOF primitive uses bounded nested candidate comparison.

Useful review questions:

- Why is match presence distinct from NULL-extended right values?
- Which boundary proves a later left preparation consumes the prior join's exact output?
- Why does instantiation copy configuration instead of moving it out of the reusable plan?
- How are unconsumed sibling sources released if a preparation allocation fails?
