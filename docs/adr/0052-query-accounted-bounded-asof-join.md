# ADR 0052: Query-Accounted Bounded ASOF Join

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-execution maintainers

## Context

SQL v1 defines ASOF and ASOF LEFT joins independently of scan order: ordinary SQL equality keys
must match, the right timestamp must be no greater than the left timestamp, and the greatest
eligible right timestamp wins. A timestamp tie uses the right schema physical-ordering key and
then stable row-version identity. The scalar engine is authoritative but its row-owned nested loop
cannot be used as the vector product path.

The current physical plan is intentionally unary. Before choosing a relational-plan graph or scan
scheduler, Phase 9 needs a storage-independent two-input ownership boundary that can be tested
directly and later instantiated by a checked relational plan.

## Decision

- `AsofJoinOperator` owns two finite pull-based inputs. Before retaining either side, it reserves a
  conservative query charge for both chunk vectors, both row-reference vectors, the output-row
  references, and allocation overhead. Left rows, right rows, equality keys, physical keys, output
  width, output buffers, and retained bytes all have finite checked limits.
- The baseline algorithm is a bounded nested loop. Equality uses SQL semantics: NULL never matches
  and any floating comparison involving NaN is false. Eligibility requires non-NULL timestamps and
  `right_time <= left_time`.
- Winner comparison is greatest right timestamp, then greatest schema physical-ordering-key tuple
  under the scalar NULL-last total order, then greatest WAL ID, record sequence, and row ordinal.
  Every tie is explicit; input order and operator stability are irrelevant.
- Output is one independently owned canonical chunk containing configured left columns, configured
  right columns, and one final non-null Boolean match-presence column. ASOF LEFT null-extends every
  configured right column on a miss. The presence value remains distinct from nullable right data
  so later joined-row identity never guesses whether a row matched.
- Both exact input shapes and the non-null four-column right row-version suffix are checked before
  execution. Any input, shape, comparison, cancellation, allocation, or materialization failure
  cancels both siblings and destroys all retained chunks and reservations before returning.

## Consequences

The baseline has `O(L * R * K)` comparison work for `L` left rows, `R` right rows, and key/tie width
`K`, plus linear canonical output materialization. It intentionally supplies a structurally simple
vector oracle before an indexed, merge, or partitioned ASOF algorithm is selected by evidence.

The match-presence column is physical metadata, never client-visible SQL output. It is required for
exact left-join identity and must survive until any final presentation tie is constructed.

## Alternatives considered

- **Assume the right input is timestamp sorted:** makes correctness depend on an unproven scan or
  optimizer property and still needs equality partitioning and exact ties.
- **Hash equality groups and binary search time:** attractive future optimization, but requires a
  query-accounted variable-cardinality index and a cost/selection rule.
- **Infer match presence from NULL right columns:** incorrect when matched rows contain nullable
  values and impossible when every projected right value is NULL.
- **Use arrival order for equal timestamps:** violates the physical-key and row-version contract.

## Validation plan

Unit tests cover ASOF and ASOF LEFT, NULL keys/timestamps, no-match rows, timestamp, physical-key and
row-version ties, hidden-column selection, hostile shapes, finite limits, cancellation, and complete
credit cleanup. An allocation-failure sweep covers creation, retained-state vectors, output
planning, every canonical column buffer, and final ownership transfer. A hostile-definition fuzzer
exercises checked admission. Execution benchmarks vary both side cardinalities and equality-key
cardinality and report allocations and bytes. Public-header, installation, external-consumer,
ASan/UBSan, TSan, static-analysis, and full repository gates remain mandatory.

## Unresolved questions

Bound-ASOF expression extraction, multi-source physical plan shape, snapshot-source instantiation,
joined ORDER BY identity lowering, indexed/merge optimizer selection, parallel scheduling, and spill
are subsequent Phase 9 decisions.

## References

- [SQL v1](../product/sql-v1.md)
- [ADR 0008](0008-custom-sql-and-vectorized-execution.md)
- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0022](0022-pull-based-physical-operator-lifecycle.md)
- [ADR 0045](0045-shared-vector-row-version-suffix.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
