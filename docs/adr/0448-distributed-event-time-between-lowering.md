# ADR 0448: Distributed event-time BETWEEN lowering

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB SQL, query-planning, and distributed-query maintainers
- **Extends:** [ADR 0439](0439-schema-bound-distributed-row-sql-lowering.md)

## Context

Distributed row SQL accepted individual comparisons against the event-time column but rejected SQL
`BETWEEN`, even though the worker's exact open/closed range predicate can represent its inclusive
truth without a protocol change. Falling back to local execution would violate the distributed
request's authority and completeness boundary.

## Decision

The distributed row lowerer accepts positive `event_time BETWEEN lower AND upper` leaves inside the
existing `AND` tree when both bounds are bound `TIMESTAMP` literals. It normalizes the leaf to an
inclusive lower and upper `EventTimePredicate` bound, then intersects it with comparison leaves by
the existing tightest-bound rules.

Reversed bounds are retained as an empty range rather than reordered. `NOT BETWEEN`, a non-event-
time value, computed bounds, and non-TIMESTAMP bounds fail with source-spanned `NOT_SUPPORTED`
diagnostics. Worker execution remains the exact truth boundary; metadata pruning is not substituted
for row filtering.

## Consequences

Native distributed queries can express a common closed event-time window with the same fragment,
transport, and worker formats. Lowering remains allocation-neutral beyond the already owned plan.
This does not add computed output expressions, arbitrary predicates, grouping, aggregation, joins,
historical reads, or a local fallback.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): the normalized predicate remains part of the exact
  schema-bound logical query identity.
- [Invariant 13](../architecture/invariants.md): inclusive nanosecond endpoints are retained without
  increment, decrement, or overflow.
- [Invariant 18](../architecture/invariants.md): unsupported negation or bound shapes fail closed.

## Validation

Focused lowering tests cover inclusive normalization, intersection with a stricter open bound,
reversed empty ranges, `NOT BETWEEN`, and a non-event-time value. The replicated two-tablet Native
test executes an inclusive single-nanosecond window through the production mutable worker and
requires byte-identical output to the equivalent comparison query. All 400 query tests, 52 query
allocation-failure tests, and 106 service tests pass; the query and focused service paths also pass
under ASan/UBSan. Formatting and installed consumption pass. The LLVM 18 static-analysis target
passes with the lowering file's two existing missing-field-initializer warnings.

## Migration and rollback

This is an additive SQL-lowering rule with no durable or network byte change. Rollback removes the
new AST lowering branch; previously accepted distributed queries are unaffected.

## References

- [Distributed row SQL lowering](../learning/distributed-row-sql-lowering.md)
- [Timestamp predicate filtering](0030-timestamp-predicate-filtering.md)
