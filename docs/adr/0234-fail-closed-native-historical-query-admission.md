# ADR 0234: Fail-Closed Native Historical Query Admission

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, query, and temporal-history maintainers

## Context

SQL v1 parsing, binding, and the scalar reference engine support `FOR SYSTEM_TIME AS OF`. Physical
unary lowering deliberately remains source-agnostic so a caller can supply either current or
historically resolved rows. The native single-node service supplied only the current Manifest v1
append-only snapshot, but did not inspect the bound historical timestamp. It could therefore return
present rows for a historical query, which is a silent violation of system-time semantics.

The implemented temporal history path uses Temporal Mutation Command v1 and CSEG/Manifest v2.
`SingleNodeDatabase` currently owns Columnar Append v1 and Manifest v1 state, so it cannot prove or
resolve the requested historical boundary yet.

## Decision

After parse and bind, native query admission rejects any SELECT carrying `FOR SYSTEM_TIME AS OF`
with an explicit `NOT_SUPPORTED` execution failure before query resources, storage snapshots, or
physical operators are created. Unary and ASOF current-snapshot queries remain unchanged.

The physical lowerer is not changed: the timestamp selects a source snapshot rather than changing
unary operator semantics, and existing temporal callers may correctly lower a plan after resolving
history. Native admission must be replaced by real temporal-source selection when the single-node
owner composes the accepted temporal command and Manifest v2 recovery path.

## Consequences

Native historical SQL is temporarily unavailable but can no longer return a plausible, incorrect
current result. No durable or wire format changes. The protocol response is one terminal execution
error and no partial result frame.

## Validation

A focused service test first commits visible current rows, issues a historical aggregate, and
requires exactly one execution-failure frame with no result rows. Existing current unary and ASOF
queries remain covered by the service regression suite.

## References

- [ADR 0007](0007-event-time-system-time-and-row-versioning.md)
- [ADR 0070](0070-feature-pass-logical-boundaries.md)
- [ADR 0111](0111-query-accounted-temporal-vector-source.md)
- [ADR 0222](0222-bounded-native-vector-query-results.md)
