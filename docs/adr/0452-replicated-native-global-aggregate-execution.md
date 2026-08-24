# ADR 0452: Replicated Native global aggregate execution

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB SQL, query, replicated-service, cluster, and daemon maintainers
- **Extends:** [ADR 0444](0444-proof-revalidated-local-and-remote-native-row-merge.md),
  [ADR 0447](0447-fresh-all-group-native-query-rebinding.md),
  [ADR 0450](0450-schema-bound-distributed-global-aggregate-sql-lowering.md),
  [ADR 0451](0451-bounded-mutable-row-global-aggregate-finalization.md)

## Context

Global aggregate SQL could be lowered, and a complete mutable all-tablet row result could be
accumulated and Native-encoded, but no production owner composed those pieces. The replicated
Native service still sent every SELECT through row-only lowering, so aggregate requests failed
before authority binding. `chronosd` consequently had no distributed aggregate path even though it
already packaged authenticated mutable query workers and routes.

## Decision

`DistributedVectorAggregateSqlPlan` owns two inseparable views of one bound query:

- `input_rows`, an unlimited unordered identity row plan with the unique aggregate projection,
  exact source descriptors, event-time truth, and no client LIMIT; and
- the ungrouped aggregate intent plus client result schema, including the global LIMIT.

Lowering constructs and validates both views from the same bound schema snapshot. The input row
descriptors use source column names/types/nullability and remain aligned one-to-one with projection
indices. A COUNT(*)-only plan carries its explicit event-time anchor in that input plan.

The replicated Native service now selects row or aggregate lowering after binding. Both modes use
the existing correlated snapshot, all-group read authorities, one nonnil query ID, local proof-
revalidating worker, remote mutual-TLS scheduler, whole-attempt retry/rebinding, deadline, and exact
cancellation path. Aggregate retries discard all rows from the failed attempt. Once every tablet
stream closes, row mode uses global row finalization while aggregate mode uses the bounded mutable-
row aggregate finalizer and returns exactly one schema-bearing QUERY_RESULT followed by QUERY_END.
LIMIT zero still sends the schema-bearing zero-row result.

No new listener, message type, durable format, or authenticated principal policy is introduced.
The packaged daemon inherits aggregate support because it already owns this service and mutable
query plane.

## Consequences

Correctness and authority behavior are identical for direct rows and admitted global aggregates.
The transitional aggregate data path still transfers matching projected rows rather than worker
sufficient state, so its configured input row/byte/memory limits can reject a large query with
`RESOURCE_EXHAUSTED`; it never truncates. Worker-side pushdown remains a future performance change,
not a correctness prerequisite.

Aggregate output is independently bounded by Native service row, column-name, payload, and protocol
limits. The execution owner is synchronous on one request thread; remote scheduler and cancellation
memory-ordering arguments are unchanged from ADRs 0444, 0446, and 0447. This change adds no
acknowledged-write guarantee.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): input and output plans originate from one bound
  catalog snapshot and are rebound only through fresh correlated authority.
- [Invariant 10](../architecture/invariants.md): no aggregate scalar is published before every
  selected tablet stream terminates.
- [Invariant 14](../architecture/invariants.md): local, remote, and daemon paths share the same
  canonical aggregate kernel and Native finalizer.
- [Invariant 15](../architecture/invariants.md): source rows, messages, bytes, state, deadlines,
  retries, and response payloads are all finite.
- [Invariant 18](../architecture/invariants.md): unsupported grouping, computed inputs, ordering,
  or final expressions fail closed during lowering.

## Validation

The two-tablet service integration executes the same COUNT/COUNT(column)/MIN/MAX query through a
remote mutual-TLS worker and through co-located workers, compares identical Native payloads, checks
event-time BETWEEN, LIMIT zero, and explicit source-row overload. Existing retry and cancellation
tests exercise the shared pre-finalization owner. The Linux three-daemon gate now issues aggregate
SQL through a nonleader before and after leader replacement in addition to its ordered row query.

The completed local verification on macOS was:

- normal query, cluster, and service suites: 404, 195, and 106 tests passed;
- query, cluster, and service allocation-failure suites: 53, 26, and 3 tests passed;
- focused ASan/UBSan coverage: 9 aggregate-lowering tests, 2 lowering allocation tests,
  3 aggregate-row finalizer tests, 1 finalizer allocation test, and the two-tablet service
  integration test passed with leak detection disabled;
- formatting, diff checking, changed-source LLVM 18 clang-tidy, and the installed public-target
  consumer passed; and
- the full LLVM 18 static-analysis target advanced through the changed query source and then stopped
  on the pre-existing unchecked-optional warning in `src/network/native_query_retry.cpp`.

The Linux-only three-daemon process test source is extended but cannot execute on this macOS host;
it remains a required Linux CI gate and is not claimed as a local pass. This change adds no new
concurrency primitive, so it does not add a distinct TSan target beyond the existing query-plane
concurrency coverage.

## Migration and rollback

The aggregate SQL product changes only an unused pre-alpha public construction shape by nesting its
explicit input row plan. No wire or durable bytes change. Rollback restores row-only service
dispatch and removes the aggregate branch; existing row SQL remains byte-compatible.

## References

- [Distributed aggregate SQL lowering](../learning/distributed-aggregate-sql-lowering.md)
- [Mutable-row aggregate finalization](../learning/mutable-row-aggregate-finalization.md)
- [Native protocol request lifecycle](../learning/native-protocol-request-lifecycle.md)
