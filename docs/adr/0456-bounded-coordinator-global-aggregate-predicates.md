# ADR 0456: Bounded coordinator global-aggregate predicates

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB SQL, query, cluster, replicated-service, and daemon maintainers
- **Extends:** [ADR 0450](0450-schema-bound-distributed-global-aggregate-sql-lowering.md),
  [ADR 0451](0451-bounded-mutable-row-global-aggregate-finalization.md),
  [ADR 0452](0452-replicated-native-global-aggregate-execution.md), and
  [ADR 0455](0455-bounded-coordinator-row-predicate-execution.md)

## Context

Replicated global aggregates accepted exact event-time ranges but rejected the rest of the checked
SQL v1 Boolean expression surface. Direct row queries already retain and evaluate that surface over
complete authenticated Native rows. Aggregate queries use the same mutable-row carrier and shared
canonical aggregate kernel, so a second predicate engine or protocol is unnecessary.

## Decision

`DistributedVectorAggregateSqlPlan` may own one checked Boolean `VectorExpression` coordinator
predicate. Exact event-time comparison/positive-BETWEEN conjunctions retain worker execution.
Every other admitted WHERE expression is lowered as one coordinator program. Mixed event-time and
general predicates remain wholly coordinator-side rather than being partially rewritten.

A source-dependent program causes the unlimited identity input plan to project the complete source
schema in ordinal order. Aggregate input indices then name those worker output positions. A
source-independent predicate keeps the compact unique aggregate projection; COUNT(*) still carries
its real event-time row-count anchor when otherwise necessary. Expression instruction and retained
configuration bounds are explicit lowering inputs.

The mutable-row aggregate finalizer validates the predicate result and every source leaf against
the exact input result schema. It reuses one canonical-cell view vector and evaluates the predicate
once per decoded row after every tablet stream has been validated as complete. SQL TRUE admits the
row; FALSE and NULL skip it before COUNT(*) or any other aggregate state advances. Runtime failure
destroys all partial states and publishes no Native result.

Predicate configuration and canonical row-view capacity are conservatively charged to the existing
working-memory limit. Evaluation succeeds without per-row heap allocation. The service selects the
predicate-aware finalizer only when lowering produced the optional program. This decision changes
no durable bytes, wire bytes, authority, retry, cancellation, or acknowledged-write guarantee.

[ADR 0458](0458-bounded-coordinator-global-aggregate-output-expressions.md) subsequently composes
this pre-aggregate predicate with checked post-aggregate visible expressions.

## Consequences

Replicated Native global aggregates now accept the same checked WHERE expression intersection as
distributed rows. General predicates may transfer a complete source row for every worker match;
this is a correctness-first transitional cost until a separately versioned worker expression
protocol is justified. Exact event-time-only queries retain their sparse worker projection and
pushdown path.

Time is `O(rows * (predicate instructions + aggregate outputs))`. Extra retained state is bounded
by the predicate program, one canonical input-row view, aggregate states, one decoded batch, and
retained extrema. Execution is synchronous and thread-affine, so no new memory-ordering argument
applies.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): predicate leaves, aggregate inputs, and Native row
  descriptors share one exact bound schema identity.
- [Invariant 10](../architecture/invariants.md): all tablet streams are validated terminal before
  any aggregate result can be finalized or published.
- [Invariant 11](../architecture/invariants.md): predicate cell views borrow only the currently
  owned decoded batch during synchronous evaluation.
- [Invariant 14](../architecture/invariants.md): existing versioned fragment and exchange formats
  are unchanged.
- [Invariant 15](../architecture/invariants.md): expression configuration, source width, canonical
  row state, batches, rows, messages, aggregate state, and output remain finite.
- [Invariant 18](../architecture/invariants.md): worker specialization and coordinator evaluation
  preserve identical SQL WHERE truth rather than weakening it for performance.

## Validation

Focused lowering tests cover source-dependent full-schema mapping, compact constant predicates,
NOT BETWEEN, aggregate input remapping, expression bounds, and allocation failure. Finalizer tests
cover TRUE/FALSE/NULL filtering before every aggregate, stale and non-Boolean programs, working
memory, and allocation failure. The two-tablet service integration executes a computed aggregate
predicate through both a remote mutual-TLS worker and co-located workers and requires identical
Native payloads.

The normal query, cluster, and service suites pass with 409, 201, and 106 tests; their allocation-
failure suites pass with 56, 27, and 3 tests. Focused ASan/UBSan runs pass aggregate predicate
lowering, lowering allocation failure, predicate-aware finalization including hostile runtime
errors, finalizer allocation failure, and the two-tablet service integration with leak detection
disabled. All three changed production sources pass LLVM 18 clang-tidy without user-code
diagnostics. Formatting, diff checks, and the installed public-target consumer pass. The Linux
daemon process gate remains Linux-only and is not claimed on macOS.

## Migration and rollback

The added plan member and finalizer overload are pre-alpha in-memory APIs. Rollback rejects general
aggregate predicates and removes the overload without changing durable or protocol compatibility.

## References

- [Distributed aggregate SQL lowering](../learning/distributed-aggregate-sql-lowering.md)
- [Mutable-row aggregate finalization](../learning/mutable-row-aggregate-finalization.md)
- [Checked vector expression programs](../learning/vector-expression-programs.md)
