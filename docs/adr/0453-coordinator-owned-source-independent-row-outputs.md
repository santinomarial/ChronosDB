# ADR 0453: Coordinator-owned source-independent row outputs

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB SQL, query, cluster, replicated-service, and daemon maintainers
- **Extends:** [ADR 0439](0439-schema-bound-distributed-row-sql-lowering.md),
  [ADR 0449](0449-hidden-distributed-row-order-outputs.md), and
  [ADR 0452](0452-replicated-native-global-aggregate-execution.md)

## Context

Distributed row SQL accepted only direct source outputs even when an output was independent of every
row, such as a literal or `upper('ok')`. The current schema-neutral worker plan cannot carry a
vector-expression program. Changing that plan merely to repeat one constant would add a wire
version and duplicate deterministic work on every tablet.

## Decision

Row lowering accepts a bound scalar output only when its complete expression tree contains no source
column, star, aggregate, or relational dependency. It evaluates that expression once through the
existing scalar oracle and stores its exact typed canonical bytes in an optional coordinator
projection. Direct outputs in the same query store checked worker-output indices in that projection.
Constant bytes and result names are independently bounded.

The wire plan still carries only real source outputs. If every visible output is constant, lowering
projects the table's event-time column as a row-count anchor. Direct ORDER BY source columns remain
worker outputs and global sort keys; ordering by a selected constant is redundant and is removed.
After all streams close, the existing finalizer sorts and limits real rows, validates every source
shape and constant payload, then injects the final source/constant output vector while Native bytes
are encoded. LIMIT zero still emits a schema-bearing zero-row result.

Constant evaluation errors are planning errors before distributed I/O. Row-dependent computed
outputs and computed order keys remain unsupported; admitting them requires a separately versioned
worker expression contract or a bounded coordinator row-expression stage.

No durable or network format, listener, authentication rule, snapshot proof, retry rule, or
acknowledged-write guarantee changes.

## Consequences

Queries such as `SELECT 7 AS marker, upper('ok') AS word FROM events LIMIT 1` now execute through
local, remote mutual-TLS, and packaged-daemon paths without transferring synthetic columns. The
anchor adds one bounded real column when no source output exists. Canonical constant storage is
owned by the request plan and borrowed only during synchronous finalization; Native encoding copies
the final payload before that plan is destroyed.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): constants and source mappings come from the same
  bound catalog snapshot as the worker plan.
- [Invariant 14](../architecture/invariants.md): Plan Intent 1.0/1.1 bytes remain unchanged and
  constants are validated in the existing Native scalar representation.
- [Invariant 15](../architecture/invariants.md): constant configuration, source rows, working state,
  response cells, and payload bytes remain explicitly bounded.
- [Invariant 18](../architecture/invariants.md): row-dependent expressions fail closed instead of
  being evaluated against partial or unauthoritative data.

## Validation

Focused tests cover mixed source/constant output, an all-constant event-time anchor, canonical fixed,
Boolean, text, and NULL encoding, direct hidden ordering, post-order/post-LIMIT injection, malformed
source mappings, constant-byte limits, and exhaustive lowering/finalizer allocation failure. The
two-tablet service test requires byte-identical results from a remote mutual-TLS worker and a
co-located worker. The Linux three-daemon source gate issues the query before and after leader
replacement.

The complete normal query, cluster, and service suites pass with 406, 196, and 106 tests; their
allocation-failure suites pass with 54, 27, and 3 tests. Focused ASan/UBSan runs pass 3 lowering/
canonical-value tests, 1 lowering allocation test, 1 projection-finalizer test, 1 finalizer
allocation test, and the two-tablet service integration with leak detection disabled. Formatting,
diff checking, changed-source LLVM 18 clang-tidy, and the installed public-target consumer pass. The
full static-analysis target advances through both changed query sources and then stops on the
pre-existing unchecked-optional warning in `src/network/native_query_retry.cpp`.

The Linux-only three-daemon process gate compiles into the Linux target but cannot execute on this
macOS host; it remains a required CI gate and is not claimed as a local pass. This change adds no
concurrency primitive, so it adds no distinct TSan target beyond the existing query-plane ownership
coverage.

## Migration and rollback

The pre-alpha row SQL product gains an optional coordinator-only field. Existing direct-row plans
leave it empty and retain identical wire bytes and finalization behavior. Rollback removes the new
lowering branch and projected finalizer call; previously accepted queries are unaffected.

## References

- [Distributed row SQL lowering](../learning/distributed-row-sql-lowering.md)
- [Distributed Vector Plan Intent v1](../formats/distributed-vector-plan-intent-v1.md)
- [Bounded global vector row finalization v2](0379-bounded-global-vector-row-finalization-v2.md)
