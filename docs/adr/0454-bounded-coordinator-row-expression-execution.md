# ADR 0454: Bounded coordinator row-expression execution

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB SQL, query, cluster, replicated-service, and daemon maintainers
- **Extends:** [ADR 0035](0035-bounded-checked-vector-expression-programs.md),
  [ADR 0038](0038-borrowed-variable-width-vector-materialization.md),
  [ADR 0379](0379-bounded-global-vector-row-finalization-v2.md), and
  [ADR 0453](0453-coordinator-owned-source-independent-row-outputs.md)

## Context

Distributed Native row queries could return direct columns and source-independent expressions, but
still rejected ordinary row-dependent outputs such as `value + 2`, `value > 4`, and
`lower(label)`. The checked vector-expression engine already owns those scalar semantics.
Serializing its in-memory program in Plan Intent would prematurely create a network bytecode and
require every worker version to execute it. Evaluating an AST per row at the coordinator would
instead duplicate the physical engine and lose its allocation and short-circuit contracts.

## Decision

Distributed row lowering may retain an existing immutable `VectorExpression` in the coordinator
projection. If any SELECT output is row-dependent, workers carry every source column exactly once
in schema-ordinal order. Expression input ordinals therefore remain the bound schema ordinals; the
distributed wire plan still contains only ordinary source projection and result-schema fields.
Direct and source-independent outputs continue to share the same coordinator projection.

The coordinator first validates each program's result shape and every source leaf against the
worker schema. It closes all tablet streams, performs global direct-column ORDER BY, and applies
LIMIT before evaluating visible expression outputs. Computed ORDER BY keys remain unsupported
because ordering must precede this stage.

Final materialization uses two passes over selected rows. The first evaluates programs to determine
NULL state and exact payload sizes. The finalizer then admits a bounded size vector and one
contiguous canonical-byte arena, reevaluates each program, and writes fixed-width values or borrowed
ASCII text transforms directly into that arena. Native encoding copies from the stable arena. No
successful expression cell allocates independently, and no output batch is published before all
sizing and resource checks succeed.

Expression instruction/configuration bytes, size entries, canonical row views, transformed payload
bytes, Native response bytes, rows, batches, and input messages remain under independent finite
limits. Runtime arithmetic, cast, or shape errors fail the complete query. The synchronous
finalizer has one caller thread and publishes only an owned terminal value, so this decision adds no
memory-ordering argument or acknowledged-write guarantee.

This decision adds no durable or network format, worker bytecode, optimizer rule, computed
predicate, computed aggregate input, computed ORDER BY key, listener, authentication rule, retry
rule, or storage visibility rule.

## Consequences

The replicated Native path can execute the existing fixed-width and borrowed STRING/SYMBOL vector
subset for row-dependent SELECT outputs across local and remote workers. The transitional cost is
that one computed output causes the complete source row to cross the worker boundary even when its
program reads only a few columns. A future versioned worker-expression contract may prune and
execute these programs closer to storage, but must remain differential with this coordinator
baseline.

Expression runtime errors occur after the distributed read has completed but before a result is
published. Re-evaluation in the materialization pass is deterministic because programs and decoded
input bytes are immutable and the expression engine has no external state.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): bound schema ordinals and exact physical shapes are
  preserved through the full-source worker projection.
- [Invariant 11](../architecture/invariants.md): Native output owns copied canonical bytes; row views
  and program constants are borrowed only during synchronous finalization.
- [Invariant 14](../architecture/invariants.md): Plan Intent 1.0/1.1 and Native scalar bytes are
  unchanged.
- [Invariant 15](../architecture/invariants.md): configuration, size state, canonical payloads, and
  response batches are explicitly bounded before publication.
- [Invariant 18](../architecture/invariants.md): the distributed executor reuses the checked vector
  oracle and fails closed for computed ordering or unsupported relational semantics.

## Validation

Unit coverage includes fixed arithmetic, Boolean and variable text lowering, full schema-ordinal
projection, post-sort/post-LIMIT evaluation across split result batches, NULL text, stale source
shapes, checked runtime failure, row-count and payload-driven batch boundaries, configuration and
working-memory bounds, exact canonical decode/write helpers, and exhaustive lowering/finalizer
allocation failure. The two-tablet service
integration requires byte-identical computed results from remote mutual-TLS and co-located workers.
Normal, sanitizer, formatting, static-analysis, installed-consumer, and final-diff checks are
required before this milestone is released; only commands actually run may be reported.

The complete normal query, cluster, and service suites pass with 407, 199, and 106 tests; their
allocation-failure suites pass with 55, 27, and 3 tests. Focused ASan/UBSan runs pass 3 query tests,
1 query allocation test, 2 finalizer tests, 1 finalizer allocation test, and the two-tablet service
integration with leak detection disabled. The five changed production sources pass LLVM 18
clang-tidy, and formatting plus the installed public-target consumer pass. The Linux-only
three-daemon process source includes the expression query but cannot execute on this macOS host; it
remains a CI gate and is not claimed as a local pass.

## Migration and rollback

The new expression variant is coordinator-owned and in-memory only. Existing direct and constant
plans retain their previous projection and wire bytes. Rollback removes expression admission and
the finalizer branch without changing durable state or protocol compatibility.

## Subsequent decision

[ADR 0455](0455-bounded-coordinator-row-predicate-execution.md) uses the same canonical-row adapter
to execute general checked Boolean WHERE programs before global ordering and LIMIT.

## References

- [Distributed row SQL lowering](../learning/distributed-row-sql-lowering.md)
- [Checked vector expression programs](../learning/vector-expression-programs.md)
- [Distributed Vector Plan Intent v1](../formats/distributed-vector-plan-intent-v1.md)
