# ADR 0457: Bounded coordinator computed row ordering

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB SQL, query, cluster, replicated-service, and daemon maintainers
- **Extends:** [ADR 0035](0035-bounded-checked-vector-expression-programs.md),
  [ADR 0379](0379-bounded-global-vector-row-finalization-v2.md),
  [ADR 0449](0449-hidden-distributed-row-order-outputs.md), and
  [ADR 0454](0454-bounded-coordinator-row-expression-execution.md)

## Context

Distributed row SQL could globally order direct source values, including hidden helper outputs, but
rejected computed keys already executable by the checked vector engine. Evaluating visible outputs
after LIMIT cannot supply ordering because discarded rows may sort before retained rows. Adding
worker bytecode would create a new versioned protocol before a measured need.

## Decision

The coordinator projection may own an ordered vector of checked expression keys, each with exact
direction and NULL placement. If any ORDER BY key is computed, lowering places every nonconstant key
in that vector in SQL precedence order and leaves Plan Intent's direct order vector empty. Direct
aliases and source references become one-instruction input programs; source-independent keys are
removed because they cannot distinguish rows. Workers carry the complete source schema so program
ordinals remain the bound schema ordinals.

After all tablet streams close and the optional WHERE predicate filters rows, the finalizer performs
the same stable merge sort used for direct keys. Its comparator loads two reusable canonical source-
row views. Fixed-width programs return owned scalar values and use the shared scalar comparator.
Variable-width programs return borrowed bytes plus an optional ASCII case transform; a nonallocating
lexicographic comparator applies transforms byte by byte. NULL placement is independent of direction,
and descending reverses only non-NULL comparisons. LIMIT and visible output materialization follow.

Every program and source leaf is validated against the worker schema. Key count, expression
configuration, two canonical row views, row references, merge scratch, input batches, and output
remain under existing finite limits. Runtime expression failure aborts before any Native batch is
published. No durable, wire, authority, retry, cancellation, or acknowledged-write contract changes.

## Consequences

Distributed Native rows now support checked fixed-width and borrowed STRING/SYMBOL computed ORDER BY
expressions, selected aliases, mixed keys, explicit/default NULL placement, and global LIMIT. The
correctness-first comparator may reevaluate programs `O(rows log rows)` times. Materializing all key
values could reduce CPU at the cost of an additional bounded arena and needs benchmark evidence
before adoption.

The finalizer is synchronous and thread-affine. Programs and decoded input remain immutable during
sorting, so reevaluation is deterministic and no memory-ordering argument applies.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): full source ordinals and shapes remain bound to one
  exact distributed snapshot.
- [Invariant 11](../architecture/invariants.md): variable key bytes borrow only immutable decoded
  batches or program constants during synchronous comparison.
- [Invariant 14](../architecture/invariants.md): Plan Intent and result-exchange bytes are unchanged.
- [Invariant 15](../architecture/invariants.md): key count, configuration, row views, stable-sort
  state, and results remain explicitly finite.
- [Invariant 18](../architecture/invariants.md): computed ordering uses the shared checked expression
  and scalar ordering semantics without tablet-local LIMIT or weakened NULL behavior.

## Validation

Lowering coverage requires selected aliases, nonselected mixed fixed/text keys, constants, full-
schema mapping, NULL policy, bounds, and allocation injection. Finalizer coverage requires stable
computed ordering before LIMIT and output expressions, ASCII transforms, ties, NULLs, stale shapes,
runtime arithmetic failure, direct/coordinator conflict rejection, working memory, and allocation
injection. The replicated two-tablet integration requires identical computed-order payloads through
remote mutual TLS and co-located workers. Full normal/allocation suites, focused ASan/UBSan, LLVM 18
analysis, formatting, and the installed consumer are required before acceptance.

The normal query, cluster, and service suites pass with 409, 202, and 106 tests; their allocation-
failure suites pass with 56, 27, and 3 tests. Focused ASan/UBSan runs pass lowering and allocation
injection, computed fixed/text ordering and hostile finalization, finalizer allocation injection,
and the two-tablet remote/local service integration with leak detection disabled. Both changed
production sources pass LLVM 18 clang-tidy without user-code diagnostics. Formatting and diff
checks and the installed public-target consumer pass.

## Migration and rollback

The new coordinator order vector is a pre-alpha in-memory field. Existing direct-only plans retain
their current Plan Intent bytes. Rollback removes computed-order admission without changing stored
or network compatibility.

## References

- [Distributed row SQL lowering](../learning/distributed-row-sql-lowering.md)
- [Checked vector expression programs](../learning/vector-expression-programs.md)
- [Distributed Vector Plan Intent v1](../formats/distributed-vector-plan-intent-v1.md)
