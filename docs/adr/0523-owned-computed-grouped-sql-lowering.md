# ADR 0523: Owned computed grouped SQL lowering

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB SQL, query execution, cluster, and replicated-service maintainers

## Context

ADRs 0520 through 0522 established a versioned pre-group vector program, proof-bound local
execution, and backward-compatible authenticated transport. The grouped sufficient-state SQL
lowerer still rejected computed keys and aggregate inputs, so Native SQL selected the row-backed
oracle even though workers could execute the required program.

## Decision

- The grouped sufficient-state lowerer emits one owned pre-group output for every group key and
  non-star aggregate input whenever any such expression is computed. Direct-only plans retain their
  prior projection and omit the program.
- Program leaves keep full source-schema ordinals. The destination projection records the unique
  source dependencies used for authority and bounds, while grouped plan inputs name ordered program
  outputs. The lowerer validates result shapes against those outputs and proves the complete program
  is encodable within the accepted v1 program format before returning it.
- Repeated group expressions use the binder's structural equivalence rule: bound columns compare by
  source and column identity and all other nodes compare their complete typed syntax recursively.
  Final expressions bind an equivalent whole group subtree to the finalized group column, so a
  computed key is never evaluated a second time over its own result.
- Replicated preparation derives grouped key and aggregate authority from program output shapes,
  transfers the program into every exact-publication fragment, and relies on the existing source
  leaf proof at fragment binding.
- The logical query identity includes the complete optional program. Mixed tablet programs and
  fresh-authority retries that change computation fail closed.

## Consequences

Computed grouping keys and computed aggregate inputs now use the scalable grouped sufficient-state
path through local or authenticated remote Native execution. Non-event-time predicates, hidden
`ORDER BY` expressions, and expressions outside the checked vector vocabulary still select the
row-backed correctness path or fail according to the existing boundary. No durable format changes.
Fragments without programs retain Mutable Fragment v1 bytes; computed fragments use the accepted
v2 nesting from ADR 0522.

## Validation plan

- Unit-test computed keys, computed aggregate inputs, computed final expressions over a grouped
  subtree, direct-plan compatibility, resource limits, and allocation failure.
- Prove replicated preparation owns the exact program in every tablet fragment.
- Exercise the existing two-tablet mutual-TLS Native query with a computed nullable string key and
  compare exact expected values and local execution bytes.
- Reject mixed pre-group programs in the complete logical identity used by fresh-authority retry.
- Run full query, cluster, service, allocation-failure, sanitizer, formatting, and applicable static
  analysis gates.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): workers execute only committed applied publications
  admitted by the exact leader proof.
- [Invariant 6](../architecture/invariants.md): source leaves, program outputs, grouped authority,
  final projection, and every tablet fragment belong to one coherent query snapshot.
- [Invariant 14](../architecture/invariants.md): computed programs cross the network only through the
  accepted versioned and checksummed nested formats.
- [Invariant 15](../architecture/invariants.md): expression, program, fragment, and response memory
  influence remains explicitly bounded.
- [Invariant 18](../architecture/invariants.md): selecting sufficient-state execution preserves the
  checked row-backed SQL semantics.

## References

- [ADR 0520](0520-versioned-owned-pre-group-vector-program.md)
- [ADR 0521](0521-proof-bound-local-mutable-pre-group-execution.md)
- [ADR 0522](0522-backward-compatible-mutable-pre-group-fragments.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
