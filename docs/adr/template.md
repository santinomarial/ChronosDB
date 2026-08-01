# ADR NNNN: Title

- **Status:** proposed
- **Date:** YYYY-MM-DD
- **Owners:** names or roles accountable for the decision

## Context

Describe the problem, constraints, current state, forces, and why a durable decision is needed now. Distinguish measured facts from assumptions and future projections.

## Decision

State the chosen behavior and boundaries precisely enough to guide implementation and review. Include compatibility, failure, ownership, and concurrency semantics where relevant.

## Detailed rationale

Explain why this choice best fits the engineering priorities and current roadmap phase. Cite real specifications, measurements, prototypes, or tests when they exist; do not fabricate evidence.

## Alternatives considered

For each credible alternative, explain its advantages, disadvantages, and the concrete reason it was not selected. Include “defer the decision” when relevant.

## Consequences

List positive and negative consequences, operational effects, new constraints, follow-up work, and risks. State which future choices this makes easier or harder.

## Affected invariants

Link each applicable invariant in [`docs/architecture/invariants.md`](../architecture/invariants.md) by number and explain how the decision preserves, strengthens, or changes its validation obligations. If none apply, justify that conclusion.

## Validation plan

Define tests, fault injection, simulations, benchmarks, profiles, review evidence, and exit criteria. Name the configurations and success/failure signals; compilation alone is not validation.

## Migration or rollback considerations

Describe adoption order, on-disk/on-wire compatibility, mixed-version behavior, data conversion, rollback boundary, and recovery if rollout fails. If there is no deployed state, explain how the design preserves future migration options.

## Unresolved questions

List decisions intentionally left open, their owner, and when they must be resolved. Do not hide unresolved correctness questions in implementation TODOs.

## References

Link only to existing relevant specifications, ADRs, measurements, prototypes, issues, or external authoritative material that reviewers can verify.
