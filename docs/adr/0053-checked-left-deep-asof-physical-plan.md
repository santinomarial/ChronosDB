# ADR 0053: Checked left-deep ASOF physical plan

- **Status:** Accepted
- **Date:** 2026-08-07

## Context

The bounded ASOF operator accepts two already prepared physical inputs. SQL v1 permits a chain of
ASOF joins whose left expressions may refer to any prior source, so a reusable physical plan must
represent the exact preparation and shape handoff around every binary operator. The unary pipeline
cannot encode source count, sibling ownership, or binary shape compatibility.

## Decision

`PhysicalAsofPlan` is the checked binary-plan boundary. It owns one left and right unary
preparation pipeline per left-deep join, the exact `VectorAsofJoinDefinition` and limits for that
join, and one final unary pipeline. Creation requires at least one join and validates:

- each preparation output against the corresponding ASOF input shape;
- every later left-preparation input against the preceding join output;
- the final-pipeline input against the last join output;
- each complete ASOF definition through the operator's shared output-shape planner; and
- a finite join count and conservative retained-configuration limit.

Instantiation consumes exactly `join_count + 1` non-null sources in SQL source order. It builds
each checked preparation and join from left to right, then attaches the final pipeline. A failure
destroys every not-yet-consumed sibling and all already-created wrappers. Runtime failures retain
the ASOF operator's query-wide cancellation and credit-release behavior.

The plan remains immutable and reusable. Instantiation copies owned operator configuration, so
allocation failure is reported as `RESOURCE_EXHAUSTED`; source ownership is never returned after
the call begins.

## Consequences

SQL lowering gains a shape-safe target without weakening the unary plan contract. Match-presence
and row-version columns can survive between joins and be removed only by the final pipeline. This
increment does not choose join order: SQL v1 ASOF dependency order is semantic, and optimizer
selection remains separate.

## Alternatives rejected

- Extending the unary stage variant with a second raw operator would make a plan depend on runtime
  ownership and would not describe source preparation.
- Trusting the lowering implementation's ordinals would move shape errors to execution.
- Reusing one preparation pipeline for both sides would incorrectly require identical schemas and
  expression environments.
