# ADR 0521: Proof-Bound Local Mutable Pre-Group Execution

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB query execution and distributed protocol maintainers

## Context

ADR 0520 gave computed pre-group expressions an owned byte representation. Before changing the
mutable-fragment wire format, the in-memory binding and worker boundary must prove that a program's
declared source leaves and outputs are authoritative for one exact publication.

## Decision

- A mutable grouped fragment may own an optional pre-group program. Rows fragments reject it.
- Binding validates every input leaf's ordinal, type, and nullability against the exact current
  schema. Plan inputs and result-schema validation use ordered program result shapes, not raw source
  projection shapes.
- The grouped worker repeats source-shape validation against its local exact schema, includes the
  program in retained-configuration bounds, and materializes outputs with `ColumnOutputOperator`
  after event-time filtering and before grouped aggregation.
- Existing v1 encoding fails with `NOT_SUPPORTED` when a program is present. It must never omit an
  accepted in-memory program. A later versioned protocol decision will remove this deliberate stop.

## Consequences

Local exact-publication execution now proves and runs computed grouping keys and aggregate inputs
without borrowing coordinator AST state. Remote use remains unavailable and fail-closed.

## Validation plan

An exact mutable publication test binds an owned `UPPER` program, executes it, and observes the
transformed grouped key. It also proves transport rejection and stale source-shape rejection. Full
query, allocation-injection, sanitizer, format, and applicable static-analysis gates remain
required before the subsequent wire milestone.

## References

- [ADR 0520](0520-versioned-owned-pre-group-vector-program.md)
- [Distributed Vector Pre-Group Program v1](../formats/distributed-vector-pre-group-program-v1.md)
