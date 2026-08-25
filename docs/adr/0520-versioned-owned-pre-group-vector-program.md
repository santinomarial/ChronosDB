# ADR 0520: Versioned Owned Pre-Group Vector Program

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB query execution and distributed protocol maintainers

## Context

Grouped distributed SQL can currently send only direct source-column ordinals to a mutable worker.
Computed grouping keys and aggregate inputs need the same checked `VectorExpression` semantics as
local execution, but passing an in-memory expression object would make remote execution dependent
on process ABI and borrowed coordinator state.

## Decision

- Define Distributed Vector Pre-Group Program v1 as an owned, checksummed byte format containing an
  ordered list of immutable `VectorExpression` outputs.
- Expression inputs name exact source-schema ordinals. Later fragment integration will validate
  those declared input shapes against each selected immutable publication before scanning.
- Freeze explicit instruction and operation codes rather than serializing C++ enum or variant
  representations. Logical types and constant values use existing validated schema and canonical
  scalar encodings.
- Enforce hard frame, expression, instruction, and constant bounds before allocating decoded
  values, plus caller-supplied limits no broader than the hard limits.
- Keep this format standalone in this milestone. Embedding it changes mutable-fragment semantics
  and therefore requires its own versioned protocol decision and worker proof.

## Consequences

Coordinator-owned computed expressions can now be represented independently of syntax trees,
addresses, and ABI. Corruption is detected at header, payload, expression, constant, and complete
frame boundaries. The codec alone does not claim computed distributed SQL support.

The fixed-width instruction representation is larger than a compact bytecode. Its bounded size and
simple validation are preferred until workload evidence justifies compression.

## Affected invariants

This supports invariants 5, 6, 10, and 18: the format is explicit and checksummed, decoded programs
retain exact types, corrupt bytes fail closed, and expression construction revalidates the complete
typed DAG.

## Validation plan

- Round-trip every instruction variant, nullable inputs, variable-width constants, and typed NULL.
- Reject damage, unknown versions, invalid programs, and caller-limit exhaustion.
- Inject allocation failure across every owned encode and decode allocation.
- Compile the public header alone and run warning-as-error, sanitizer, format, and static checks.

## Migration or rollback considerations

No existing fragment or durable format changes. Removing this codec removes only unused v1 program
bytes. A future fragment version must nest the exact frame and explicitly define old-version
compatibility.

## References

- [ADR 0035](0035-bounded-checked-vector-expression-programs.md)
- [Distributed Vector Pre-Group Program v1](../formats/distributed-vector-pre-group-program-v1.md)
- [Architecture invariants](../architecture/invariants.md)
