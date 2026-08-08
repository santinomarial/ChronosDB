# ADR 0065: Self-describing Protocol v1 query result batches

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** networking and query subsystems
- **Supersedes:** none

## Context

`QUERY_RESULT` was assigned but its payload was not. Reusing Columnar Batch v1 would require a
nonzero table/schema identity and complete table-schema shape, neither of which exists for computed,
aggregate, joined, or aliased SQL outputs. Inventing identities would weaken both format contracts.

## Decision

Protocol v1 uses a self-describing row-batch payload defined in the native protocol specification.
Every batch repeats ordered output descriptors (name, frozen logical type parameters, nullability)
and then row-major canonical cells. Repetition allows a client to validate any frame independently
and lets a zero-row batch communicate result schema. Descriptor and cell lengths are checked before
access or allocation. Text names/values are valid Unicode-scalar UTF-8; fixed cells use the same
canonical scalar bytes as Columnar Batch v1. A reserved `0xffffffff` length denotes NULL and is
legal only for nullable columns with no bytes.

`QUERY_RESULT.END_STREAM` means no later result batch, but successful request completion remains an
empty `QUERY_END`. This keeps stream termination distinct from execution success and permits the
server to report a late execution error after earlier batches. Reactors accept a result only for an
active query and only after full payload validation.

## Alternatives considered

- **Embed Columnar Batch v1:** rejected because synthetic table/schema identities would be false.
- **Opaque implementation-defined bytes:** rejected because clients could not interoperate.
- **Serialize query C++ value variants:** rejected because native object layouts are not wire
  formats.
- **Column-major result vectors:** deferred until a result-specific vector format has measured need;
  it would require more offset metadata and does not change framing or lifecycle.

## Consequences

The baseline has row-major traversal and repeats descriptors, favoring a small exact interoperable
contract over maximum throughput. Phase 12 may introduce a negotiated compatible encoding, but
Protocol 1.0 cannot silently reinterpret this payload.

## Validation

Tests cover mixed fixed/text/NULL cells, zero rows, hostile shapes, noncanonical Boolean, truncation,
trailing bytes, allocation failure, reactor response validation, installed use, and fuzz input.

## References

- [Native Protocol v1](../protocol/native-v1.md)
- [Columnar Batch v1](../formats/columnar-batch-v1.md)
- [ADR 0060](0060-native-protocol-v1-framing.md)

