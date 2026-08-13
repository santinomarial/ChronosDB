# ADR 0350: Canonical distributed vector-batch exchange

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, columnar, and networking maintainers
- **Extends:** [ADR 0070](0070-feature-pass-logical-boundaries.md)

## Context

Distributed aggregate and grouped frames cannot carry general projected rows without changing their
frozen bytes. ChronosDB already has a versioned, checksummed, hostile-input-safe Columnar Batch v1
codec for every current logical type. Defining a second column layout for distributed results would
duplicate type, null, variable-width, decimal, UTF-8, padding, and integrity rules.

## Decision

Distributed Vector Exchange v1 is a distinct checksummed envelope containing query/tablet identity,
positive stream sequence, terminal state, and zero or one exact Columnar Batch v1. The nested bytes
remain independently checksummed and versioned. Empty payload is permitted only for a terminal-only
empty stream; a data batch may itself be terminal.

The fixed 80-byte header has an early CRC before any encoded length drives slicing. The complete CRC
covers header, stored header CRC, reserved bytes, and nested batch. Decode limits bound the complete
frame plus nested batch bytes, rows, and columns before descriptor allocation. Decoding returns
owning nested bytes; callers may exact-decode borrowed column views while that owner remains alive.

This decision defines result bytes only. It does not define physical-plan request bytes, stream
coordination, network carrier ownership, schema authorization, or arbitrary-expression execution.
Existing aggregate/grouped and Columnar Batch v1 bytes remain unchanged.

## Consequences and validation

Encoding and decoding are linear in frame size and retain one bounded owned byte vector. Two focused
tests round-trip a canonical mixed-type batch and terminal-only frame, then reject truncation,
checksum-valid nested corruption, frame-limit excess, and an empty nonterminal frame. Public header
self-containment and installed consumption cover the API.

Vector fragments, exact node-routed request/response bytes, partial-I/O owners, and bounded
exact-retry coordination are implemented separately. Packaged execution, multi-process integration,
and broad fault/measurement evidence remain incomplete. No Phase 16 exit gate is claimed.

Invariants 5, 6, 10, 14, 15, and 18 apply.

## References

- [Distributed Vector Exchange v1](../formats/distributed-vector-exchange-v1.md)
- [Columnar Batch v1](../formats/columnar-batch-v1.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
