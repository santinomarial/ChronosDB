# ADR 0356: Bounded distributed vector fragment partial I/O

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and distributed-systems maintainers
- **Extends:** [ADR 0353](0353-group-scoped-distributed-vector-fragment.md)

## Context

The variable-length vector dispatch codec previously required a carrier to assemble an exact frame.
That left fragmentation, coalesced successor ownership, allocation from declared lengths, and
short-write continuation outside the public request contract.

## Decision

`DistributedVectorFragmentReader` is a noncopyable, nonmovable, connection-owned state machine. It
retains the fixed 232-byte header first, verifies its CRC, validates canonical allocation-driving
fields and hard/caller limits, and only then allocates the exact frame. One `consume` call advances
at most one frame and reports the exact caller prefix consumed. A coalesced successor remains with
the caller. A retained-input protocol failure is sticky; an invalid limit configuration consumes
nothing and remains correctable by constructing a valid reader.

`DistributedVectorFragmentWriteCursor` owns the canonical encoded frame. It exposes only the
unwritten suffix, rejects over-acknowledgement without advancing, and transfers sole continuation
ownership on move. The moved-from cursor is complete. These types do not own descriptors, TLS,
deadlines, retries, or query authority.

The exact decoder also accepts an explicit outer frame limit. Physical-format overflow or damage is
corruption; a well-formed frame above a lower valid caller limit is resource exhaustion.

## Consequences and validation

The focused test enumerates every split of one variable frame, preserves a coalesced successor,
checks sticky header corruption and lower caller limits, and exercises short writes, rollback on
over-acknowledgement, and move transfer. Header self-containment and the installed external consumer
cover the public types.

Metadata-backed vector batch construction, worker execution, result coordination, authenticated
transport, and process integration remain incomplete. No Phase 16 exit gate is claimed.

Invariants 4, 5, 6, 11, 14, and 18 apply.

## References

- [Group-scoped distributed vector fragment](0353-group-scoped-distributed-vector-fragment.md)
- [Distributed Vector Fragment Dispatch v1](../formats/distributed-vector-fragment-dispatch-v1.md)
