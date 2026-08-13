# ADR 0323: Bounded grouped-terminal partial I/O

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and networking maintainers
- **Extends:** [ADR 0321](0321-bounded-grouped-exchange-partial-io.md),
  [ADR 0322](0322-distinct-empty-grouped-stream-terminal.md)

## Context

Grouped Exchange Terminal v1 gives an empty tablet an unambiguous completion frame, but an
embedding still needed to own fragmented reads and short-write progress. Reusing the 136-byte
grouped-partial reader would either require an implicit frame discriminator or risk retaining the
wrong bound. Neither contract belongs in the fixed terminal codec.

## Decision

`GroupedExchangeTerminalFrameReader` owns exactly one 64-byte array and fill count. Each `consume`
copies only the prefix belonging to that frame, reports the exact consumed byte count, and leaves a
coalesced successor with the caller. Completion delegates to exact terminal decoding. Decode
failure is sticky; the connection-owned reader is neither copyable nor movable.

`GroupedExchangeTerminalFrameWriteCursor` owns one validated canonical terminal frame and exposes
only its unwritten suffix. Checked advancement rejects an overrun without changing progress. The
cursor is move-only, and a move forces the source complete so only the destination can continue the
write obligation. Zero-byte progress is valid.

These classes do not define a unified grouped-stream discriminator, socket transport, retry policy,
or grouped coordinator. They do not change the 64-byte terminal format.

## Consequences and validation

Read retention is constant and successful construction/advancement does not allocate payload
storage. A carrier can consume exactly one terminal from an arbitrarily fragmented or coalesced
byte stream without losing successor bytes.

Focused tests enumerate all 65 two-part split positions, process a coalesced successor through the
reported consumption, prove corruption remains sticky, verify multiple short-write advances and
overrun rollback, and prove moved-from cursors are inert. The installed-consumer gate covers both
public carrier interfaces.

Grouped stream multiplexing, fragment planning, ordering/top-N/LIMIT, and broader socket/fault
evidence remain incomplete. Bounded single-key sequencing and merge state are the accepted
follow-up in [ADR 0324](0324-bounded-grouped-float64-coordinator.md). No Phase 16 exit gate is
claimed.

Invariants 6, 10, 11, 14, 15, and 18 apply.

## References

- [Distributed Grouped FLOAT64 Aggregate Exchange
  v1](../formats/distributed-grouped-float64-exchange-v1.md)
- [Bounded grouped-exchange partial I/O](0321-bounded-grouped-exchange-partial-io.md)
- [Distinct empty grouped-stream terminal](0322-distinct-empty-grouped-stream-terminal.md)
