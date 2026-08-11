# ADR 0162: Bounded distributed exchange partial I/O

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB query and networking maintainers
- **Extends:** [ADR 0161](0161-canonical-distributed-aggregate-exchange-frame.md)

## Context

The canonical aggregate frame assigns exact bytes, but nonblocking stream reads and writes rarely
transfer all 128 bytes at once. A caller must not reinterpret a prefix, lose a coalesced successor,
duplicate bytes after a short write, or grow peer-controlled buffering without a bound.

## Decision

`ExchangeFrameReader` owns exactly one 128-byte array and a fill count. `consume` copies at most the
remaining bytes for one frame and reports the exact consumed prefix, leaving a coalesced suffix in
caller ownership for the next call. It emits nothing until the array is complete and exact decoding
succeeds. Any decode failure is sticky for that reader; subsequent input returns the same failure
without modifying stream state. Readers are neither copyable nor movable, preventing partial state
from being duplicated or silently transferred between connection owners.

`ExchangeFrameWriteCursor` owns one canonical encoded frame and exposes only its unwritten suffix.
`consume_written` rejects an over-advance before changing the cursor. The cursor is move-only; a
moved-from cursor is forced complete so it cannot retransmit the frame accidentally. A zero-byte
advance is harmless, matching a nonblocking loop that made no progress.

These are portable byte-ownership primitives, not socket owners. They do not add queues, blocking,
retry, authentication, connection deadlines, or sequence policy.

## Consequences and validation

Reader and writer retained memory is constant and allocation-free after construction. Each byte is
copied or exposed once, so stream processing is linear in bytes. Backpressure remains the owning
connection's responsibility; one reader or cursor cannot retain an unbounded peer-controlled
suffix.

Tests enumerate all 129 two-part split positions, process two coalesced frames using reported
consumption, verify sticky corruption, validate exact short-write suffixes and over-advance
rollback, and prove moved-from write cursors are inert. Focused ASan/UBSan and the installed public
consumer cover the new interfaces. Real socket integration and fault matrices remain follow-up
work.

Invariants 6, 10, 11, 14, 15, and 18 apply.

## Migration and rollback

The carrier does not change Exchange v1 bytes. A rollback can use exact codec calls with an
equivalent external buffering owner, but must preserve the same boundedness, exact consumption, and
short-write ownership rules.

## References

- [Distributed Aggregate Exchange v1](../formats/distributed-aggregate-exchange-v1.md)
- [Canonical distributed aggregate exchange frame](0161-canonical-distributed-aggregate-exchange-frame.md)
