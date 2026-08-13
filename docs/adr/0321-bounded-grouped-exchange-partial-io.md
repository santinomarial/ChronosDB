# ADR 0321: Bounded grouped-exchange partial I/O

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and networking maintainers
- **Extends:** [ADR 0162](0162-bounded-distributed-exchange-partial-io.md),
  [ADR 0320](0320-canonical-nullable-float64-grouped-exchange.md)

## Context

The canonical grouped frame assigned exact bytes but still required an embedding to retain partial
stream reads and short-write progress. Unbounded buffers, prefix decoding, coalesced-frame loss, or
copyable write cursors would weaken the ownership guarantees already established for the ungrouped
exchange.

## Decision

`GroupedFloat64ExchangeFrameReader` owns exactly one 136-byte array and a fill count. Each `consume`
copies at most the remaining bytes, reports the exact consumed prefix, and leaves any coalesced
suffix with the caller. A complete frame passes through exact grouped decoding. Decode failure is
sticky, and the reader is neither copyable nor movable.

`GroupedFloat64ExchangeFrameWriteCursor` owns one canonical encoded frame and exposes only its
unwritten suffix. Checked advancement rejects an overrun before mutation. Moving transfers the sole
write obligation and forces the source cursor complete; zero-byte progress is valid.

These classes add no sockets, queues, retries, blocking, authentication, sequencing policy, or
grouped coordinator semantics. They do not change the 136-byte frame.

## Consequences and validation

Retained storage remains constant and allocation-free after construction. Every byte is copied or
exposed once, and peer-controlled coalesced suffixes never enter the fixed reader.

Focused tests enumerate all 137 two-part split positions, process coalesced grouped frames through
reported consumption, prove corruption remains sticky, verify exact write suffixes and overrun
rollback, and prove moved-from cursors are inert. The installed-consumer gate covers both public
carrier interfaces.

Grouped transport envelopes, coordinator merge/duplicate state, fragment planning, ordering,
top-N, LIMIT, and broader socket/fault evidence remain incomplete. No Phase 16 exit gate is claimed.

Invariants 6, 10, 11, 14, 15, and 18 apply.

## References

- [Distributed Grouped FLOAT64 Aggregate Exchange
  v1](../formats/distributed-grouped-float64-exchange-v1.md)
- [Bounded distributed exchange partial I/O](0162-bounded-distributed-exchange-partial-io.md)
- [Canonical nullable-FLOAT64 grouped exchange](0320-canonical-nullable-float64-grouped-exchange.md)
