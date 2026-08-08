# ADR 0062: Bounded Connection Buffer Ownership

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** ChronosDB networking maintainers

## Context

Nonblocking sockets fragment and coalesce frames and complete writes partially. The reactor needs a
portable ownership layer that cannot grow with a slow or malicious peer.

## Accepted decision

Each connection owns one bounded inbound byte vector and a bounded FIFO of immutable encoded
outbound frames. Header validation determines the exact body requirement before further parsing.
Complete frames are decoded in order; an incomplete suffix remains owned for the next read.
Outbound progress advances an explicit offset in the front frame, never mutates its bytes, and
releases that frame immediately after its final byte.

Inbound bytes, outbound bytes, and outbound frame count have independent finite limits. Admission
fails immediately with resource exhaustion; there is no side queue or blocking socket write.
Disconnect clears every buffer and offset.

## Detailed rationale

Immutable frame ownership makes short-write resumption and lifetime obvious. Separate byte and
frame-count limits cover both large frames and many empty frames. The portable layer allows every
fragmentation boundary to be tested without epoll.

## Alternatives considered

Unbounded vectors violate peer-controlled allocation safety. Scatter/gather retention adds lifetime
complexity before profiling. Blocking writes allow one peer to occupy a reactor. Per-read decoding
incorrectly assumes syscall boundaries equal frame boundaries.

## Consequences

The reactor toggles writable interest from queue state and applies its configured overload action
when enqueue fails. Buffer compaction is linear in retained suffix bytes and can be optimized only
with measured evidence.

## Affected invariants

This supports invariants 11, 15, and 17 through explicit lifetime, finite slow-peer influence, and
validation-before-allocation.

## Validation plan

Every two-part split, coalesced frames, partial suffixes, short writes, queue/byte saturation,
allocation sweeps, fuzzing, sanitizers, and reactor socket tests.

## Deferred decisions

Timeout values, exact disconnect versus overload-response policy, epoll interest management, and
shard queue ownership remain later increments.

## Migration or reversal implications

Changing internal compaction is nonbreaking if limits and byte order remain exact. Weakening finite
admission or frame immutability requires a superseding ADR.

## References

- [ADR 0009](0009-network-reactor-strategy.md)
- [Native Protocol v1](../protocol/native-v1.md)
