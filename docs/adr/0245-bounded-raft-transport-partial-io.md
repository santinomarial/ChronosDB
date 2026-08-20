# ADR 0245: Bounded Raft Transport Partial-I/O Ownership

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft and cluster transport maintainers

## Context

Raft Transport Envelope v1 defines complete canonical frames, while nonblocking TLS and TCP can
deliver a header or frame in arbitrary fragments and can accept only part of an outbound write.
Allocating the declared frame before authenticating its fixed header would trust damaged length
state. Retaining caller-owned read or write buffers across readiness callbacks would create unclear
lifetime and mutation hazards.

## Decision

Provide a move-only `RaftTransportFrameReader` and `RaftTransportFrameWriteCursor` in
`chronos_raft`. The reader retains a fixed 96-byte header, verifies its checksum and every
allocation-relevant field against configured limits, and only then allocates the exact complete
frame. It owns partial input until one fully decoded envelope is returned. A successful return names
the exact consumed prefix so a caller can resubmit any coalesced suffix. A parse or allocation
failure is sticky for the reader lifetime.

The write cursor accepts only a complete frame that passes the canonical decoder, owns those bytes,
and exposes a pending span plus checked short-write advancement. Moving it transfers ownership and
leaves the source complete. Neither type owns a descriptor, TLS object, retry policy, clock, or
Raft runtime.

## Consequences

Socket carriers can use readiness-driven partial I/O without borrowing transient buffers or
allocating from unchecked wire lengths. Each active reader can hold at most one configured frame,
but the default limit remains 64 MiB, so connection admission and carrier-wide memory budgets still
belong above this API. The codec intentionally returns at most one frame per consume call to make
dispatch and backpressure explicit.

## Validation

Focused tests cover byte-at-a-time reads, fragmented payloads, coalesced frames and exact consumed
prefixes, sticky damaged-header failure, checked short writes, move ownership, invalid complete
frames, and configured frame limits. Deterministic allocator sweeps complete the final header byte
under injection, fail every observed exact-frame and nested append/snapshot decode allocation, and
prove `RESOURCE_EXHAUSTED` is sticky. Move-owned write-cursor validation is swept through exact
success without publishing partial ownership. Real TLS descriptors, carrier-wide admission,
disconnects, and hostile long-running partial-I/O schedules remain in the Phase 18 ledger.

## References

- [ADR 0243](0243-canonical-raft-transport-envelope.md)
- [Raft Transport Envelope v1](../formats/raft-transport-v1.md)
- [ADR 0062](0062-bounded-connection-buffer-ownership.md)
- [ADR 0144](0144-maintained-mutual-tls-socket-carrier.md)
