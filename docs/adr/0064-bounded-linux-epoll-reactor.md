# ADR 0064: Bounded Linux epoll reactor ownership and overload

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** networking subsystem
- **Supersedes:** none

## Context

Protocol framing, connection buffers, and reactor-to-shard rings are portable components, but the
first server transport needs one authoritative Linux readiness owner. It must keep every retained
byte and file descriptor finite, preserve request identity across the shard handoff, and define
disconnect and overload behavior.

## Decision

`EpollReactor` is a movable PIMPL whose public header exposes no Linux type. One owner thread calls
`poll_once` and `shutdown`; it exclusively owns the listener, epoll descriptor, accepted sockets,
connection state, buffers, interest changes, timeouts, and metrics. Each reactor is the sole
producer of one request SPSC ring and sole consumer of one response SPSC ring. A monotonically
increasing nonzero connection ID disambiguates descriptor reuse; exhaustion rejects admission
rather than reusing an identity.

Admission is bounded by configured connection, readiness-event, and per-event I/O-operation limits,
so a continuously ready listener or peer must yield to another poll. Per-connection bounds remain those
of ADRs 0061 and 0062. Request-ring saturation produces `OVERLOADED` and removes that request from
the active set. Responses for a closed connection, cancelled/completed request, mismatched request
kind, or invalid terminal payload are dropped and counted. Output-buffer saturation closes the
connection because another error frame cannot be admitted within the same bound.

Readable data is consumed before a simultaneous half-close notification. EOF then closes the
connection. Closing detaches active requests, clears retained bytes, removes the descriptor, and
best-effort enqueues cancellations in request-ID order until the request ring is full. Later
responses are deterministically dropped by connection ID. Explicit cancellation removes the
request before publishing its cancellation task, so late results are dropped even if notification
encounters saturation.

Handshake and idle deadlines are progress deadlines checked after each poll. Configuration is
validated before the listener becomes observable. Startup allocation failure closes created
descriptors and returns `RESOURCE_EXHAUSTED`.

## Alternatives considered

- **One thread per connection:** rejected because ownership and memory/backpressure scale poorly.
- **Unbounded queues:** rejected because a stalled shard or client could exhaust memory.
- **Use file descriptors as identity:** rejected because descriptors are reused.
- **Block the reactor on a full shard queue:** rejected because one request could stall all peers.
- **Treat CRC32C as authentication:** rejected; it detects accidental corruption only.

## Consequences

The baseline is single-owner and bounded. Wakeups, TLS record I/O, and worker scheduling may be
added behind the boundary but cannot weaken overload, identity, or cleanup. Concurrent reactor
mutation is outside the contract.

## Validation

Linux real-socket tests cover fragmented reads, response routing, queue overload, admission, and
slow-handshake expiry. Portable tests prove the non-Linux boundary and self-contained public header.
Startup allocations are exhaustively failed; queue concurrency is covered separately under TSan.

## References

- [ADR 0009](0009-network-reactor-strategy.md)
- [ADR 0061](0061-native-protocol-handshake-and-request-lifecycle.md)
- [ADR 0062](0062-bounded-connection-buffer-ownership.md)
- [ADR 0063](0063-bounded-reactor-shard-spsc-routing.md)
- [Native Protocol v1](../protocol/native-v1.md)
