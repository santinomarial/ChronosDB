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

After publishing a response, the single response producer calls `notify_response_ready()`. A
nonblocking close-on-exec `eventfd` coalesces notifications and interrupts `epoll_wait`; the owner
drains it before the response ring. The producer may call only this method concurrently and must be
joined before shutdown. SPSC release/acquire remains the data-visibility edge; eventfd is readiness,
not shared-memory synchronization.

Admission is bounded by configured connection, readiness-event, and per-event I/O-operation limits,
so a continuously ready listener or peer must yield to another poll. Per-connection bounds remain those
of ADRs 0061 and 0062. Request-ring saturation produces `OVERLOADED` and removes that request from
the active set. Responses for a closed connection, cancelled/completed request, mismatched request
kind, or invalid terminal payload are dropped and counted. Output-buffer saturation closes the
connection because another error frame cannot be admitted within the same bound.

Accepted sockets enable `TCP_NODELAY` before they become observable to epoll. Protocol terminal
frames are distinct bounded buffers and may require distinct `send` calls; allowing Nagle to hold a
small `QUERY_END` behind its preceding result introduced an observed delayed-ACK-sized completion
penalty. If the socket option cannot be established, admission fails and is counted rather than
silently changing the declared latency behavior.

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

The baseline is single-owner and bounded. TLS record I/O and worker scheduling may be
added behind the boundary but cannot weaken overload, identity, or cleanup. Concurrent reactor
mutation is outside the contract.

ADR 0175 later factors the portable public RAII ownership and nonblocking completion rules for
IPv4 listener/connector descriptors used outside this native-protocol reactor.

## Validation

Linux real-socket tests cover fragmented reads, response routing, queue overload, admission, and
slow-handshake expiry. Portable tests prove the non-Linux boundary and self-contained public header.
Startup allocations are exhaustively failed; queue concurrency is covered separately under TSan.
The Phase 10 Linux benchmark compares equal-work query rounds across 1, 8, and 32 connections and
retains the pre-decision delayed-terminal observation as the evidence for `TCP_NODELAY`.

## References

- [ADR 0009](0009-network-reactor-strategy.md)
- [ADR 0061](0061-native-protocol-handshake-and-request-lifecycle.md)
- [ADR 0062](0062-bounded-connection-buffer-ownership.md)
- [ADR 0063](0063-bounded-reactor-shard-spsc-routing.md)
- [Native Protocol v1](../protocol/native-v1.md)
- [ADR 0175](0175-nonblocking-ipv4-tcp-descriptor-ownership.md)
