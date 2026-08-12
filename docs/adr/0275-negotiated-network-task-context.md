# ADR 0275: Preserve negotiated context in network tasks

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB network, service, and runtime maintainers

## Context and decision

The reactor validates frames against connection-owned protocol version, feature bits, and payload
limit, but `NetworkTask` previously retained only route identity and the frame. A downstream service
could not independently distinguish negotiated QUORUM_SYNC from forged protocol-v2-looking bytes.

Every dispatched request and reactor-generated cancellation now carries an immutable
`NetworkTaskProtocolContext`: selected major/minor, negotiated feature bits, and negotiated maximum
payload size copied from `ServerConnectionState`. The SPSC queue publishes this context through the
same release/acquire edge as the owning frame. Services must consume these captured facts rather
than reconstruct capabilities from frame headers or server defaults.

Responses may preserve the context incidentally but the reactor continues to validate them against
its live connection state; response bytes do not acquire new authority from task metadata. No
network or durable bytes change.

## Consequences and validation

The task grows by a small fixed amount and both epoll and io_uring dispatch/cancellation paths copy
the same fields. Queue tests prove FIFO ownership preserves the complete context. Real protocol-v2
reactor propagation, disconnect saturation, Linux io_uring, and TSan matrices remain deferred.

## References

- [ADR 0271](0271-native-protocol-v2-quorum-sync-negotiation.md)
- [ADR 0274](0274-nonblocking-replicated-ingest-operation.md)
