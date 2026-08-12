# ADR 0253: Ownership-Safe Raft TCP Connect Attempt

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft transport and networking maintainers

## Context

The outbound Raft TLS carrier borrows its descriptor, while reconnect retry must retain complete
frames during a nonblocking TCP attempt. Returning only a TLS carrier from a connector would leave
its descriptor owned by a shorter-lived object and create a dangling transport lifetime.

## Decision

`RaftTransportTcpConnector` owns one exact IPv4 nonblocking connect attempt and a bounded set of
complete retry frames. It validates every frame's canonical source/destination and aggregate carrier
capacity before opening a socket, requires the authentication IP to equal the route, enforces a
positive monotonic connect deadline, and creates TLS only after authoritative `SO_ERROR` completion.

On connection, the retry set moves through one prevalidated batch admission into a fresh
`RaftTransportTlsClient`. The batch preflights all slots and bytes and then performs only vector
moves into preallocated carrier slots. The connector returns `RaftTransportConnectedPeer`, which
owns the `TcpSocket` before the borrowing TLS carrier in declaration order; reverse destruction
therefore releases TLS first. The production peer pool retains that pair and returns both on failed
carrier handoff. Externally descriptor-owned test embeddings may still add a bare carrier.

A connect error or exact timeout closes the descriptor but retains every original retry frame for
the policy owner. The connector owns no backoff, address resolution, alternative-address choice, or
retry budget.

## Consequences and validation

TCP/TLS lifetime is now explicit across connector, pool, and failure handoff. Focused real-loopback
mutual-TLS coverage destroys the connector after taking the pair, then sends the retained canonical
frame through the pool, proving the descriptor remains alive. Additional tests prove exact timeout
returns all frames and a foreign route is rejected without consuming caller ownership. Connection
churn, address catalogs, capped backoff, and replacement scheduling remain separate work.

**Retrospective note (2026-08-12):** [ADR 0254](0254-capped-raft-peer-reconnect-policy.md) now owns
one exact peer's retry deadlines, capped exponential delay, and complete frames between attempts.
A bounded multi-peer poll/catalog owner remains separate.

## References

- [ADR 0175](0175-nonblocking-ipv4-tcp-descriptor-ownership.md)
- [ADR 0177](0177-deadline-bound-distributed-query-tcp-client.md)
- [ADR 0248](0248-persistent-outbound-raft-mtls-carrier.md)
- [ADR 0251](0251-bounded-raft-peer-carrier-pool.md)
