# ADR 0248: Persistent Outbound Raft Mutual-TLS Carrier

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft, cluster transport, networking, and security maintainers

## Context

Post-synchronization Raft transitions can emit several frames for distinct peers. The sender for one
peer needs bounded buffering, mutual authentication, short-write ownership, deadlines, and a safe
reconnect contract. Retrying only the unwritten suffix on a new TLS stream would create invalid
framing, while dropping a partially written message would obscure liveness and recovery behavior.

## Decision

`RaftTransportTlsClient` owns one persistent outbound stream to one exact peer over a borrowed
nonblocking `TlsSocket`. A single event-loop thread owns all methods. Creation allocates a fixed-slot
ring; configured frame and byte totals bound all queued ownership. `try_enqueue` decodes and
validates the canonical frame, exact-matches its source and destination to the connection, checks
both queue bounds, and moves caller bytes only on successful admission.

The carrier completes mutual TLS, derives the server's stable principal, and authorizes that
principal for the configured peer node before writing any queued byte. It then writes FIFO frames
across arbitrary TLS short writes. Handshake and each active front frame have finite monotonic
deadlines. No cross-thread synchronization is used: the event-loop thread exclusively owns socket,
ring indexes, byte counts, and write offsets.

On terminal failure, `drain_retry_frames` returns every complete original frame in FIFO order. The
front frame is returned from offset zero even if a prefix reached the failed connection. Retrying the
whole canonical message can duplicate a Raft RPC, which the deterministic protocol already handles;
retrying a suffix would not be a valid frame. Drain reserves its complete result before moving any
owned bytes, so allocation failure leaves the internal retry set intact.

## Consequences

Outbound bytes are bounded, peer-authenticated, ordered, deadline-controlled, and recoverable for
reconnect. The carrier deliberately owns no address resolution, TCP connect, connection pool,
backoff, peer replacement, routing table, or retransmission decision. An embedding groups frames by
destination, creates/replaces one carrier per peer, and decides when drained duplicates are retried.

**Retrospective note (2026-08-12):** [ADR 0251](0251-bounded-raft-peer-carrier-pool.md) now provides
the fixed-capacity exact-peer map, durable-result routing preflight, and explicit failed-carrier
handoff. TCP connection establishment, address policy, backoff, and replacement automation remain
outside the carrier and pool.

The default 64 MiB queue can hold only one maximum-size default frame; operators must configure
append batching and carrier budgets together. Queue admission never truncates a message.

## Validation

Focused real mutual-TLS tests queue before handshake, authenticate the server, deliver two FIFO
frames over one connection, enforce frame-count backpressure without consuming the rejected caller
value, reject a route mismatch, reject an unauthorized peer before writing, expire the exact
handshake deadline, and recover the full queued frame for reconnect. Forced partial-prefix socket
failure, connection churn, queue fairness, and large-frame memory pressure remain in Phase 18.

## References

- [ADR 0243](0243-canonical-raft-transport-envelope.md)
- [ADR 0247](0247-persistent-inbound-raft-mtls-carrier.md)
- [ADR 0172](0172-maintained-mutual-tls-client-socket.md)
- [ADR 0173](0173-bounded-outbound-distributed-query-tls-carrier.md)
