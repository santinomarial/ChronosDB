# ADR 0251: Bounded Raft Peer Carrier Pool

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft transport and cluster networking maintainers

## Context

Durable Multi-Raft transitions may contain messages for several peers. Encoding and enqueueing one
message at a time can partially consume a transition before discovering a missing peer or a full
later queue. The event-loop owner also needs an exact mapping between authenticated outbound TLS
carriers and peer node identities, plus a lossless handoff when a carrier fails.

## Decision

`RaftTransportPeerPool` is a move-only, single-event-loop, fixed-capacity map from peer node ID to
one `RaftTransportTlsClient`. Creation preallocates every peer slot. Adding a carrier exact-matches
its local node to the pool, rejects duplicates, and never silently replaces a live connection.

`route_result` borrows an already durable transition. Before moving any frame, it verifies that
every destination has a carrier, computes every canonical encoded length, aggregates frame and byte
demand per peer, and compares the totals with each carrier's remaining fixed queue capacity. It
then encodes the whole transition, which exact-validates group and local-source ownership, before
enqueueing the frames to their exact peers.

This preflight prevents missing-route, failed-carrier, aggregate-capacity, allocation, and validation
failures from partially admitting a durable result. Whole-result encoding completes before enqueue.
The pool then uses a private prevalidated carrier admission path that only moves vectors into
already allocated slots. If that path reports an invariant failure despite the single-threaded
preflight, routing stops with `CORRUPTION`; the caller still owns the unchanged durable transition
and must treat retry as potentially duplicate. Raft messages are duplicate-safe, but suffix-only
retry is prohibited.

Failed carriers are not removed implicitly. `take_failed_peer` first reserves and drains every
complete original frame, including a partially written front frame from offset zero, and only then
removes the carrier. If retry-vector allocation fails, the failed carrier and all queued bytes stay
owned by the pool. The returned carrier and frames give the reconnect owner an explicit handoff.

## Consequences

Outbound durable results now have bounded exact-peer routing, batch-level capacity preflight, and a
failure-safe reconnect boundary. The pool deliberately does not resolve addresses, create TCP
sockets, choose TLS contexts, schedule backoff, or decide retransmission lifetime. Those policies
belong to a connector/replacement owner layered above this already-connected carrier pool.

Linear peer lookup is bounded by `maximum_peers`; this favors simple fixed ownership during the
architecture phase. A measured need can justify a preallocated index later without changing the
carrier or retry contracts.

## Validation

Focused tests prove that a missing destination queues nothing, aggregate per-peer exhaustion queues
nothing, a full later peer does not change earlier queues, pool/carrier ownership and capacity are
enforced, and a failed peer is removed only after its complete canonical retry frame is returned and
decoded. Phase 18 retains allocation-failure injection between peer enqueues, sustained churn,
fairness, and large-peer measurements.

## References

- [ADR 0243](0243-canonical-raft-transport-envelope.md)
- [ADR 0248](0248-persistent-outbound-raft-mtls-carrier.md)
- [ADR 0250](0250-async-durable-raft-timer-driver.md)
