# ADR 0247: Persistent Inbound Raft Mutual-TLS Carrier

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft, cluster transport, networking, and security maintainers

## Context

The authenticated receiver accepts complete frames, but production sockets deliver arbitrary TLS
record fragments and must never call the single-owner durable runtime directly. A Raft receive can
emit messages for multiple peers and can request external snapshot installation, so treating the
inbound socket as a request/response channel would misroute work or discard non-response results.

## Decision

`RaftTransportTlsServer` owns one persistent inbound stream over one borrowed nonblocking
`TlsSocket`. One event-loop thread serializes handshake, read, state, and result-pickup calls. The
carrier completes mutual TLS, derives a stable authenticated principal through the configured
authenticator, and only then reads Envelope v1 bytes through `RaftTransportFrameReader`.

The carrier uses a fixed 16 KiB scratch buffer and limits each TLS read to the current header or
frame remainder, so a read never crosses the exact frame boundary. It admits at most one decoded
frame to `RaftTransportReceiver`, stops reading while the asynchronous durable completion is
pending, and exposes the complete group/source/result value to the embedding. It resumes the same
authenticated stream only after that value is moved out.

Handshake and incomplete-frame reads have finite monotonic deadlines. Accepted durable work has no
carrier deadline because it cannot be cancelled after the single owner may have mutated and
synchronized state. `AsyncDurableRaftCompletion::wait` supplies the mutex acquire edge for the
complete result; all carrier calls otherwise remain on one event-loop thread. The embedding routes
outbound messages and handles snapshot-install and read-barrier results; the inbound carrier never
assumes they belong on the source connection.

The receiver's decoded-value entry point independently reapplies its configured semantic and size
limits before authorization and runtime admission. This prevents an in-process carrier with looser
limits from bypassing the receiver policy.

## Consequences

Mutual-TLS authentication, bounded fragmented reads, asynchronous persist-before-result release,
and persistent connection backpressure now compose without network types entering the Raft core.
Each session owns at most one configured frame, one 16 KiB scratch buffer, one admitted operation,
or one completed durable result. Carrier-wide connection and total-memory admission remain external.

Completion readiness currently requires the event loop to poll or receive an embedding wakeup;
there is no completion descriptor. Outbound TLS connection pooling, retry, routing queues, accepted-
socket admission, peer replacement, and snapshot application remain separate tasks.

## Validation

Focused socket-pair tests use real mutual TLS, authenticate the client certificate, deliver frames
in seven-byte application writes, process two sequential elections on one connection, retain each
post-sync transition until pickup, reject a denied principal before admission, and expire the
handshake at the exact deadline. Receiver tests reject values outside its own configured limits.
Disconnect races, storage stalls/failures, connection churn, and process-level fault matrices remain
in the Phase 18 ledger.

**Retrospective note (2026-08-12):** [ADR 0248](0248-persistent-outbound-raft-mtls-carrier.md) adds
the symmetric peer-authenticated bounded FIFO sender and complete-frame reconnect drain.

## References

- [ADR 0245](0245-bounded-raft-transport-partial-io.md)
- [ADR 0246](0246-authenticated-raft-transport-receiver.md)
- [ADR 0114](0114-bounded-asynchronous-multi-raft-owner.md)
- [ADR 0144](0144-maintained-mutual-tls-socket-carrier.md)
- [ADR 0174](0174-bounded-inbound-distributed-query-tls-carrier.md)
