# ADR 0246: Authenticated Raft Transport Receiver

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft, cluster transport, security, and recovery maintainers

## Context

Canonical Raft frames and partial-I/O ownership do not themselves establish peer identity, local
routing, or persist-before-response dispatch. Passing decoded messages directly from a carrier to
`RaftNode` could permit a certificate principal to claim another node, route a frame to the wrong
process, call the single-thread-affine runtime from a reactor, or release response bytes before the
associated term/vote/log state is synchronized.

## Decision

`RaftTransportReceiver` is the authenticated ingress boundary for one complete Envelope v1 frame.
It rejects an absent or zero stable principal before decoding any bytes, decodes under configured
bounds, authorizes that principal for the claimed source node, exact-matches the destination to the
local node, and only then nonblockingly submits one owning `ReceiveOperation` to
`AsyncDurableMultiRaftRuntime`.

Successful admission returns an owning completion. The existing asynchronous durable owner releases
that completion only after any associated persistence record is appended and locally synchronized.
`encode_durable_raft_outbound_v1` borrows, rather than consumes, the completed transition while
validating its group and local-source ownership and encoding each outbound envelope. Consequently,
an encoding or configured-size error does not silently discard the only owned Raft response.

The receiver borrows the authorizer and runtime. Calls may originate from multiple carrier
producers because the runtime owns FIFO serialization; the authorizer must provide its own
synchronization. The receiver does not own descriptors, TLS sessions, stream readers, timers,
retries, snapshot application, or response queues.

## Consequences

Authentication, application authorization, routing, asynchronous ownership, and local durable
release now compose without placing networking in the deterministic Raft core. An authenticated
frame for an unknown group is admitted to the sole runtime owner and returns `NOT_FOUND` as an
ordinary operation result without failing the runtime. Runtime admission backpressure is preserved.

A production carrier must still authenticate TLS before passing plaintext to a stream reader,
retain completed transitions until all encoded frames are accepted by an owning write queue, and
handle snapshot-install/application work. Append suffixes larger than the configured frame bound
remain an explicit batching gap rather than being truncated.

## Validation

Focused tests prove authenticated vote receipt through durable completion and canonical response
encoding; reject unauthenticated bytes before decode; reject unauthorized sources and wrong
destinations before runtime admission; and surface an unknown group without terminal runtime
failure. Real mutual-TLS carrier scheduling, disconnect/retry ownership, output queue bounds,
snapshot-install composition, and fault matrices remain in the Phase 18 ledger.

**Retrospective note (2026-08-12):** [ADR 0247](0247-persistent-inbound-raft-mtls-carrier.md) composes
the receiver with persistent bounded mutual-TLS input and publishes the complete durable result for
embedding-owned routing and snapshot handling.

## References

- [ADR 0243](0243-canonical-raft-transport-envelope.md)
- [ADR 0245](0245-bounded-raft-transport-partial-io.md)
- [ADR 0114](0114-bounded-asynchronous-multi-raft-owner.md)
- [ADR 0144](0144-maintained-mutual-tls-socket-carrier.md)
- [ADR 0168](0168-authenticated-distributed-query-transport.md)
