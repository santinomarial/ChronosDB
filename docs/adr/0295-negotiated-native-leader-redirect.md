# ADR 0295: Negotiated native leader redirect

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB networking, query, ingest, and distributed-systems maintainers

## Context

A multi-Raft node can accept a native request while another node leads the required group. Returning
an unstructured error loses the exact group, observed leader term, and placement generation needed
for bounded client routing. Silently proxying is outside the native connection owner's authority,
and accepting a redirect after query rows have been emitted would make whole-request retry unsafe.

## Decision

Protocol 2.0 assigns feature bit 2 (`0x4`) to `LEADER_REDIRECT` and server-only message type `13`.
Both peers must negotiate the feature. The fixed 48-byte payload carries format 1, six zero reserved
bytes, a non-nil Raft group UUID, nonzero leader node ID, nonzero observed leader term, and nonzero
placement epoch. It deliberately carries no endpoint: the deployment's authenticated client-routing
configuration, not a Raft peer endpoint, maps the stable node ID to a native service address.

A redirect is a terminal response only for an active ingest or finite query request and only before
any acknowledgement or query-result batch. It is an observation, not a lease or proof that the named
node remains leader. Clients must bound retries, reject stale placement observations, and repeat the
normal consistency proof at the destination. Subscriptions are excluded because their resumable
handoff has a separate committed-boundary contract.

Generic framing requires Protocol 2 for type 13. Message codecs require canonical exact bytes. The
client session, server connection state, epoll response path, and io_uring response path all validate
the negotiated capability and terminal lifecycle before releasing request ownership.

## Consequences and validation

Protocol 1 bytes remain unchanged and reject type 13 and feature bit 2. A server may understand the
codec without advertising it; packaged service wiring controls advertisement. Focused codec tests
cover round trip, reserved bytes, and semantic identities. Client/server lifecycle tests cover
negotiation, terminal completion, and rejection after partial query output.

This decision supplies the transport response but does not select a redirect from live Raft state,
configure native endpoints, or retry a request. Those are separate service/client composition tasks.

## References

- [Native Protocol v2](../protocol/native-v2.md)
- [ADR 0271](0271-native-protocol-v2-quorum-sync-negotiation.md)
- [ADR 0294](0294-applied-replicated-read-barrier-vector.md)
- [Consistency and durability](../product/consistency-and-durability.md)
