# ADR 0271: Native Protocol 2.0 QUORUM_SYNC negotiation

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB networking, durability, and distributed-systems maintainers
- **Supersedes:** no prior bytes; extends the handshake range from Protocol 1 to Protocol 2

## Context

Native Protocol 1 fixes durability values `1` (`ASYNC`) and `2` (`LOCAL_SYNC`). ADR 0061 requires a
new protocol major before changing that registry. The Raft runtime and tablet state machine can now
produce the proof required by ADR 0074, but accepting value `3` in a Protocol 1 payload would make
an old byte sequence acquire new meaning and violate invariant 14.

The existing hello payload already carries a major-version range. Its framing must remain readable
by a Protocol 1 peer so an old server can select v1 or reject a v2-only client cleanly.

## Decision

The Protocol 1.0-framed `CLIENT_HELLO` and `SERVER_HELLO` exchange may select Protocol 2.0. Every
post-handshake frame then carries major `2`, minor `0`. Protocol 2 inherits the bounded 40-byte
frame, message registry, payload limits, request lifecycle, subscriptions, and all Protocol 1
payloads without reinterpreting their accepted bytes.

Protocol 2 assigns feature bit 1 to `QUORUM_SYNC`. A server advertises it only when its configured
ingest owner can preserve ADR 0074 through committed tablet application. An ingest request may use
durability value `3` only in a selected Protocol 2 session with that bit negotiated. Protocol 1 and
Protocol 2 sessions without the bit reject value `3` before dispatch.

Message type `12`, `QUORUM_SYNC_INGEST_ACKNOWLEDGEMENT`, is server-only and Protocol-2-only. Its
fixed 64-byte payload carries requested/effective `QUORUM_SYNC`, the applied or matching-retry
outcome, and the complete current `QuorumSyncReceipt`: group UUID, leader node and term, log index,
entry term, and the leader's covering local durable physical sequence. All identities and
sequences are nonzero. A conventional WAL acknowledgement (type `11`) is never accepted for a
request that asked for `QUORUM_SYNC`, and a client never silently accepts a different effective
mode.

Negotiation is capability admission, not proof by itself. The acknowledgement may be encoded only
after the leader remains current, the stable or joint majority proof covers the exact entry, the
leader commit transition is synchronized, and tablet application covers the index. Losing
leadership, cancellation, timeout, storage failure, or inability to reconstruct the proof returns a
terminal error rather than a weaker acknowledgement.

## Consequences

- Frozen Protocol 1 frames and payloads remain byte-for-byte compatible and still reject value `3`
  and type `12`.
- Generic frame parsers accept only majors 1 and 2. Session state requires every post-handshake
  frame to match the selected major and minor exactly.
- Single-node servers continue to advertise Protocol 1 by default. Enabling Protocol 2 or its
  feature bit is explicit configuration owned by the replicated service composition.
- The 64-byte acknowledgement is immutable Protocol 2.0 network state. Extending it requires a
  compatible negotiated extension or a later protocol version.

## Affected invariants and validation

Invariants 1, 4, 5, 8, 9, 10, 14, and 18 apply. Focused tests preserve the v1 golden frame, reject
the new type and durability under v1, negotiate v2 through a v1-framed hello, require the feature
bit, round-trip the exact receipt, reject malformed receipt fields, and require the matching client
and server acknowledgement lifecycle.

Authenticated replicated service integration, cancellation/timeouts while awaiting commit,
minority-loss crash reconciliation, mixed-version process matrices, metrics, and sustained fuzzing
remain required before a deployment enables the feature.

## References

- [Native Protocol v2](../protocol/native-v2.md)
- [Native Protocol v1](../protocol/native-v1.md)
- [ADR 0061](0061-native-protocol-handshake-and-request-lifecycle.md)
- [ADR 0074](0074-quorum-sync-proof-boundary.md)
- [Consistency and durability](../product/consistency-and-durability.md)
