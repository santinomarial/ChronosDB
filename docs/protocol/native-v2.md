# ChronosDB Native Protocol v2

> **Status: Protocol 2.0 framing, negotiation, QUORUM_SYNC request/receipt, and negotiated leader
> redirect are implemented. Packaged replicated QUORUM_SYNC, exact ingest leader redirect,
> deadline-bound native TCP/mutual-TLS client replay, exact transport-independent finite-query
> redirect replay, and applied-barrier SELECT execute.**

Protocol 2.0 inherits the complete [Native Protocol v1](native-v1.md) framing, limits, type
assignments, request lifecycle, security boundary, and Protocol 1.1 subscription payloads except
for the additions below. Its subscription change remains payload format 1 and WAL-only; the later
Protocol 1.2 source-tagged payload does not retroactively change accepted Protocol 2.0 bytes.
Existing Protocol 1 bytes never acquire Protocol 2 meaning.

## Negotiation

The first `CLIENT_HELLO` and its `SERVER_HELLO` response use Protocol 1.0 frame headers. A client
offers a major range containing `2`; a compatible server may select major `2`, minor `0`. All later
frames on that connection MUST carry exactly `2.0`. A v2-only client sets both major bounds to `2`.

Protocol 2.0 supports feature bit 0 (subscriptions) and assigns feature bit 1 (`0x2`) to
`QUORUM_SYNC`. Both peers must offer/enable bit 1. A server without a configured replicated ingest
owner MUST omit it even if its frame parser understands Protocol 2.

Feature bit 2 (`0x4`) enables `LEADER_REDIRECT`. A server MUST omit it until its request service can
derive exact redirect observations from authoritative placement and ordered Raft state. The
packaged replicated-ingest service satisfies that condition for its one derived tablet group.

## Ingest request durability

The inherited `INGEST_REQUEST` envelope assigns durability value `3` to `QUORUM_SYNC` only when
feature bit 1 was negotiated. Values `1` and `2` retain their v1 meanings. An unnegotiated value `3`
is an invalid request and cannot reach an ingest worker.

## QUORUM_SYNC acknowledgement

Message type `12`, `QUORUM_SYNC_INGEST_ACKNOWLEDGEMENT`, is server-to-client only and valid only in
Protocol 2. Its payload is exactly 64 bytes:

| Offset | Width | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 2 | payload format | `1` |
| 2 | 1 | requested durability | `3` (`QUORUM_SYNC`) |
| 3 | 1 | effective durability | `3`; downgrade is forbidden |
| 4 | 1 | outcome | `1` applied, `2` matching retry |
| 5 | 3 | reserved | zero |
| 8 | 16 | Raft group UUID | non-nil canonical bytes |
| 24 | 8 | leader node ID | nonzero u64 |
| 32 | 8 | leader term | nonzero u64 |
| 40 | 8 | log index | nonzero u64 |
| 48 | 8 | entry term | nonzero u64 |
| 56 | 8 | local durable physical sequence | nonzero u64 |

The surrounding frame CRC32C covers these exact bytes. The fields reproduce the immutable receipt
from ADR 0074; they do not constitute a lease or authorize a later acknowledgement. A type-11 WAL
acknowledgement cannot complete a `QUORUM_SYNC` request, and type 12 cannot complete an `ASYNC` or
`LOCAL_SYNC` request.

## Leader redirect

Message type `13`, `LEADER_REDIRECT`, is server-to-client only, Protocol-2-only, and requires
negotiated feature bit 2. Its payload is exactly 48 bytes:

| Offset | Width | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 2 | payload format | `1` |
| 2 | 6 | reserved | zero |
| 8 | 16 | Raft group UUID | non-nil canonical bytes |
| 24 | 8 | leader node ID | nonzero u64 |
| 32 | 8 | observed leader term | nonzero u64 |
| 40 | 8 | placement epoch | nonzero u64 |

The response terminally completes an ingest or finite query only before any acknowledgement or
query-result batch. It is invalid for subscriptions and invalid after partial query output. The
stable node ID is resolved through deployment-owned authenticated native endpoints; a Raft peer
endpoint is not a native client endpoint. The observation is not a lease, so a retry must still
prove the requested consistency at the destination.

The client library provides a bounded policy owner for this join. It exact-binds one group and a
canonical node-to-native-endpoint/TLS map, rejects stale placement or term authority, conflicting
same-term leaders, self redirects, and unknown nodes, and stops after a configured redirect count.
The strict `CHRONOSDB_NATIVE_CLIENT_ROUTES_V1` deployment file supplies only that endpoint, expected
TLS identity, and certificate-principal map; it cannot assert group membership or leader authority.
The owning TLS-route composition securely loads and qualifies that map plus local client
credentials, creates one immutable expected-identity context per node, and publishes stable route
pointers only after complete construction.
The QUORUM_SYNC replay owner above it retains one exact append, creates a fresh Protocol 2 session
and connection-local request ID for every accepted destination, and publishes only a receipt that
matches the bound group/current route and does not regress the redirect term. Those policy owners
remain transport-independent. `NativeQuorumIngestTcpClient` composes them with nonblocking TCP,
mutual TLS, certificate-principal-to-node authorization, bounded partial I/O, and separate connect,
handshake, and exchange deadlines. It reconnects only for a validated redirect; an ambiguous
transport failure is terminal. `NativeQuorumIngestTcpExecution` supplies the constant-storage poll
owner above that client, bounding each wait by both the active carrier phase and an optional whole-
operation deadline while preserving explicit cancellation and terminal-only receipt publication.
`NativeQueryRetry` provides the corresponding finite-query policy owner. It retains exact SQL across
fresh Protocol 2 sessions, enforces aggregate batch/row/byte limits, and publishes owned encoded
result batches only after `QUERY_END`. `NativeQueryTcpClient` composes it with the same nonblocking
mutual-TLS, certificate-principal-to-node authorization, partial-I/O, reconnect, and phase-deadline
rules as ingest. `NativeQueryTcpExecution` adds bounded poll scheduling, whole-operation deadline,
cancellation, and terminal route/attempt metrics. These owners do not make a multi-group query
redirectable through one group.

## Compatibility and rejection

- Protocol 1 decoders reject message types 12 and 13, durability value 3, and Protocol-2 feature
  bits.
- Protocol 2 without negotiated feature bit 1 rejects durability value 3.
- Unknown majors, nonzero Protocol 2 minors, unknown features, malformed receipt identities, mixed
  post-handshake versions, and mismatched acknowledgement kinds fail closed.
- Hello framing remains Protocol 1.0 so a v1 peer can select v1 or report no compatible version.

Golden mixed-version fixtures, fuzz campaigns, authenticated replicated service execution, and
crash reconciliation are tracked in the Phase 18 validation ledger.
