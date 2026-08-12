# ChronosDB Native Protocol v2

> **Status: Protocol 2.0 framing, negotiation, QUORUM_SYNC request, and receipt codecs are
> implemented. Replicated service advertisement and execution are not yet enabled.**

Protocol 2.0 inherits the complete [Native Protocol v1](native-v1.md) framing, limits, type
assignments, payloads, request lifecycle, security boundary, and subscription semantics except for
the additions below. Existing Protocol 1 bytes never acquire Protocol 2 meaning.

## Negotiation

The first `CLIENT_HELLO` and its `SERVER_HELLO` response use Protocol 1.0 frame headers. A client
offers a major range containing `2`; a compatible server may select major `2`, minor `0`. All later
frames on that connection MUST carry exactly `2.0`. A v2-only client sets both major bounds to `2`.

Protocol 2.0 supports feature bit 0 (subscriptions) and assigns feature bit 1 (`0x2`) to
`QUORUM_SYNC`. Both peers must offer/enable bit 1. A server without a configured replicated ingest
owner MUST omit it even if its frame parser understands Protocol 2.

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

## Compatibility and rejection

- Protocol 1 decoders reject message type 12 and durability value 3.
- Protocol 2 without negotiated feature bit 1 rejects durability value 3.
- Unknown majors, nonzero Protocol 2 minors, unknown features, malformed receipt identities, mixed
  post-handshake versions, and mismatched acknowledgement kinds fail closed.
- Hello framing remains Protocol 1.0 so a v1 peer can select v1 or report no compatible version.

Golden mixed-version fixtures, fuzz campaigns, authenticated replicated service execution, and
crash reconciliation are tracked in the Phase 18 validation ledger.
