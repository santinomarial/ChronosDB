# Distributed Vector Query Transport v2

> **Status:** accepted and implemented exact codec/partial-I/O contract. An authenticated,
> schema-bound receiver owns the worker handoff and complete bounded response publication.
> Single-attempt mutual-TLS carriers own authenticated partial I/O and deadlines. A production
> request-local row worker and heap-stable inbound service owner now supply proof-revalidated
> real-CSEG execution through the bounded authenticated TCP/mTLS stack. A pinned portable owner now
> joins finite senders to the result coordinator, and a multi-tablet TCP scheduler prevalidates
> routes, drives due attempts, and owns deadline/cancellation teardown. A bounded final pass now
> applies row-mode global ordering and LIMIT before native-result rebatching. Aggregate execution,
> authority rebinding, and process integration remain separate.

All integers are unsigned little-endian. Reserved bytes are zero. CRC32C detects accidental damage
and is not authentication. Request and response payloads retain their own independent checksums.

## Request

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVREQ2` |
| 8 | 2 | major | `2` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `80` |
| 16 | 8 | frame length | Exact header + payload + trailer |
| 24 | 8 | source node | Nonzero |
| 32 | 8 | target node | Nonzero and different from source |
| 40 | 8 | payload length | Exact Fragment-v2 length |
| 48 | 4 | payload CRC32C | CRC32C of exact payload |
| 52 | 24 | reserved | Zero |
| 76 | 4 | header CRC32C | CRC32C of bytes `[0,76)` |
| 80 | variable | payload | One exact [Distributed Vector Fragment v2](distributed-vector-fragment-v2.md) |
| final - 4 | 4 | frame CRC32C | CRC32C of every preceding byte |

The complete request is the 84-byte outer overhead plus the exact nested Fragment-v2 length, up to
`4,344,308` bytes. V1 and v2 request decoders reject each other's magic.

## Response

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVRSP2` |
| 8 | 2 | major | `2` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `112` |
| 16 | 8 | frame length | Exact header + payload + trailer |
| 24 | 8 | source node | Nonzero |
| 32 | 8 | target node | Nonzero and different from source |
| 40 | 16 | query UUID | Nonzero |
| 56 | 16 | tablet UUID | Nonzero |
| 72 | 1 | status | Fixed v1 status-code mapping `0..12` |
| 73 | 1 | payload kind | `0` absent, `1` Result Exchange v2 |
| 74 | 1 | flags | Bit 0 means leader hint present |
| 75 | 1 | reserved | Zero |
| 76 | 4 | payload length | Exact nested length |
| 80 | 4 | payload CRC32C | Zero iff absent |
| 84 | 4 | reserved | Zero |
| 88 | 8 | leader node | Nonzero iff hint present |
| 96 | 8 | placement epoch | Nonzero iff hint present |
| 104 | 4 | reserved | Zero |
| 108 | 4 | header CRC32C | CRC32C of bytes `[0,108)` |
| 112 | variable | payload | One exact [Result Exchange v2](distributed-vector-result-exchange-v2.md) on success |
| final - 4 | 4 | frame CRC32C | CRC32C of every preceding byte |

`OK` requires kind 1. Every failure status requires kind 0, zero payload length, and zero payload
CRC. The nested query/tablet must exact-match the outer values. Every response operation also
requires the admitted Fragment-v2 result schema, and a nonempty nested result batch must
exact-match it. The complete response is `116..16,777,416` bytes.

## Decode and ownership

Exact decode validates configuration/schema, physical hard bounds, magic, header checksum, exact
version and layout, caller frame bound where applicable, complete and payload checksums, nested
canonical bytes, schema binding, and correlation. Unknown checksum-valid versions are unsupported;
damage and contradiction are corruption; lower caller bounds are resource exhaustion.

Readers retain only the fixed header before allocating the exact declared frame, publish only after
exact decode, consume at most one frame, leave a coalesced successor caller-owned, and retain sticky
frame failure. The response reader owns its expected schema. The typed move-only cursor constructs
only a validated request or schema-supplied response, exposes its unwritten suffix, rejects
over-acknowledgement, and leaves a moved-from cursor complete.
