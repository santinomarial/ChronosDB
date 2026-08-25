# Distributed Vector Grouped Aggregate Query Transport v2

> **Status:** accepted and implemented exact response codec and partial-I/O contract. Requests
> reuse the exact Fragment-v2 `CHDVREQ2` carrier. Authenticated receiver, finite sender/retry,
> mutual-TLS/TCP ownership, and multi-tablet scheduling remain separate follow-on boundaries.

All integers are unsigned little-endian. Reserved bytes are zero. CRC32C detects accidental damage
and is not authentication. The nested grouped payload retains its own independent checksums.

## Request

The request is exactly [Distributed Vector Query Transport
v2](distributed-vector-query-transport-v2.md) `CHDVREQ2`. A grouped sufficient-state endpoint must
additionally admit only a `GROUPED_AGGREGATE` Fragment-v2 plan and bind its exact key and aggregate
authority from current local schema/snapshot proof. Sharing the request preserves one canonical
general-vector dispatch; endpoint capability and the distinct response magic prevent row,
ungrouped-state, and grouped-state response confusion.

## Response

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVGRP2` |
| 8 | 2 | major | `2` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `112` |
| 16 | 8 | frame length | Exact header + payload + trailer |
| 24 | 8 | source node | Nonzero |
| 32 | 8 | target node | Nonzero and different from source |
| 40 | 16 | query UUID | Nonzero |
| 56 | 16 | tablet UUID | Nonzero |
| 72 | 1 | status | Fixed v1 status-code mapping `0..12` |
| 73 | 1 | payload kind | `0` absent, `1` grouped aggregate exchange v1 |
| 74 | 1 | flags | Bit 0 means leader hint present |
| 75 | 1 | reserved | Zero |
| 76 | 4 | payload length | Exact nested length |
| 80 | 4 | payload CRC32C | Zero iff absent |
| 84 | 4 | reserved | Zero |
| 88 | 8 | leader node | Nonzero iff hint present |
| 96 | 8 | placement epoch | Nonzero iff hint present |
| 104 | 4 | reserved | Zero |
| 108 | 4 | header CRC32C | CRC32C of bytes `[0,108)` |
| 112 | variable | payload | One exact [grouped aggregate exchange v1](distributed-vector-grouped-aggregate-exchange-v1.md) on success |
| final - 4 | 4 | frame CRC32C | CRC32C of every preceding byte |

`OK` requires kind 1. Every failure status requires kind 0, zero payload length, and zero payload
CRC. Nested query/tablet identity must exact-match the outer response. The complete response is
`116..67,108,980` bytes.

## Authority, resource, and I/O ownership

Every encoder, exact decoder, reader, and write-cursor constructor requires the complete ordered
group-key and aggregate-definition vectors. Decoder and reader additionally require the query
resource context that owns decoded variable key bytes and nested variable extrema. Both authority
vectors and every nested limit are validated even for failure responses, so an error-only path
cannot omit or weaken the admitted schema contract.

The reader validates magic, fixed header CRC, versions, reserved fields, identity, status/payload
shape, hard payload bounds, caller nested-frame bound, and caller outer-frame bound before exact
allocation. It consumes at most one frame, leaves a coalesced successor caller-owned, and retains
sticky frame failure. Exact decode then validates the complete outer CRC, payload CRC, nested frame,
authority, and correlation. The move-only cursor exposes only its unwritten suffix and leaves a
moved-from cursor complete.

Unknown versions are `NOT_SUPPORTED`; damage, type confusion, and wire contradiction are
`CORRUPTION`; invalid local authority/limits are `INVALID_ARGUMENT`; lower deployment bounds and
allocation failure are `RESOURCE_EXHAUSTED`. The carrier owns no socket, peer authentication,
retry policy, clock, coordinator, or process lifecycle.
