# Distributed Vector Aggregate Query Transport v2

> **Status:** accepted and implemented exact response codec/partial-I/O contract. Requests reuse
> the exact Fragment-v2 `CHDVREQ2` carrier. Authenticated receiver/sender, TLS/TCP lifecycle,
> retries, worker service integration, and process ownership remain separate.

All integers are unsigned little-endian. Reserved bytes are zero. CRC32C detects accidental damage
and is not authentication. The nested payload retains its own independent checksums.

## Request

The request is exactly [Distributed Vector Query Transport v2](distributed-vector-query-transport-v2.md)
`CHDVREQ2`. An aggregate endpoint additionally admits only an `UNGROUPED_AGGREGATE` Fragment-v2
plan. Sharing the request preserves one canonical general-vector dispatch; endpoint capability and
the distinct response magic prevent row and aggregate response confusion.

## Response

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVARP2` |
| 8 | 2 | major | `2` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `112` |
| 16 | 8 | frame length | Exact header + payload + trailer |
| 24 | 8 | source node | Nonzero |
| 32 | 8 | target node | Nonzero and different from source |
| 40 | 16 | query UUID | Nonzero |
| 56 | 16 | tablet UUID | Nonzero |
| 72 | 1 | status | Fixed v1 status-code mapping `0..12` |
| 73 | 1 | payload kind | `0` absent, `1` Vector Aggregate Exchange v1 |
| 74 | 1 | flags | Bit 0 means leader hint present |
| 75 | 1 | reserved | Zero |
| 76 | 4 | payload length | Exact nested length |
| 80 | 4 | payload CRC32C | Zero iff absent |
| 84 | 4 | reserved | Zero |
| 88 | 8 | leader node | Nonzero iff hint present |
| 96 | 8 | placement epoch | Nonzero iff hint present |
| 104 | 4 | reserved | Zero |
| 108 | 4 | header CRC32C | CRC32C of bytes `[0,108)` |
| 112 | variable | payload | One exact [Vector Aggregate Exchange v1](distributed-vector-aggregate-exchange-v1.md) on success |
| final - 4 | 4 | frame CRC32C | CRC32C of every preceding byte |

`OK` requires kind 1. Every failure status requires kind 0, zero payload length, and zero payload
CRC. Nested query/tablet identity must exact-match the outer response. The complete response is
`116..1,048,908` bytes.

## Definition, resource, and I/O ownership

Every encoder, exact decoder, reader, and write-cursor constructor requires the complete ordered
Fragment-v2 aggregate definition vector. A decoder and reader additionally require the query
resource context that owns any variable-length extremum retained by the nested state. Definitions
and decode limits are validated even for failure responses, so the authority contract cannot be
omitted on an error-only path.

The reader validates the checksummed fixed header, hard bounds, deployment payload bound, and
caller outer-frame bound before exact allocation. It consumes at most one frame, leaves a
coalesced successor caller-owned, and retains sticky frame failure. The move-only cursor exposes
only its unwritten suffix and leaves a moved-from cursor complete. Unknown versions are
unsupported; damage and wire contradiction are corruption; invalid local contracts are invalid
arguments; lower deployment bounds are resource exhaustion.
