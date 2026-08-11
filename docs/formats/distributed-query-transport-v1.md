# Distributed Query Transport v1

> **Status:** accepted with implemented exact codecs and authenticated receiver dispatch.

This cluster protocol carries one group-scoped distributed aggregate dispatch to a remote worker and
returns either one terminal aggregate exchange or one status. All integers are little-endian. CRC32C
detects accidental damage; the mutually authenticated carrier and node-principal authorization
establish peer identity.

## Request

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHDQREQ1` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Header length `80` |
| 16 | 8 | Exact complete request length |
| 24 | 8 | Nonzero source node |
| 32 | 8 | Nonzero, distinct target node |
| 40 | 8 | Exact dispatch payload length |
| 48 | 4 | CRC32C of the dispatch payload |
| 52 | 24 | Zero reserved bytes |
| 76 | 4 | CRC32C of bytes `[0, 76)` |
| 80 | variable | One exact [Distributed Aggregate Fragment Dispatch v1](distributed-aggregate-fragment-dispatch-v1.md) |
| final 4 | 4 | CRC32C of every preceding request byte |

The payload is 308..16,688 bytes and the complete request is at most 16,772 bytes. Decoding checks
physical bounds, magic, and header integrity before trusting lengths; it then checks exact version,
route, reserved bytes, total length, complete-frame integrity, payload integrity, and the inner exact
dispatch. A checksum-valid unknown version is `NOT_SUPPORTED`.

## Response

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHDQRSP1` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Header length `112` |
| 16 | 8 | Exact complete response length |
| 24 | 8 | Nonzero source node |
| 32 | 8 | Nonzero, distinct target node |
| 40 | 16 | Nonzero query UUID |
| 56 | 16 | Nonzero tablet UUID |
| 72 | 1 | Status code |
| 73 | 1 | Flags; bit 0 means leader hint present |
| 74 | 2 | Zero reserved bytes |
| 76 | 4 | Payload length: `0` or `128` |
| 80 | 4 | Payload CRC32C, or zero when absent |
| 84 | 4 | Zero reserved bytes |
| 88 | 8 | Leader node, or zero |
| 96 | 8 | Leader placement epoch, or zero |
| 104 | 4 | Zero reserved bytes |
| 108 | 4 | CRC32C of bytes `[0, 108)` |
| 112 | 0 or 128 | One exact [Distributed Aggregate Exchange v1](distributed-aggregate-exchange-v1.md) |
| final 4 | 4 | CRC32C of every preceding response byte |

Status codes use the fixed numeric order `OK`, `CANCELLED`, `INVALID_ARGUMENT`, `OUT_OF_RANGE`,
`NOT_FOUND`, `ALREADY_EXISTS`, `CORRUPTION`, `IO_ERROR`, `RESOURCE_EXHAUSTED`, `UNAVAILABLE`,
`NOT_SUPPORTED`, `UNAUTHENTICATED`, and `INTERNAL`, numbered 0 through 12. `OK` requires the
128-byte payload; every failure forbids it. The exchange must exactly match the header query and
tablet, use sequence 1, and be terminal. A leader hint is advisory and requires both nonzero fields.
The only valid complete response lengths are 116 and 244 bytes.

## Authentication and compatibility

The receiver rejects an unauthenticated peer before decoding. After decoding it authorizes the
authenticated principal for the claimed source node and exact-matches the target to the local node
before invoking the embedding-owned worker. The worker independently revalidates group, placement,
barrier, schema, and snapshot authority. Authentication, authorization, CRC integrity, and Raft
authority are distinct checks; none substitutes for another.

Minor-version compatibility is exact in v1. Reserved fields and flags must remain zero unless a
later accepted version defines them. Stream fragmentation, write ownership, connection deadlines,
and sender retry are carrier responsibilities and do not change these canonical bytes.
