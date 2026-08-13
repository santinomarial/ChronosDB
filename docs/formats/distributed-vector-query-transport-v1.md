# Distributed Vector Query Transport v1

> **Status:** accepted with implemented exact codecs and bounded partial-I/O ownership.
> Authentication, scheduling, and execution remain separate.

This distinct cluster request carries one group-scoped vector dispatch to an exact remote node. All
integers are little-endian. CRC32C detects accidental damage; it is not authentication.

## Request

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHDVREQ1` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Header length `80` |
| 16 | 8 | Exact complete request length |
| 24 | 8 | Nonzero source node |
| 32 | 8 | Nonzero, distinct target node |
| 40 | 8 | Exact dispatch payload length |
| 48 | 4 | CRC32C of dispatch payload |
| 52 | 24 | Zero reserved bytes |
| 76 | 4 | CRC32C of bytes `[0,76)` |
| 80 | variable | One exact [Vector Fragment Dispatch v1](distributed-vector-fragment-dispatch-v1.md) |
| final - 4 | 4 | CRC32C of every preceding request byte |

The payload is `292..84,264` bytes and the complete request is `376..84,348` bytes. Decode checks
physical bounds, magic, and header integrity before trusting lengths, then exact version, route,
reserved fields, length relationships, complete integrity, payload integrity, and the nested
dispatch. Unknown checksum-valid versions are unsupported; damage and contradictions are
corruption. Encoding rejects invalid routes or nested dispatches before publication.

## Response

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHDVRSP1` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Header length `112` |
| 16 | 8 | Exact complete response length |
| 24 | 8 | Nonzero source node |
| 32 | 8 | Nonzero, distinct target node |
| 40 | 16 | Nonzero query UUID |
| 56 | 16 | Nonzero tablet UUID |
| 72 | 1 | Status code |
| 73 | 1 | Payload kind: `0` absent, `1` vector exchange |
| 74 | 1 | Flags; bit 0 means leader hint present |
| 75 | 1 | Zero reserved byte |
| 76 | 4 | Exact payload length |
| 80 | 4 | CRC32C of payload, or zero when absent |
| 84 | 4 | Zero reserved bytes |
| 88 | 8 | Leader node, or zero |
| 96 | 8 | Leader placement epoch, or zero |
| 104 | 4 | Zero reserved bytes |
| 108 | 4 | CRC32C of bytes `[0,108)` |
| 112 | variable | One exact [Distributed Vector Exchange v1](distributed-vector-exchange-v1.md) |
| final - 4 | 4 | CRC32C of every preceding response byte |

Status codes use the fixed numeric order `OK`, `CANCELLED`, `INVALID_ARGUMENT`, `OUT_OF_RANGE`,
`NOT_FOUND`, `ALREADY_EXISTS`, `CORRUPTION`, `IO_ERROR`, `RESOURCE_EXHAUSTED`, `UNAVAILABLE`,
`NOT_SUPPORTED`, `UNAUTHENTICATED`, and `INTERNAL`, numbered 0 through 12. `OK` requires kind 1;
failure requires kind 0, zero payload length, and zero payload CRC. A payload exact-matches the
response query/tablet; sequence and terminal state remain the later coordinator's authority. A
leader hint requires both nonzero fields and is advisory only. The complete response is
`116..16,777,192` bytes.

## Compatibility and ownership

The magic is distinct from aggregate and grouped query transports; no decoder accepts another
protocol's request or response. Minor-version compatibility is exact in v1. Exact decoders borrow
input only for the call and return value-owned dispatch/exchange bytes. The codecs do not yet define
authentication, retry, or socket ownership.

The noncopyable, nonmovable request and response readers retain only their fixed headers until
header integrity, physical length relationships, hard maxima, and caller frame limits pass. They
then allocate exactly one frame, consume at most that caller prefix, leave coalesced successors
caller-owned, exact-decode before publication, and retain sticky frame failure. Invalid reader-limit
configuration consumes nothing. The move-only frame cursor accepts only a complete exact vector
request or response, exposes its unwritten suffix, rejects over-acknowledgement without advancing,
and leaves a moved-from cursor complete.
