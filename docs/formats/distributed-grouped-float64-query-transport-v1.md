# Distributed Grouped FLOAT64 Query Transport v1

> **Status:** accepted with implemented exact request/response codecs and bounded partial-I/O
> ownership and authenticated receiver dispatch. TLS and socket lifecycle remain separate.

This cluster protocol carries one group-scoped grouped FLOAT64 fragment dispatch to a remote worker
and correlates each returned grouped partial, empty-stream terminal, or failure. It is distinct from
Distributed Query Transport v1 because grouped execution can return multiple partial frames and has
a separate terminal-only representation. All integers are little-endian. CRC32C detects accidental
damage; it is not peer authentication.

## Request

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHDGREQ1` |
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
| 80 | variable | One exact [Grouped FLOAT64 Fragment Dispatch v1](distributed-grouped-float64-fragment-dispatch-v1.md) |
| final 4 | 4 | CRC32C of every preceding request byte |

The payload is `352..16732` bytes and the complete request is `436..16816` bytes. Decoding checks
physical bounds, magic, and header integrity before trusting lengths, then exact version, route,
reserved fields, length relationships, complete integrity, payload integrity, and the nested
dispatch. Checksum-valid unknown versions are `NOT_SUPPORTED`.

## Response

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHDGRSP1` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Header length `112` |
| 16 | 8 | Exact complete response length |
| 24 | 8 | Nonzero source node |
| 32 | 8 | Nonzero, distinct target node |
| 40 | 16 | Nonzero query UUID |
| 56 | 16 | Nonzero tablet UUID |
| 72 | 1 | Status code |
| 73 | 1 | Payload kind: `0` absent, `1` grouped partial, `2` empty terminal |
| 74 | 1 | Flags; bit 0 means leader hint present |
| 75 | 1 | Zero reserved byte |
| 76 | 4 | Payload length: `0`, `64`, or `136` |
| 80 | 4 | CRC32C of payload, or zero when absent |
| 84 | 4 | Zero reserved bytes |
| 88 | 8 | Leader node, or zero |
| 96 | 8 | Leader placement epoch, or zero |
| 104 | 4 | Zero reserved bytes |
| 108 | 4 | CRC32C of bytes `[0, 108)` |
| 112 | variable | Exact payload selected by kind |
| final 4 | 4 | CRC32C of every preceding response byte |

Payload kind 1 is one exact 136-byte
[Grouped FLOAT64 Aggregate Exchange v1](distributed-grouped-float64-exchange-v1.md). Payload kind 2
is its distinct 64-byte empty-stream terminal. Both must exact-match the header query and tablet;
their nonzero sequence remains owned by the bounded grouped coordinator. Kind 0 requires zero
length and CRC. The only valid complete response lengths are 116, 180, and 252 bytes.

Status codes use the fixed numeric order `OK`, `CANCELLED`, `INVALID_ARGUMENT`, `OUT_OF_RANGE`,
`NOT_FOUND`, `ALREADY_EXISTS`, `CORRUPTION`, `IO_ERROR`, `RESOURCE_EXHAUSTED`, `UNAVAILABLE`,
`NOT_SUPPORTED`, `UNAUTHENTICATED`, and `INTERNAL`, numbered 0 through 12. `OK` requires payload kind
1 or 2; every failure requires kind 0. A leader hint requires both nonzero fields and remains
advisory only.

## Compatibility and ownership

Distinct request and response magics make these frames invalid to the ungrouped transport decoders
and vice versa. Minor-version compatibility is exact in v1. Reserved fields and flags must remain
zero unless a later accepted version defines them.

The exact codecs return value-owned dispatches and payloads. Fixed-storage request and response
readers first integrity-check the complete header, retain no more than their protocol maximum,
consume at most one frame, leave coalesced successors caller-owned, and fail sticky. A move-only
write cursor accepts only an exact grouped request or response and exposes its checked unwritten
suffix; moving leaves the source complete.

The implemented receiver requires carrier-supplied peer authentication, authorizes the claimed
source, exact-matches the local target, invokes an embedding-owned grouped worker once, and returns
only a completely validated and encoded response-frame vector. It enforces correlated contiguous
sequence and terminal placement under a configured frame bound. Unavailable worker failures may
acquire an advisory leader hint from the committed metadata provider only after those trust gates.

These primitives do not define multiple-response connection closure, retry arbitration, TLS, TCP,
or a production real-CSEG service adapter. The network owner must preserve response order and must
not publish partial query success if the connection fails mid-stream.
