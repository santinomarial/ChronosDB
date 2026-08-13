# Distributed Vector Query Transport v1

> **Status:** accepted with implemented exact request codec. Response framing, stream ownership,
> authentication, scheduling, and execution remain separate.

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

## Compatibility and ownership

The magic is distinct from aggregate and grouped query transports; no decoder accepts another
protocol's request. Minor-version compatibility is exact in v1. The exact decoder borrows input
only for the call and returns a value-owned dispatch. This request codec does not yet define partial
I/O, response/failure bytes, authentication, retry, or socket ownership.
