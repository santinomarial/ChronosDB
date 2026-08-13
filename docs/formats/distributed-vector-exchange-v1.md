# Distributed Vector Exchange v1

> **Status: accepted and implemented.** This is a distinct frame from the frozen aggregate and
> grouped exchange protocols. Its optional payload is exactly one canonical Columnar Batch v1.

All integers are unsigned little-endian. Reserved bytes are zero. UUIDs are network-order bytes.
The maximum frame length is 16,777,076 bytes. An enclosing transport may impose smaller bounds.

## Layout

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDXVEC1` |
| 8 | 2 | major | `1` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `80` |
| 16 | 8 | frame length | Exact header + batch + trailer length |
| 24 | 16 | query ID | Nonzero |
| 40 | 16 | tablet ID | Nonzero |
| 56 | 8 | sequence | Positive, contiguous within the enclosing stream contract |
| 64 | 4 | flags | Bit 0 is `TERMINAL`; all others zero |
| 68 | 4 | batch length | Exact nested byte length |
| 72 | 4 | header CRC32C | CRC32C of bytes `[0,72)` |
| 76 | 4 | reserved | Zero |
| 80 | variable | batch | Empty only when `TERMINAL`; otherwise exact Columnar Batch v1 |
| final - 4 | 4 | frame CRC32C | CRC32C of every preceding frame byte |

A data frame may also be terminal. A terminal-only frame closes an empty stream without inventing a
zero-row Columnar Batch, which Columnar Batch v1 deliberately forbids.

## Validation and compatibility

Readers validate magic and the header checksum before trusting lengths, enforce caller bounds,
require exact framing and canonical flags/reserved bytes, validate the complete frame checksum, then
exact-decode the nested Columnar Batch under caller-supplied row/column/byte limits. Truncation,
trailing bytes, checksum failure, identity failure, and canonical contradictions fail closed.
Unknown versions are unsupported. The nested batch retains its own independent version, integrity,
logical-type, UTF-8, decimal, shape, and padding rules.

This frame supplies result correlation and safe all-type batch movement. Stream coordination,
physical-plan requests, schema authorization, transport, and execution are separate protocols.
