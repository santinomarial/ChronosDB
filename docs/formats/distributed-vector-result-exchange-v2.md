# Distributed Vector Result Exchange v2

> **Status: accepted and implemented.** This frame is distinct from Distributed Vector Exchange v1.
> Its optional payload is exactly one Native Protocol v1 `QUERY_RESULT` payload whose descriptors
> must equal a separately supplied Distributed Vector Result Schema v1.

All integers are unsigned little-endian. Reserved bytes are zero. UUIDs are network-order bytes.
The maximum result payload is 16,777,216 bytes and the maximum complete frame is 16,777,300 bytes.
An enclosing transport or caller may impose smaller bounds. CRC32C detects accidental damage; it is
not authentication.

## Layout

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDXVEC2` |
| 8 | 2 | major | `2` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `80` |
| 16 | 8 | frame length | Exact header + result batch + trailer length |
| 24 | 16 | query ID | Nonzero |
| 40 | 16 | tablet ID | Nonzero |
| 56 | 8 | sequence | Positive; enclosing stream contract requires contiguity |
| 64 | 4 | flags | Bit 0 is `TERMINAL`; all others zero |
| 68 | 4 | result batch length | Exact nested byte length |
| 72 | 4 | header CRC32C | CRC32C of bytes `[0,72)` |
| 76 | 4 | reserved | Zero |
| 80 | variable | result batch | Empty only when `TERMINAL`; otherwise exact native `QUERY_RESULT` payload |
| final - 4 | 4 | frame CRC32C | CRC32C of every preceding frame byte |

A data frame may also be terminal. A terminal-only frame closes an empty tablet stream under the
separately authorized result schema. A nonempty zero-row native result batch is also legal because
its descriptors remain present.

## Schema binding and cells

The codec API requires one valid [Distributed Vector Result Schema
v1](distributed-vector-result-schema-v1.md) obtained from the admitted Fragment v2 context. Every
nonempty nested result batch is exact-decoded under finite native protocol row, column, name, and
payload limits. Its ordered descriptor count, names, logical type codes and parameters, and
nullability must exactly equal the supplied schema. Duplicate names are legal. Terminal-only frames
still require a valid supplied schema even though no descriptors appear in their bytes.

Cell bytes, row-major order, `0xffffffff` NULL sentinel, fixed widths, Boolean representation,
decimal precision, UTF-8 rules, and zero-row semantics are unchanged from [Native Protocol
v1](../protocol/native-v1.md). This frame adds no table UUID, schema UUID, roles, or expression
identity.

## Validation and ownership

Exact decoding validates caller configuration and the expected schema, then hard physical length,
caller frame limit, magic, header checksum, exact version and layout, caller result-payload bound,
complete checksum, identities, terminal/empty consistency, canonical nested payload, and exact
descriptor equality. Unknown checksum-valid versions are unsupported. Damage, contradictions,
trailing bytes, invalid identities, and descriptor mismatch are corruption. Lower caller frame,
payload, and expected-schema bounds are resource exhaustion; nested row/cell validation retains the
Native Protocol v1 status contract. Invalid caller limits or expected schemas are invalid arguments.

The streaming reader owns the fragment-bound schema. It retains only the 80-byte header until the
header checksum, hard and caller lengths, flags, reserved bytes, and nested byte bound pass. It then
owns exactly one declared frame, consumes no coalesced successor bytes, exact-decodes before
publication, and retains sticky frame failure. The move-only write cursor validates and owns one
complete encoded frame, exposes only the unwritten suffix, rejects over-acknowledgement without
advancing, and leaves a moved-from cursor complete.

Exchange v1 remains unchanged and table-shaped. V1 and v2 decoders reject each other's magic. Cluster
request/response carriage, authenticated lifecycle, sequencing coordination, and worker execution
are separate versioned contracts.
