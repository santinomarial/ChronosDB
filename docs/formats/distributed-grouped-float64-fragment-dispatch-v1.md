# Distributed Grouped FLOAT64 Fragment Dispatch v1

> **Status:** accepted with implemented canonical owned encoding and exact bounded decoding.

This envelope binds one exact
[Distributed Grouped FLOAT64 Fragment Intent v1](distributed-grouped-float64-fragment-intent-v1.md)
to the Raft group whose log positions scope its nested consistency proof. All integers are
little-endian; the two inner layers retain their own versions and integrity boundaries.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHDGDSP1` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Header length `80` |
| 16 | 8 | Exact complete dispatch length |
| 24 | 16 | Nonzero Raft group UUID |
| 40 | 8 | Exact grouped-intent length |
| 48 | 28 | Zero reserved bytes |
| 76 | 4 | CRC32C of bytes `[0, 76)` |
| 80 | variable | One exact Grouped FLOAT64 Fragment Intent v1 frame |
| final 4 | 4 | CRC32C of every preceding dispatch byte |

The inner length is `268..16648` and equals the exact bytes between dispatch header and trailer.
Complete dispatch length is `84 + inner_length`, or `352..16732` bytes.

Decoding validates physical bounds, magic, and header CRC before using either length. It then
requires exact version/header/length/reserved fields, verifies the complete dispatch CRC, validates
the nonzero group identity, and exact-decodes the grouped intent under the caller's projection
limit. A checksum-valid unknown envelope version is unsupported. Inner failures retain their exact
classification.

Distinct magic makes every grouped dispatch invalid to the ungrouped decoder and vice versa. CRC32C
detects damage, not malicious group substitution. An authenticated receiver must exact-match the
decoded group to local tablet authority and invoke a worker that independently reproves every
nested route, placement, schema, snapshot, and consistency field before storage access.

A compatible grouped snapshot owner now derives every plan-ordered dispatch from one pinned
Manifest epoch and the exact schema binding that proves its shared key input is FLOAT64. It changes
no dispatch bytes.
