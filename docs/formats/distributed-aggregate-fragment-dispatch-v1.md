# Distributed Aggregate Fragment Dispatch v1

> **Status:** accepted with implemented canonical owned encoding and exact bounded decoding.

This envelope binds one [Distributed Aggregate Fragment v1](distributed-aggregate-fragment-v1.md)
to the Raft group whose log positions give its consistency proof meaning. All integers are
little-endian; the inner payload retains its own version and integrity boundaries.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHDFDSP1` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Header length `80` |
| 16 | 8 | Exact complete dispatch length |
| 24 | 16 | Nonzero Raft group UUID |
| 40 | 8 | Exact inner fragment length |
| 48 | 28 | Zero reserved bytes |
| 76 | 4 | CRC32C of bytes `[0, 76)` |
| 80 | variable | One exact Distributed Aggregate Fragment v1 frame |
| final 4 | 4 | CRC32C of every preceding dispatch byte |

The inner length is `224..16,604` and must equal the exact bytes between the dispatch header and
trailer. Complete dispatch length is `84 + inner_length`, at most 16,688 bytes.

Decoding validates physical bounds, magic, and header CRC before using either length. It then
requires exact version/header/length/reserved fields, verifies the complete dispatch CRC, validates
the nonzero group identity, and exact-decodes the inner frame under the caller's projection limit.
A checksum-valid unknown envelope version is unsupported. Inner failures retain their precise
classification.

CRC32C detects damage, not malicious identity substitution. The authenticated worker receiver must
exact-match the decoded group UUID to its configured tablet group, then independently reprove the
inner tablet, placement epoch, serving node, and snapshot position. A bare inner fragment is not an
executable distributed request because its Raft indexes are otherwise group-ambiguous.
