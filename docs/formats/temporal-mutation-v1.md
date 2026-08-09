# Temporal Mutation Command v1

> **Status: canonical codec is implemented; WAL commit/recovery application is the next integration.**

Temporal Mutation Command v1 is WAL application format `1`, kind `3`. It stores one schema-shaped
Columnar Batch v1 plus row-aligned metadata for original versions, corrections, replacements, and
tombstones. WAL record sequence remains the authoritative system commit position; the command
stores the system commit timestamp chosen by the commit owner. All integers are little-endian.

The standard 16-byte WAL application envelope is followed by this body:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `43 48 52 4e 54 4d 50 00` (`CHRNTMP\0`) |
| 8 | 2 | Major `1` |
| 10 | 2 | Minor `0` |
| 12 | 4 | Header size `96` |
| 16 | 4 | Total body size including trailer |
| 20 | 4 | Embedded Columnar Batch v1 size |
| 24 | 4 | Mutation/row count |
| 28 | 4 | Mutation metadata size |
| 32 | 16 | Table UUID |
| 48 | 16 | Schema UUID |
| 64 | 8 | Schema version |
| 72 | 8 | System commit time in nanoseconds (`i64`) |
| 80 | 4 | CRC32C of embedded batch |
| 84 | 4 | CRC32C of mutation metadata |
| 88 | 4 | CRC32C of header with this field zero |
| 92 | 4 | Required zero |

The header is followed by the exact embedded batch, then one row-aligned descriptor per batch row:
kind (1), three zero bytes, logical-identity length (4), event time (8), receive time (8), and exact
identity bytes. Kinds `1`–`4` mean original, correction, replacement, and tombstone. Logical
identities are nonempty, bounded, and unique within a command. The final four bytes are CRC32C over
the complete body excluding that trailer.

Decoding validates the envelope, header before length-driven slicing, all checksums, exact size
relationships, limits, descriptor exhaustion, batch row count, and repeated table/schema/version
identity. Schema-dependent column validation occurs before committed application. Unknown versions
or application identity are unsupported; damaged canonical bytes are corruption.
