# Distributed Vector Result Schema v1

> **Status: accepted and implemented.** This schema-light value follows the established Native
> Protocol v1 result descriptor model and does not carry table roles or synthetic catalog identity.

All integers are unsigned little-endian. Reserved bytes are zero. The maximum column count is 4,096,
the maximum UTF-8 name is 1,024 bytes, and the maximum frame is 4,259,892 bytes.

## Layout

The 48-byte header contains: magic `CHDVRSC1` (8), major/minor `1.0` (2 each), header length (4),
exact frame length (8), column count (4), descriptor-byte length (4), CRC32C of bytes `[0,32)` (4),
and 12 zero bytes. Descriptors follow in result order. Each has logical type code and two parameters
(u16 each), nullable (u8 0/1), zero u8, UTF-8 name length (u32), zero u32, and exact nonempty name
bytes. Duplicate names are legal because SQL output aliases need not be unique. The final u32 is
CRC32C of every preceding byte.

Header integrity and physical length relationships pass before descriptor allocation. Unknown
versions are unsupported, format violations are corruption, and lower caller frame/column/name
limits return resource exhaustion. Decoding owns every name and descriptor.

Shape validation separately proves the ordered descriptor type/nullability against a Vector Plan
Intent and its exact projected physical inputs. Names remain caller-bound SQL identities. Row
projection may repeat; grouped outputs remain keys followed by aggregates; aggregate result shapes
reuse the local vector aggregate oracle. ORDER BY and LIMIT do not change output shape.
