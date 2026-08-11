# Distributed Aggregate Exchange v1

> **Status:** accepted with implemented canonical owned encoding and exact borrowed decoding.

This fixed-width frame carries one mergeable ungrouped aggregate state from a tablet worker to its
coordinator. All integers are unsigned little-endian. UUID fields use the canonical UUID byte order;
floating-point fields preserve their IEEE-754 binary64 bits.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHDXCHG1` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Exact frame length `128` |
| 16 | 16 | Query UUID |
| 32 | 16 | Tablet UUID |
| 48 | 8 | Per-tablet message sequence, nonzero |
| 56 | 8 | Aggregate count |
| 64 | 8 | Sum, IEEE-754 binary64 bits |
| 72 | 8 | Minimum, IEEE-754 binary64 bits |
| 80 | 8 | Maximum, IEEE-754 binary64 bits |
| 88 | 8 | Mean, IEEE-754 binary64 bits |
| 96 | 8 | M2, IEEE-754 binary64 bits |
| 104 | 4 | Flags |
| 108 | 16 | Zero reserved bytes |
| 124 | 4 | CRC32C of bytes `[0, 124)` |

Flag bit 0 is `terminal`, bit 1 is `minimum present`, and bit 2 is `maximum present`. All other
bits are zero. The extrema-presence bits must agree. When the extrema are absent, both encoded
extrema fields are positive zero. An empty state has count zero, no extrema, and positive-zero sum,
mean, and M2. A nonempty state has both extrema.

Exact decoding requires exactly 128 bytes. It checks magic and CRC32C before interpreting version,
identity, sequence, aggregate state, flags, or reserved bytes. A checksum-valid unknown major or
minor version is unsupported. Bad length, integrity, identity, flags, reserved bytes, or canonical
aggregate state is corruption. Invalid values supplied to the encoder are invalid arguments.

CRC32C is an accidental-corruption boundary, not authentication. An authenticated transport and
query admission remain independently required. This frame does not serialize a physical plan,
grouping state, ordering state, cancellation, or retry policy.
