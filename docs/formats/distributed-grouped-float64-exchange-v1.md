# Distributed Grouped FLOAT64 Aggregate Exchange v1

> **Status:** accepted with implemented canonical owned encoding and exact borrowed decoding.

This fixed-width frame carries one nullable FLOAT64 group key and one mergeable Float64 aggregate
state from a tablet worker. It is a distinct protocol from the ungrouped 128-byte exchange and does
not change or extend those frozen bytes. All integers are unsigned little-endian; UUIDs use
canonical UUID order; floating values preserve IEEE-754 binary64 bits except for the group-key
canonicalization below.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHDXGRP1` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Exact frame length `136` |
| 16 | 16 | Query UUID |
| 32 | 16 | Tablet UUID |
| 48 | 8 | Per-tablet message sequence, nonzero |
| 56 | 8 | Canonical nullable FLOAT64 group-key bits |
| 64 | 8 | Aggregate count |
| 72 | 8 | Sum, IEEE-754 binary64 bits |
| 80 | 8 | Minimum, IEEE-754 binary64 bits |
| 88 | 8 | Maximum, IEEE-754 binary64 bits |
| 96 | 8 | Mean, IEEE-754 binary64 bits |
| 104 | 8 | M2, IEEE-754 binary64 bits |
| 112 | 4 | Flags |
| 116 | 16 | Zero reserved bytes |
| 132 | 4 | CRC32C of bytes `[0, 132)` |

Flag bit 0 is `terminal`, bit 1 is `group key present`, bit 2 is `minimum present`, and bit 3 is
`maximum present`. All other bits are zero. An absent group key represents SQL NULL and requires
positive-zero key bits. A present key canonicalizes both signed zeros to positive zero and every
NaN sign/payload to quiet-NaN bits `0x7ff8000000000000`; other bits remain exact. This matches the
existing grouped SQL equality contract, under which signed zeros share a group and all NaNs share a
group.

The extrema flags must agree. Absent extrema use positive-zero fields. An empty aggregate state has
count zero, no extrema, and positive-zero sum, mean, and M2. A nonempty state has both extrema.

Exact decoding requires 136 bytes and checks magic and CRC32C before interpreting the version,
length, identities, flags, key, aggregate state, or reserved bytes. Checksum-valid unknown versions
are unsupported. Noncanonical key/aggregate encodings and all other semantic damage are corruption.
Invalid values supplied to the encoder are invalid arguments.

CRC32C is accidental-corruption coverage, not authentication. The existing authenticated cluster
transport remains a separate trust boundary. This first grouping-state format does not define
multi-key tuples, non-FLOAT64 keys, grouped fragment plans, coordinator merge/order semantics,
partial-I/O ownership, top-N, or LIMIT.
