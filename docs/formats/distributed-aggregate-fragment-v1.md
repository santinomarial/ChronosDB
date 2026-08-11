# Distributed Aggregate Fragment v1

> **Status:** accepted with implemented canonical owned encoding and exact bounded decoding.

This frame describes one snapshot-bound, single-tablet worker request for the current projected
Float64 aggregate path. All integers are little-endian. UUIDs use canonical UUID byte order. The
header is fixed; the body is a bounded vector of destination-schema column ordinals.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHDFRAG1` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Header length `216` |
| 16 | 8 | Exact complete frame length |
| 24 | 16 | Query UUID |
| 40 | 16 | Database UUID |
| 56 | 16 | Table UUID |
| 72 | 16 | Tablet UUID |
| 88 | 16 | Destination schema UUID |
| 104 | 8 | Aggregate database snapshot generation |
| 112 | 8 | Serving node ID |
| 120 | 8 | Applied tablet position |
| 128 | 8 | Observed leader commit position |
| 136 | 8 | Placement epoch |
| 144 | 8 | Maximum staleness positions, or zero |
| 152 | 8 | Linearizable barrier term, or zero |
| 160 | 8 | Linearizable barrier context, or zero |
| 168 | 8 | Linearizable barrier read index, or zero |
| 176 | 4 | Projection ordinal count |
| 180 | 4 | Aggregate input index within the projection |
| 184 | 4 | Flags |
| 188 | 1 | `DistributedReadConsistency` code |
| 189 | 3 | Zero reserved bytes |
| 192 | 8 | Lower event-time bound value, or zero |
| 200 | 8 | Upper event-time bound value, or zero |
| 208 | 4 | Zero reserved bytes |
| 212 | 4 | CRC32C of bytes `[0, 212)` |
| 216 | `4 * projection_count` | Unique destination-schema ordinals |
| final 4 | 4 | CRC32C of every preceding frame byte |

Flag bits 0/1 mean lower bound present/inclusive, bits 2/3 mean upper bound
present/inclusive, bit 4 means maximum staleness present, and bit 5 means linearizable barrier
present. Every other bit is zero. An inclusive bit requires its corresponding presence bit. An
absent optional numeric field is zero. A present event-time predicate has at least one bound.

Projection count is `1..4096`, subject to a lower caller limit. Ordinals are unique and less than
4096; the aggregate input index is less than the projection count. Exact frame length is
`220 + 4 * projection_count`, at most 16,604 bytes.

Leader-linearizable requests carry a nonzero barrier and no staleness bound, with applied position
at least its read index. Bounded-stale requests carry a staleness bound, no barrier, a nonzero
observed leader commit position, and lag within the bound. Local-eventual requests carry neither
proof. Every request has nonzero identities, snapshot generation, serving node, and placement
epoch.

Decoding validates caller limits, minimum/maximum physical size, magic, and header CRC before any
length or count controls interpretation. It then validates version/header fields and exact length,
checks the complete-frame CRC before allocating or interpreting projection entries, and finally
checks canonical semantics. A checksum-valid unknown version is unsupported; configured projection
excess is resource exhaustion; damaged or contradictory bytes are corruption.

The frame does not prove that a local snapshot matches the named generation or position and does
not authenticate the sender. Worker admission and an authenticated carrier must independently
reprove those facts before scan execution.
