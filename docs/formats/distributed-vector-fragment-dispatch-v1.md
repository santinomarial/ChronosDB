# Distributed Vector Fragment Dispatch v1

> **Status: accepted and implemented.** Decoded values are structurally complete but are not
> executable until constructed by a committed-authority binder and revalidated by the target
> worker.

All integers are little-endian. UUIDs use canonical network byte order. Reserved bytes are zero.
The maximum complete frame is 84,264 bytes.

## Header

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHDVFDP1` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Header length `232` |
| 16 | 8 | Exact complete frame length |
| 24 | 16 | Query UUID |
| 40 | 16 | Database UUID |
| 56 | 16 | Table UUID |
| 72 | 16 | Tablet UUID |
| 88 | 16 | Destination schema UUID |
| 104 | 16 | Raft group UUID |
| 120 | 8 | Snapshot generation |
| 128 | 8 | Serving node ID |
| 136 | 8 | Applied position |
| 144 | 8 | Observed leader commit position |
| 152 | 8 | Placement epoch |
| 160 | 8 | Maximum staleness positions, or zero |
| 168 | 8 | Linearizable barrier term, or zero |
| 176 | 8 | Linearizable barrier context, or zero |
| 184 | 8 | Linearizable barrier read index, or zero |
| 192 | 4 | Projection ordinal count |
| 196 | 4 | Flags |
| 200 | 1 | `DistributedReadConsistency` code |
| 201 | 3 | Reserved zero |
| 204 | 8 | Lower event-time bound, or zero |
| 212 | 8 | Upper event-time bound, or zero |
| 220 | 4 | Exact nested plan length |
| 224 | 4 | Reserved zero |
| 228 | 4 | CRC32C of bytes `[0,228)` |

Flag bits 0/1 mean lower present/inclusive, bits 2/3 upper present/inclusive, bit 4 maximum
staleness present, and bit 5 linearizable barrier present. All other bits are zero. Absent numeric
values are zero; inclusive requires present.

The body is `projection_count` unique 32-bit destination-schema ordinals, followed by one exact
[Distributed Vector Plan Intent v1](distributed-vector-plan-intent-v1.md), followed by CRC32C of
every preceding outer-frame byte. Projection count is `1..4096`; the nested plan's input indices
are bounded by that exact count. The nested plan retains its independent header and complete CRCs.

Leader-linearizable, follower-bounded-stale, and local-eventual proof relationships match
Distributed Aggregate Fragment v1. Every identity, snapshot generation, serving node, and placement
epoch is nonzero. Header CRC validation precedes use of projection/plan lengths; the complete outer
CRC precedes projection allocation and nested decode. Unknown versions are unsupported, lower
caller limits are resource exhaustion, and damaged or contradictory bytes are corruption.

## Stream ownership

`DistributedVectorFragmentReader` retains only the fixed 232-byte header until its CRC, the derived
exact outer length, the hard nested-plan byte bound, and caller frame/projection limits pass. It then
allocates the exact complete frame; caller plan-shape limits are enforced by exact nested decode
before publication. It consumes at most one frame per call and reports the exact consumed prefix so
a coalesced successor remains caller-owned. A frame error after bytes have been retained is sticky;
invalid caller limit configuration consumes nothing and is not sticky.

`DistributedVectorFragmentWriteCursor` owns one exact encoded frame and exposes only its pending
suffix. Checked acknowledgements cannot advance beyond that suffix. Moving the cursor transfers the
frame and makes the source complete. Neither state machine defines socket scheduling or security.

Unlike the aggregate path's historical two-layer fragment/dispatch formats, this new format carries
Raft group identity from version one. CRC is not authentication. A production binder must still
derive every field from one committed metadata and Manifest snapshot plus the exact read admission;
the worker must reprove local route, group, schema, position, and storage authority before I/O.
