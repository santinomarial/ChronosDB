# Tablet Physical Part Chunk v1

> **Status:** accepted with implemented codec, restart-safe durable receipt, streamed completion,
> and capped verified composition into destination CSEG installation.

All integers are unsigned little-endian. UUIDs use durable network byte order. The payload is
nonempty and the frame has no trailing bytes.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHRPCHK\0` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Header length `192` |
| 16 | 8 | Exact encoded frame length |
| 24 | 16 | Table ID |
| 40 | 16 | Tablet ID |
| 56 | 16 | Raft group ID |
| 72 | 8 | Placement epoch |
| 80 | 8 | Source node ID |
| 88 | 8 | Target node ID |
| 96 | 8 | Manifest generation |
| 104 | 16 | CSEG part ID |
| 120 | 8 | Exact complete object length |
| 128 | 32 | Expected SHA-256 of complete CSEG bytes |
| 160 | 8 | Payload offset in the object |
| 168 | 4 | Payload length |
| 172 | 4 | CRC32C of exact payload bytes |
| 176 | 4 | Header CRC32C with this field zeroed |
| 180 | 12 | Zero reserved bytes |
| 192 | variable | Payload bytes |
| final 4 | 4 | CRC32C of every preceding frame byte |

The object length is `1..68,719,476,736`, subject to a possibly lower caller limit. The offset is no
greater than the object length, and `payload_length <= object_length - offset`. Default limits are
4 MiB of payload and 16 MiB for the complete frame. Counts and lengths are checked before allocation.

A checksum-valid unknown version is unsupported. Invalid caller limits and encoder inputs are
invalid arguments. Damaged or contradictory decoded bytes are corruption. Allocation or platform
container limits produce resource exhaustion.

After published ownership and movement readiness are proved, the receipt transitions permanently
to [Tablet Physical Part Reclamation Marker v1](tablet-physical-part-reclamation-marker-v1.md).
Chunk frames remaining beside that marker are cleanup residue, never an active transfer.
