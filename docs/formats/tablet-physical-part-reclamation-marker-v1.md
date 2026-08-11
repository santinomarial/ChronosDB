# Tablet Physical Part Reclamation Marker v1

> **Status:** accepted and implemented.

The final file name is exactly `RECLAIMED`; the installation temporary is `RECLAIMED.tmp`. All
integers are unsigned little-endian. UUIDs use durable network byte order. The record is exactly 160
bytes and has no trailing data.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHRPRCL\0` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Record length `160` |
| 16 | 16 | Table ID |
| 32 | 16 | Tablet ID |
| 48 | 16 | Raft group ID |
| 64 | 8 | Placement epoch |
| 72 | 8 | Source node ID |
| 80 | 8 | Target node ID |
| 88 | 8 | Source Manifest generation |
| 96 | 16 | CSEG part ID |
| 112 | 8 | Exact complete object length |
| 120 | 32 | Expected SHA-256 of complete CSEG bytes |
| 152 | 4 | CRC32C of the complete record with this field zeroed |
| 156 | 4 | Zero reserved bytes |

All transfer-session semantic constraints from Tablet Physical Part Chunk v1 apply. A checksum-valid
unknown version is unsupported; damaged bytes, nonzero reserved bytes, an invalid session, a wrong
length, or a marker that differs from the configured receipt owner are corruption.

The marker is durable terminal state, not proof that every chunk unlink has completed. Once it is
selected, any remaining chunk files must form a valid prefix starting at zero. Recovery resumes
highest-offset-first deletion and never admits another chunk.
